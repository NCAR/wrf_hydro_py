import copy
import datetime
import f90nml
import json
import os
import pathlib
import pickle
import re
import shlex
import shutil
import subprocess
import uuid
import warnings
import xarray as xr

from .utilities import get_git_revision_hash
from .ensemble import WrfHydroEnsembleRun

class DartExec(object):
    def __init__(
        self,
        tag,
        source_dir,
        target_dir: pathlib.PosixPath=None
    ):

        # If target_dir, the executable and namelist identified by tag in
        # source_dir are copied and referenced in target_dir.

        self.exe_file = None
        """The DART executable pathlib.PosixPath"""
        self.input_nml_file = None
        """The input nml pathlib.PosixPath for this executable"""
        self.input_nml = dict()
        """The namelist dictionary for the executable."""

        input_nml_file_source = source_dir / ('input.nml.' + tag + '_default')
        if input_nml_file_source.exists():
            if target_dir is not None:
                input_nml_file_target = target_dir / ('input.nml.' + tag + '_default')
                shutil.copy(str(input_nml_file_source), str(input_nml_file_target))
            else:
                input_nml_file_target = input_nml_file_source

            self.input_nml_file = input_nml_file_target
            self.input_nml = f90nml.read(self.input_nml_file)            
        else:
            self.input_nml_file = None
            self.input_nml = None

        exe_file_source = source_dir / tag
        if exe_file_source.exists():
            if target_dir is not None:
                exe_file_target = target_dir / tag
                shutil.copy(str(exe_file_source), str(exe_file_target))
            else:
                exe_file_target = exe_file_source

            self.exe_file = exe_file_target
        else:
            self.exe_file = None


class DartSetup(object):
    """Class for a dart build = mkmf + compile and its resulting objects."""
    def __init__(
        self,
        source_dir: str,
        mkmf_template: str,
        input_nml_file: str='models/wrfHydro/work/input.nml',
        model_work_dir: str='models/wrfHydro/work',
        mpi: bool=True, 
        build_dir: str = None,
        overwrite: bool = False
    ):
        """Instantiate a WrfHydroModel object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/trunk/NDHMS'.
            mkmf_template: the file to use fo mkmf.
            build_dir: Optional, new directory to to hold results
               of code compilation.
        Returns:
            A DartSetup object.
        """
        # Instantiate all attributes and methods
        self.source_dir = source_dir
        """pathlib.Path: pathlib.Path object for source code directory."""
        self.mkmf_template = mkmf_template
        self.model_work_dir = model_work_dir
        self.mpi = mpi       
        self.build_dir = build_dir

        self.input_nml_file = input_nml_file
        self.input_nml = dict()
        """dict: the input_nml stored with the source code."""
        
        self.compiler = None
        #self.modules = None
        self.git_hash = None
        self.compile_log = None
        """CompletedProcess: The subprocess object generated at compile."""
        self.object_id = None
        """str: A unique id to join object to compile directory."""

        self.preprocess = None
        self.create_fixed_network_seq = None
        self.create_obs_sequence = None
        self.model_mod_check = None
        self.obs_diag = None
        self.obs_seq_to_netcdf = None
        self.obs_sequence_tool = None
        self.perfect_model_obs = None
        self.filter = None
        """DartExec classes for compile-time executables and their namelists.."""

        ## Setup directory paths
        self.source_dir = pathlib.PosixPath(source_dir).absolute()
        self.mkmf_template = self.source_dir / ('build_templates/' + self.mkmf_template)
        self.model_work_dir = self.source_dir / self.model_work_dir
        if self.build_dir is None:
            self.build_dir = self.model_work_dir
        else:
            self.build_dir = pathlib.PosixPath(self.build_dir)
            self.build_dir.mkdir()
            # TODO(JLM): enforce that basename(build_dir) is experiment_dir
        
        ## Load master namelists
        # TODO(JLM): allow flexibility in the input_nml_file, check its
        # basename: if none, then do the following, else leave it alone.
        self.input_nml = f90nml.read(self.source_dir / input_nml_file)
        if self.build_dir != self.model_work_dir:
            self.input_nml_file = self.build_dir / os.path.basename(input_nml_file)
            self.input_nml.write(self.input_nml_file)

        self.compile()

    # Could probably eliminate this as a method...     
    def compile(
        self
    ) -> str:

        # Ignore this stuff for the moment.
        # # A bunch of ugly logic to check compile directory.
        # if self.compile_dir is None:
        #     self.compile_dir = self.source_dir.joinpath('models/wrfHydro/work')
        # else:
        #     self.compile_dir = pathlib.Path(compile_dir).absolute()
        #     if self.compile_dir.is_dir() is False:
        #         self.compile_dir.mkdir(parents=True)
        #     else:
        #         if self.compile_dir.is_dir() is True and overwrite is True:
        #             shutil.rmtree(str(self.compile_dir))
        #             self.compile_dir.mkdir()
        #         else:
        #             raise IOError(str(self.compile_dir) + ' directory already exists')

        # Add compiler and compile options as attributes and update if needed
        #self.compiler = compiler
        #self.modules
        self.git_hash = get_git_revision_hash(self.source_dir)

        # mkmf
        mkmf_dir = self.source_dir / 'build_templates'
        mkmf_target = mkmf_dir / 'mkmf.template'
        mkmf_target.unlink()
        mkmf_target.symlink_to(self.mkmf_template)
        # TODO(JLM): Apparently, one does not need to run mkmf.
        # I'm going to leave this here for a while till i'm sure (5/18/18)
        #mkmf_cmd = './mkmf'
        #print('DartSetup: Running "' + mkmf_cmd + '"')
        #self.mkmf_log = subprocess.run(shlex.split(mkmf_cmd),
        #                               stdout=subprocess.PIPE,
        #                               stderr=subprocess.PIPE,
        #                               cwd=self.source_dir / 'build_templates')

        # compile
        build_cmd = './quickbuild.csh'
        if self.mpi:
            build_cmd += ' -mpi'
        print('DartSetup: Running "' + build_cmd + '"')
        self.compile_log = subprocess.run(shlex.split(build_cmd),
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE,
                                          cwd=self.model_work_dir)

        # Add in unique ID file to match this object to prevent assosciating
        # this directory with another object
        self.object_id = str(uuid.uuid4())

        with open(self.build_dir.joinpath('.uid'),'w') as f:
            f.write(self.object_id)

        if self.compile_log.returncode == 0:
            # Collect the built binaries.

            build_objs = [
                'preprocess',
                'create_fixed_network_seq',
                'create_obs_sequence',
                'model_mod_check',
                'obs_diag',
                'obs_seq_to_netcdf',
                'obs_sequence_tool',
                'perfect_model_obs',
                'filter'
            ]

            for bb in build_objs:
                self.__dict__[bb] = DartExec(
                    bb,
                    self.model_work_dir,
                    self.build_dir
                )

            # Save the object out to the compile directory
            self.pickle()

            print('DART successfully compiled into ' + str(self.build_dir))

        else:

            raise ValueError('DART did not successfully compile.')


    def pickle(self):
        with open(self.build_dir.joinpath('DartSetup.pkl'), 'wb') as f:
            pickle.dump(self, f, 2)


class HydroDartRun(object):
    """Class for dart and wrf-hydro runs (currently just filter?)."""
    def __init__(
        self,
        dart_setup: DartSetup,
        wrf_hydro_ens_run: WrfHydroEnsembleRun,
        config: dict()={}
    ):
        self.dart_setup = dart_setup
        self.wrf_hydro_ens_run = wrf_hydro_ens_run
        self.config = config
        # jobs_pending
        # job_active
        # jobs_completed


    def pickle(
        self,
        path: pathlib.PosixPath=None
    ):
        filepath = path / 'HydroDartRun.pkl' 
        with open(filepath, 'wb') as f:
            pickle.dump(self, f, 2)


    # def add_jobs()
    # def run_jobs()

