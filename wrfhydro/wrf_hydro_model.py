import subprocess
from pathlib import Path, PosixPath
from shutil import copyfile, rmtree
import xarray as xr
import f90nml
import json
from copy import deepcopy
from os import chdir
from uuid import uuid4
import pickle
from warnings import warn
#########################
# netcdf file object classes

class WrfHydroTs(list):
    def open(self):
        """Open a WrfHydroTs object
        Args:
            self
        Returns:
            An xarray mfdataset object concatenated on dimension 'Time'.
        """
        return(xr.open_mfdataset(self, concat_dim='Time'))

class WrfHydroStatic(PosixPath):
    def open(self):
        """Open a WrfHydroStatic object
        Args:
            self
        Returns:
            An xarray dataset object.
        """
        return (xr.open_dataset(self))


#########################
# Classes for constructing and running a wrf_hydro simulation


class WrfHydroModel(object):
    # TODO - Add in docstring describing attributes for each class
    def __init__(self, source_dir: str):
        """Create a WrfHydroModel object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/trunk/NDHMS'.
            new_compile_dir: Optional, new directory to to hold results
               of code compilation.
        Returns:
            A WrfHydroModel object.
        """

        # Setup directory paths
        self.source_dir = Path(source_dir)

        # Load master namelists
        self.hydro_namelists = \
            json.load(open(self.source_dir.joinpath('hydro_namelists.json')))
        self.hrldas_namelists = \
            json.load(open(self.source_dir.joinpath('hrldas_namelists.json')))

        # Load compile options
        self.compile_options = json.load(open(self.source_dir.joinpath('compile_options.json')))

        # Get code version
        with open(self.source_dir.joinpath('.version')) as f:
            self.version = f.read()

    def compile(self, compiler: str,
                compile_dir: str = None,
                overwrite: bool = False,
                compile_options: dict = None) -> str:
        """Compiles WRF-Hydro using specified compiler and compile options.
        Args:
            compiler: The compiler to use, must be one of 'pgi','gfort',
                'ifort', or 'luna'.
            compile_dir: A non-existant directory to use for compilation.
            overwrite: Overwrite compile directory if exists.
            compile_options: Changes to default compile-time options. Defaults
                are {'WRF_HYDRO':1, 'HYDRO_D':1, 'SPATIAL_SOIL':1,
                     'WRF_HYDRO_RAPID':0, 'WRFIO_NCD_LARGE_FILE_SUPPORT':1,
                     'NCEP_WCOSS':1, 'WRF_HYDRO_NUDGING':0 }
        Returns:
            Success of compilation and compile directory used. Sets additional
            attributes to WrfHydroModel

        """

        # A bunch of ugly logic to check compile directory.
        if compile_dir is None:
            self.compile_dir = self.source_dir.joinpath('Run')
        else:
            self.compile_dir = Path(compile_dir)
            if self.compile_dir.is_dir() is False:
                self.compile_dir.mkdir(parents=True)
            else:
                if self.compile_dir.is_dir() is True and overwrite is True:
                    rmtree(str(self.compile_dir))
                    self.compile_dir.mkdir()
                else:
                    raise IOError(str(self.compile_dir) + ' directory already exists')

        # Add compiler and compile options as attributes and update if needed
        self.compiler = compiler

        if compile_options is not None:
            self.compile_options.update(compile_options)

        # Get directroy for setEnvar
        compile_options_file = self.source_dir.joinpath('compile_options.sh')

        # Write setEnvar file
        with open(compile_options_file,'w') as file:
            for option, value in self.compile_options.items():
                file.write("export {}={}\n".format(option, value))

        # Compile
        # Change to source code directory for compile time
        chdir(self.source_dir)
        self.configure_log = subprocess.run(['./configure', compiler],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
        self.compile_log = subprocess.run(['./compile_offline_NoahMP.sh',
                                           str(compile_options_file)],
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)

        # Add in unique ID file to match this object to prevent assosciating
        # this directory with another object
        self.object_id = str(uuid4())
        with open(self.compile_dir.joinpath('.uid'),'w') as f:
            f.write(self.object_id)

        if self.compile_log.returncode == 0:
            # Open permissions on compiled files
            subprocess.run(['chmod','-R','777',str(self.source_dir.joinpath('Run'))])

            # Wrf hydro always puts files in source directory under a new directory called 'Run'
            # Copy files to new directory if its not the same as the source code directory
            if str(self.compile_dir.parent) != str(self.source_dir):
                for file in self.source_dir.joinpath('Run').glob('*.TBL'):
                    copyfile(file,str(self.compile_dir.joinpath(file.name)))

                copyfile(str(self.source_dir.joinpath('Run').joinpath('wrf_hydro.exe')),
                         str(self.compile_dir.joinpath('wrf_hydro.exe')))

                #Remove old files
                rmtree(self.source_dir.joinpath('Run'))

            # Open permissions on copied compiled files
            subprocess.run(['chmod', '-R', '777', str(self.compile_dir)])

            #Get file lists as attributes
            # Get list of table file paths
            self.table_files = list(self.compile_dir.glob('*.TBL'))

            # Get wrf_hydro.exe file path
            self.wrf_hydro_exe = self.compile_dir.joinpath('wrf_hydro.exe')

            # Save the object out to the compile directory
            with open(self.compile_dir.joinpath('WrfHydroModel.pkl'), 'wb') as f:
                pickle.dump(self, f, 2)

            print('Model successfully compiled into ' + str(self.compile_dir))
        else:
            print('Model did not successfully compile')

# WRF-Hydro Domain object
class WrfHydroDomain(object):
    def __init__(self,
                 domain_top_dir: str,
                 domain_config: str,
                 model_version: str,
                 namelist_patch_file: str = 'namelist_patches.json'):
        """Create a WrfHydroDomain object
        Args:
            domain_top_dir: Parent directory containing all domain directories and files.
            domain_config: The domain configuration to use, options are 'NWM',
                'Gridded', or 'Reach'
            model_version: The WRF-Hydro model version
            namelist_patch_file: Filename of json file containing namelist patches
        Returns:
            A WrfHydroDomain directory object
        """

        ###Instantiate arguments to object
        # Make file paths
        self.domain_top_dir = Path(domain_top_dir)
        self.namelist_patch_file = self.domain_top_dir.joinpath(namelist_patch_file)

        # Load namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file, 'r'))

        self.model_version = model_version
        self.domain_config = domain_config
        ###

        # Create file paths from hydro namelist
        domain_hydro_nlist = self.namelist_patches[self.model_version][self.domain_config][
            'hydro_namelist']['hydro_nlist']

        self.hydro_files = []
        for key, value in domain_hydro_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix =='.nc':
                    self.hydro_files.append(WrfHydroStatic(file_path))
                else:
                    self.hydro_files.append(file_path)

        # Create file paths from nudging namelist
        domain_nudging_nlist = self.namelist_patches[self.model_version][self.domain_config
        ]['hydro_namelist']['nudging_nlist']

        self.nudging_files = []
        for key, value in domain_nudging_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix =='.nc':
                    self.nudging_files.append(WrfHydroStatic(file_path))
                else:
                    self.nudging_files.append(file_path)

        # Create symlinks from lsm namelist
        domain_lsm_nlist = \
        self.namelist_patches[self.model_version][self.domain_config]['namelist_hrldas'
        ]["noahlsm_offline"]

        self.lsm_files = []
        for key, value in domain_lsm_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))

            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.lsm_files.append(WrfHydroStatic(file_path))
                else:
                    self.lsm_files.append(file_path)

            if key == 'indir':
                self.forcing_dir = file_path



class WrfHydroSim(object):
    def __init__(self, wrf_hydro_model: object,
                 wrf_hydro_domain: object,
                 domain_model_version = None):
        """Create a WrfHydroSim object
        Args:
            wrf_hydro_model: A WrfHydroModel object
            wrf_hydro_domain: A WrfHydroDomain object
        Returns:
            A WrfHydroSim object
        """
        # assign objects to self
        self.model = deepcopy(wrf_hydro_model)
        self.domain = deepcopy(wrf_hydro_domain)

        # Create namelists
        self.hydro_namelist = \
            deepcopy(self.model.hydro_namelists[self.model.version][self.domain.domain_config])

        self.hydro_namelist['hydro_nlist'].update(self.domain.namelist_patches
                                                  [self.model.version]
                                                  [self.domain.domain_config]
                                                  ['hydro_namelist']
                                                  ['hydro_nlist'])

        self.hydro_namelist['nudging_nlist'].update(self.domain.namelist_patches
                                                    [self.model.version]
                                                    [self.domain.domain_config]
                                                    ['hydro_namelist']
                                                    ['nudging_nlist'])

        self.namelist_hrldas = \
            deepcopy(self.model.hrldas_namelists[self.model.version][self.domain.domain_config])

        self.namelist_hrldas['noahlsm_offline'].update(self.domain.namelist_patches
                                                       [self.model.version]
                                                       [self.domain.domain_config]
                                                       ['namelist_hrldas']
                                                       ['noahlsm_offline'])
        self.namelist_hrldas['wrf_hydro_offline'].update(self.domain.namelist_patches
                                                         [self.model.version]
                                                         [self.domain.domain_config]
                                                         ['namelist_hrldas']
                                                         ['wrf_hydro_offline'])

    def run(self,
            simulation_dir: str,
            num_cores: int = 2,
            mode: str = 'r') -> str:
        """Run the wrf_hydro simulation
        Args:
            run_command: The command to execute the model. Defaults to prepared mpiexec
                         command using num_cores argument. Otherwise, supply a list that
                         can be passed to subprocess.run.
            num_cores: Optional, the number of cores to using default run_command
            overwrite: Overwrite directory if exists

        Returns:
            A string indicating success of run and new attributes to the object

        TODO:
            Add option for custom run commands to deal with job schedulers
        """
        #Make copy of simulation object to alter and return
        run_object = deepcopy(self)

        try:
            #add num cores as attribute
            run_object.num_cores = num_cores

            #Add sim dir
            run_object.simulation_dir = Path(simulation_dir)

            #Make directory if it does not exists
            if run_object.simulation_dir.is_dir() is False:
                run_object.simulation_dir.mkdir(parents=True)
            else:
                if run_object.simulation_dir.is_dir() is True and mode == 'w':
                    rmtree(str(run_object.simulation_dir))
                    run_object.simulation_dir.mkdir(parents=True)
                elif run_object.simulation_dir.is_dir() is True and mode == 'r':
                    raise PermissionError('Run directory already exists and mode = r')
                else:
                    warn('Existing run directory will be used for simulation')


            ### Check that compile object uid matches compile directory uid
            ### This is to ensure that a new model has not been compiled into that directory unknowingly
            with open(run_object.model.compile_dir.joinpath('.uid')) as f:
                compile_uid = f.read()

            if run_object.model.object_id != compile_uid:
                raise PermissionError('object id mismatch between WrfHydroModel object and'
                                      'WrfHydroModel.compile_dir directory. Directory may have been'
                                      'used for another compile')
            ###########################################################################
            # MAKE RUN DIRECTORIES
            # Construct all file/dir paths
            # TODO- Make all symlinks from namelist, NOT arguments or assumed folder structure
            # Convert strings to Path objects

            # Loop to make symlinks for each TBL file
            for from_file in run_object.model.table_files:
                # Create file paths to symlink
                to_file = run_object.simulation_dir.joinpath(from_file.name)
                # Create symlinks
                to_file.symlink_to(from_file)

            # Symlink in exe
            wrf_hydro_exe = run_object.model.wrf_hydro_exe
            run_object.simulation_dir.joinpath(wrf_hydro_exe.name).symlink_to(wrf_hydro_exe)

            # Symlink in forcing
            forcing_dir = run_object.domain.forcing_dir
            run_object.simulation_dir.joinpath(forcing_dir.name). \
                symlink_to(forcing_dir, target_is_directory=True)

            # create DOMAIN directory and symlink in files
            # Symlink in hydro_files
            for file_path in run_object.domain.hydro_files:
                # Get new file path for run directory, relative to the top-level domain directory
                # This is needed to ensure the path matches the domain namelist
                relative_path = file_path.relative_to(run_object.domain.domain_top_dir)
                symlink_path = run_object.simulation_dir.joinpath(relative_path)
                if symlink_path.parent.is_dir() is False:
                    symlink_path.parent.mkdir(parents=True)
                symlink_path.symlink_to(file_path)

            # Symlink in nudging files
            for file_path in run_object.domain.nudging_files:
                # Get new file path for run directory, relative to the top-level domain directory
                # This is needed to ensure the path matches the domain namelist
                relative_path = file_path.relative_to(run_object.domain.domain_top_dir)
                symlink_path = run_object.simulation_dir.joinpath(relative_path)
                if symlink_path.parent.is_dir() is False:
                    symlink_path.parent.mkdir(parents=True)
                symlink_path.symlink_to(file_path)

            # Symlink in lsm files
            for file_path in run_object.domain.lsm_files:
                # Get new file path for run directory, relative to the top-level domain directory
                # This is needed to ensure the path matches the domain namelist
                relative_path = file_path.relative_to(run_object.domain.domain_top_dir)
                symlink_path = run_object.simulation_dir.joinpath(relative_path)
                if symlink_path.parent.is_dir() is False:
                    symlink_path.parent.mkdir(parents=True)
                symlink_path.symlink_to(file_path)


            # write hydro.namelist
            f90nml.write(run_object.hydro_namelist,
                         run_object.simulation_dir.joinpath('hydro.namelist'))
            # write namelist.hrldas
            f90nml.write(run_object.namelist_hrldas,
                         run_object.simulation_dir.joinpath('namelist.hrldas'))

            # Run the model
            chdir(run_object.simulation_dir)
            run_object.run_log = subprocess.run(['mpiexec','-np',str(num_cores),'./wrf_hydro.exe'],
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE)

            # String match diag files for successfull run
            with open(run_object.simulation_dir.joinpath('diag_hydro.00000')) as f:
                diag_file = f.read()
                if 'The model finished successfully.......' in diag_file:
                    run_object.run_status = 0
                else:
                    run_object.run_status = 1

            if run_object.run_status == 0:

                #####################
                # Grab outputs as WrfHydroXX classes of file paths

                # TODO TJM - Make all files fall under an 'output_files' attirbute

                ## Get diag files
                run_object.diag = list(run_object.simulation_dir.glob('diag_hydro.*'))

                ## Get channel files
                if len(list(run_object.simulation_dir.glob('*CHRTOUT*'))) > 0:
                    run_object.channel_rt = WrfHydroTs(list(
                        run_object.simulation_dir.glob('*CHRTOUT*')
                    ))
                if len(list(run_object.simulation_dir.glob('*CHANOBS*'))) > 0:
                    run_object.chanobs = WrfHydroTs(list(
                        run_object.simulation_dir.glob('*CHANOBS*')
                    ))

                ## Get restart files and sort by modified time
                ### Hydro restarts
                run_object.restart_hydro = []
                for file in run_object.simulation_dir.glob('HYDRO_RST*'):
                    file = WrfHydroStatic(file)
                    run_object.restart_hydro.append(file)

                if len(run_object.restart_hydro) > 0:
                    run_object.restart_hydro = sorted(run_object.restart_hydro,
                                                      key=lambda file: file.stat().st_mtime_ns)

                ### LSM Restarts
                run_object.restart_lsm = []
                for file in run_object.simulation_dir.glob('RESTART*'):
                    file = WrfHydroStatic(file)
                    run_object.restart_lsm.append(file)

                if len(run_object.restart_lsm) > 0:
                    run_object.restart_lsm = sorted(run_object.restart_lsm,
                                                    key=lambda file: file.stat().st_mtime_ns)

                ### Nudging restarts
                run_object.restart_nudging = []
                for file in run_object.simulation_dir.glob('nudgingLastObs*'):
                    file = WrfHydroStatic(file)
                    run_object.restart_nudging.append(file)

                if len(run_object.restart_nudging) > 0:
                    run_object.restart_nudging = sorted(run_object.restart_nudging,
                                                        key=lambda file: file.stat().st_mtime_ns)

                #####################

                # create a UID for the simulation and save in file
                run_object.object_id = str(uuid4())
                with open(run_object.simulation_dir.joinpath('.uid'), 'w') as f:
                    f.write(run_object.object_id)

                # Save object to simulation directory
                # Save the object out to the compile directory
                with open(run_object.simulation_dir.joinpath('wrf_hydro_sim.pkl'), 'wb') as f:
                    pickle.dump(run_object, f, 2)

                print('Model run succeeded')
                return run_object
            else:
                warn('Model run failed')
                return run_object
        except Exception as e:
            warn('Model run failed')
            print(e)
            return run_object

