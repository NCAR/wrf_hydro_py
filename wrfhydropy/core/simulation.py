import copy
import os
import pandas as pd
import pathlib
import pickle
import shutil
from typing import Union
import warnings
import xarray

from .collection import open_whp_dataset

from .domain import Domain
from .ioutils import WrfHydroStatic, \
    WrfHydroTs, \
    check_input_files, \
    check_file_nans, \
    sort_files_by_time
from .job import Job
from .model import Model
from .namelist import Namelist
from .schedulers import Scheduler


class Simulation(object):
    """Class for a WRF-Hydro Simulation object. The Simulation object is used to orchestrate a
    WRF-Hydro simulation by accessing methods of Model, Domain, and Job objects. Optionally,
    a scheduler can also be added.
    """

    def __init__(self):
        """Instantiates a Simulation object"""

        # Public attributes
        self.model = None
        """Model: A Model object"""

        self.domain = None
        """Domain: A Domain object"""

        self.jobs = []
        """list: a list containing Job objects"""

        self.scheduler = None
        """Scheduler: A scheduler object to use for each Job in self.jobs"""

        self.output = SimulationOutput()
        """CompletedSim: A CompletedSim object returned by the self.collect() method"""

        self.base_hydro_namelist = Namelist()
        """dict: base hydro namelist produced from model and domain"""

        self.base_hrldas_namelist = Namelist()
        """dict: base hrldas namelist produced from model and domain"""

    # Public methods
    def add(self, obj: Union[Model, Domain, Scheduler, Job]):
        """Add an approparite object to a Simulation, such as a Model, Domain, Job, or Scheduler"""
        if isinstance(obj, Model):
            self._addmodel(obj)
        elif isinstance(obj, Domain):
            self._adddomain(obj)
        elif issubclass(type(obj), Scheduler):
            self._addscheduler(obj)
        elif isinstance(obj, Job):
            self._addjob(obj)
        else:
            raise TypeError('obj is not of a type expected for a Simulation')

    def compose(
        self,
        symlink_domain: bool = True,
        force: bool = False,
        check_nlst_warn: bool = False
    ):
        """Compose simulation directories and files
        Args:
            symlink_domain: Symlink the domain files rather than copy
            force: Compose into directory even if not empty. This is considered bad practice but
            is necessary in certain circumstances.
            check_nlst_warn: Allow the namelist checking/validation to only result in warnings.
            This is also not great practice, but necessary in certain circumstances.
        """

        print("Composing simulation into directory:'" + os.getcwd() + "'")
        # Check that the current directory is empty
        compose_dir = pathlib.Path(os.getcwd())
        self._compose_dir = compose_dir.absolute()
        compose_dir_files = list(compose_dir.rglob('*'))
        if len(compose_dir_files) > 0 and force is False:
            raise FileExistsError('Unable to compose, current working directory is not empty and '
                                  'force is False. '
                                  'Change working directory to an empty directory with os.chdir()')

        # Symlink in domain files
        print('Getting domain files...')
        self.domain.copy_files(dest_dir=os.getcwd(), symlink=symlink_domain)

        # Update job objects and make job directories
        print('Making job directories...')
        for job in self.jobs:
            job._make_job_dir()
            job._write_namelists()  # write namelists

        # If the first job is a restart, set the model end time.
        if self.jobs[0].restart:
            file_model_end_time = compose_dir / '.model_end_time'
            with file_model_end_time.open('w') as opened_file:
                _ = opened_file.write(str(self.jobs[0]._model_start_time))

        # Validate jobs
        print('Validating job input files')
        self._validate_jobs(check_nlst_warn=check_nlst_warn)

        # Compile model or copy files
        if self.model.compile_log is not None:
            if self.model.compile_log.returncode == 0:
                print('Model already compiled, copying files...')
                self.model.copy_files(os.getcwd())
            else:
                raise ValueError('model was previously compiled but return code is not 0')
        else:
            print('Compiling model...')
            self.model.compile(compile_dir=os.getcwd())

        # Make copies for each TBL file (never symlink)
        for from_file in self.model.table_files:
            from_file = pathlib.Path(from_file)
            to_file = compose_dir.joinpath(from_file.name)
            shutil.copy(str(from_file), str(to_file))

        print('Simulation successfully composed')

    def run(
        self,
        env: dict = None
    ):
        """Run the composed simulation.
        Returns: 0 for success.
        """
        compose_dir = self._compose_dir

        # Save the object out to the compile directory before run
        with compose_dir.joinpath('WrfHydroSim.pkl').open(mode='wb') as f:
            pickle.dump(self, f, 2)

        if self.scheduler is None:
            for job in self.jobs:
                job._run(env=env)
        else:
            self.scheduler.schedule(jobs=self.jobs)

        # Overwrite the object after run if successfull
        path = compose_dir.joinpath('WrfHydroSim.pkl')
        self.pickle(str(path))

        return int(not all(jj.exit_status == 0 for jj in self.jobs))

    def collect(self, sim_dir=None, output=True):
        """Collect simulation output after a run"""

        if sim_dir is None and hasattr(self, '_compose_dir'):
            compose_dir = self._compose_dir
        elif sim_dir is not None:
            compose_dir = sim_dir
        else:
            raise ValueError(
                'The simulation has not been composed and the sim_dir ragument not supplied.')
            # This is what we used to do, but I'm not seeing why this behavior would make senese
            # or that it's clear to remove it. Toremove when it's clearly unnecessary:
            # compose_dir = pathlib.Path('.')

        # Overwrite sim job objects with collected objects matched on job id
        # Create dict of index/ids so that globbed jobs match the original list order
        id_index = dict()
        for index, item in enumerate(self.jobs):
            id_index[item.job_id] = index

        # Insert collect jobs into sim job list
        job_objs = compose_dir.rglob('WrfHydroJob_postrun.pkl')
        for job_obj in job_objs:
            collect_job = pickle.load(job_obj.open(mode='rb'))
            original_idx = id_index[collect_job.job_id]
            self.jobs[original_idx] = collect_job

        if output:
            self.output.collect_output(sim_dir=os.getcwd())

    def pickle(self, path: str):
        """Pickle sim object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)

    def _pickle_sub_obj(
        self,
        sub_obj,
        path
    ):
        """
        Method to reduce *composed* simulation pickle sizes for performance applications. This
        method replaces a simulation sub-object (model, domain, or output)  with it's relative
        pathlib.Path. The inverse, to bring that object back from its path is restore_obj().
        Usage example:
            sim.model = sim._pickle_sub_obj(sim.model, 'WrfHydroModel.pkl')
        """
        with path.open(mode='wb') as f:
            pickle.dump(sub_obj, f, 2)
        return path

    def pickle_sub_objs(self, obj_list: list = ['model', 'domain', 'output']):
        if hasattr(self, '_compose_dir'):
            obj_path = self._compose_dir
        else:
            obj_path = pathlib.Path('.')
        if 'model' in obj_list:
            self.model = self._pickle_sub_obj(self.model, obj_path / 'WrfHydroModel.pkl')
        if 'domain' in obj_list:
            self.domain = self._pickle_sub_obj(self.domain, obj_path / 'WrfHydroDomain.pkl')
        if 'output' in obj_list:
            self.output = self._pickle_sub_obj(self.output, obj_path / 'WrfHydroOutput.pkl')

    def _restore_sub_obj(
        self,
        attr_name: pathlib.Path
    ):
        """
        Method to reduce *composed* simulation pickle sizes for performance applications. This
        method restores a simulation sub-object (model, domain, or output) from it's relative
        pathlib.Path, which replaces the object in the simulation. The inverse, that pickles the
        subobject is _pickle_sub_obj().
        Usage:
            sim.model = sim._restore_sub_obj(pathlib.Path('WrfHydroModel.pkl'))
        """
        return pickle.load(attr_name.open(mode="rb"))

    def restore_sub_objs(self, obj_list: list = ['model', 'domain', 'output']):
        if hasattr(self, '_compose_dir'):
            obj_path = self._compose_dir
        else:
            obj_path = pathlib.Path('.')
        if 'model' in obj_list:
            self.model = self._restore_sub_obj(obj_path / 'WrfHydroModel.pkl')
        if 'domain' in obj_list:
            self.domain = self._restore_sub_obj(obj_path / 'WrfHydroDomain.pkl')
        if 'output' in obj_list:
            self.output = self._restore_sub_obj(obj_path / 'WrfHydroOutput.pkl')

    def _validate_model_domain(self, model, domain):
        """Private method to validate that a model and a domain are compatible"""
        if model.model_config != domain.domain_config:
            raise TypeError('Model configuration ' +
                            model.model_config +
                            ' not compatible with domain configuration ' +
                            domain.domain_config)
        if model.version[0:2] != domain.compatible_version[0:2]:
            raise TypeError('Model version ' +
                            model.version +
                            ' not compatible with domain version ' +
                            domain.compatible_version)
        elif model.version != domain.compatible_version:
            warnings.warn('Model minor versions ' +
                          model.version +
                          ' do not match domain minor versions ' +
                          domain.compatible_version)

    def _validate_jobs(
        self,
        check_nlst_warn: bool = False
    ):
        """Private method to check that all files are present for each job.
        Args:
            check_nlst_warn: Allow the namelist checking/validation to only result in warnings.
            This is also not great practice, but necessary in certain circumstances.
        """
        counter = 0
        for job in self.jobs:
            counter += 1
            print(job.job_id)
            if counter == 0:
                ignore_restarts = False
            else:
                ignore_restarts = True

            check_input_files(
                hrldas_namelist=job.hrldas_namelist,
                hydro_namelist=job.hydro_namelist,
                sim_dir=os.getcwd(),
                ignore_restarts=ignore_restarts,
                check_nlst_warn=check_nlst_warn
            )

    def _set_base_namelists(self):
        """Private method to create the base namelists which are added to each Job. The Job then
        modifies the namelist times"""

        # Create namelists
        hydro_namelist = self.model.hydro_namelists
        hrldas_namelist = self.model.hrldas_namelists

        self.base_hydro_namelist = hydro_namelist.patch(self.domain.hydro_namelist_patches)
        self.base_hrldas_namelist = hrldas_namelist.patch(self.domain.hrldas_namelist_patches)

    def _addmodel(self, model: Model):
        """Private method to add a Model to a Simulation
        Args:
            model: The Model to add
        """
        model = copy.deepcopy(model)

        if self.domain is not None:
            # Check that model and domain are compatible
            self._validate_model_domain(model, self.domain)

            # Add in model
            self.model = model

            # Setup base namelists
            self._set_base_namelists()
        else:
            self.model = model

    def _adddomain(self, domain: Domain):
        """Private method to add a Domain to a Simulation
        Args:
            domain: The Domain to add
        """

        domain = copy.deepcopy(domain)
        if self.model is not None:
            # Check that model and domain are compatible
            self._validate_model_domain(self.model, domain)

            # Add in domain
            self.domain = domain

            # Setup base namelists
            self._set_base_namelists()
        else:
            self.domain = domain

    def _addscheduler(self, scheduler: Scheduler):
        """Private method to add a Scheduler to a Simulation
        Args:
            scheduler: The Scheduler to add
        """
        self.scheduler = copy.deepcopy(scheduler)

    def _addjob(self, job: Job):
        """Private method to add a job to a Simulation
        Args:
            scheduler: The Scheduler to add
        """
        if self.domain is not None and self.model is not None:
            job = copy.deepcopy(job)
            job._add_hydro_namelist(self.base_hydro_namelist)
            job._add_hrldas_namelist(self.base_hrldas_namelist)

            self.jobs.append(job)
        else:
            raise AttributeError('Can not add a job to a simulation without a model and a domain')


class SimulationOutput(object):
    """Class containing output objects from a completed Simulation, retrieved using the
    Simulation.collect() method"""
    def __init__(self):
        self.channel_rt = None
        """WrfHydroTs: Timeseries dataset of CHRTOUT files"""
        self.channel_rt_grid = None
        """WrfHydroTs: Timeseries dataset of CHRTOUT gridded files"""
        self.chanobs = None
        """WrfHydroTs: Timeseries dataset of CHANOBS files"""
        self.lakeout = None
        """WrfHydroTs: Timeseries dataset of LAKEOUT files"""
        self.gwout = None
        """WrfHydroTs: Timeseries dataset of GWOUT files"""
        self.rtout = None
        """WrfHydroTs: Timeseries dataset of RTOUT files"""
        self.ldasout = None
        """WrfHydroTs: Timeseries dataset of LDASOUT files"""
        self.restart_hydro = None
        """list: List of HYDRO_RST WrfHydroStatic objects"""
        self.restart_lsm = None
        """list: List of RESTART WrfHydroStatic objects"""
        self.restart_nudging = None
        """list: List of nudgingLastObs WrfHydroStatic objects"""

    def __print__(self):
        return self.__repr__(self)

    def __repr__(self):
        all_none = all([ ff is None for ff in self.__dict__.values() ])
        if all_none:
            return "This simulation currently has no output. Run simulation.collect()."
        the_repr = ''
        for key, val in self.__dict__.items():
            # if its an open dataset, get it's repr
            if isinstance(val, xarray.core.dataset.Dataset):
                the_repr += '\n' + key + ':\n' + val.__repr__() + '\n\n'
            else:
                if val is not None:
                    the_len = str(len(val))
                else:
                    the_len = "0"
                the_repr += key + ': ' + the_len + ' files \n'
        return the_repr

    def collect_output(self, sim_dir: Union[str, pathlib.Path] = None):
        """Collect simulation output after a run
        Args:
            sim_dir: The simulation directory to collect
        """
        if sim_dir is None:
            sim_dir = pathlib.Path(os.curdir).absolute()
        else:
            sim_dir = pathlib.Path(sim_dir).absolute()

        file_glob_dict = {
            'channel_rt': '*CHRTOUT_DOMAIN*',
            'channel_rt_grid': '*CHRTOUT_GRID*',
            'chanobs': '*CHANOBS*',
            'lakeout': '*LAKEOUT*',
            'gwout': '*GWOUT*',
            'rtout': '*.RTOUT_*',
            'ldasout': '*LDASOUT*',
            'restart_hydro': 'HYDRO_RST*',
            'restart_lsm': 'RESTART*',
            'restart_nudging': 'nudgingLastObs*'}

        for key, value in file_glob_dict.items():
            self.__dict__[key] = sort_files_by_time(list(sim_dir.glob(value)))
            if key == 'ldasout':
                self.__dict__[key] = self.__dict__[key][1:]

    def open(self, name, n_cores=None):
        if not hasattr(self, name):
            raise ValueError('Simulation output does not contain ' + name)
        the_files = self.__dict__[name]
        if isinstance(the_files, list):
            self.__dict__[name] = open_whp_dataset(the_files)
        elif isinstance(the_files, xarray.core.dataset.Dataset):
            print("This output appears to already be open: " + name)
        else:
            raise ValueError("Can not open: " + name)
        return None

    def check_output_nans(self, n_cores: int = 1):
        """Check all outputs for NA values"""

        # Get all the public attributes, which are the only atts of interest
        data_atts = [att for att in dir(self) if not att.startswith('_')]

        # Create a list to hold pandas dataframes
        df_list = []

        # Loop over attributes
        for att in data_atts:
            # Loop over files in each attribute
            att_obj = getattr(self, att)
            if isinstance(att_obj, list) or isinstance(att_obj, WrfHydroTs):
                if len(att_obj) == 0:
                    continue
                file = att_obj[-1]
                na_check_result = check_file_nans(file, n_cores=n_cores)
                if na_check_result is not None:
                    na_check_result['file'] = str(file)
                    df_list.append(na_check_result)

        # Combine all dfs into one
        if len(df_list) > 0:
            return pd.concat(df_list)
        else:
            return None
