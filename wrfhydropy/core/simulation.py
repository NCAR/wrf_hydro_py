import copy
import os
import pathlib
import pickle
from typing import Union

from .domain import Domain
from .ioutils import WrfHydroStatic, WrfHydroTs, check_input_files
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
        """Instantiates a WrfHydroSetup object"""

        # Public attributes
        self.model = None
        """Model: A Model object"""

        self.domain = None
        """Domain: A Domain object"""

        self.jobs = []
        """list: a list containing Job objects"""

        self.scheduler = None
        """Scheduler: A scheduler object to use for each Job in self.jobs"""

        self.output = None
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
        elif issubclass(type(obj),Scheduler):
            self._addscheduler(obj)
        elif isinstance(obj,Job):
            self._addjob(obj)
        else:
            raise TypeError('obj is not of a type expected for a Simulation')

    def compose(self, symlink_domain: bool = True, force: bool = False):
        """Compose simulation directories and files
        Args:
            symlink_domain: Symlink the domain files rather than copy
            force: Compose into directory even if not empty. This is considered bad practice but
            is necessary in certain circumstances.
        """

        print("Composing simulation into directory:'" + os.getcwd() + "'")
        #Check that the current directory is empty
        current_dir = pathlib.Path(os.getcwd())
        current_dir_files = list(current_dir.rglob('*'))
        if len(current_dir_files) > 0 and force is False:
            raise FileExistsError('Unable to compose, current working directory is not empty. '
                                  'Change working directory to an empty directory with os.chdir()')

        # Symlink in domain files
        print('Getting domain files...')
        self.domain.copy_files(dest_dir=os.getcwd(),symlink=symlink_domain)

        # Update job objects and make job directories
        print('Making job directories...')
        for job in self.jobs:
            job._make_job_dir()
            job._write_namelists() # write namelists

        # Validate jobs
        print('Validating job input files')
        self._validate_jobs()

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

        print('Simulation successfully composed')

    def run(self):
        """Run the composed simulation"""
        current_dir = pathlib.Path(os.curdir)

        # Save the object out to the compile directory before run
        with current_dir.joinpath('WrfHydroSim.pkl').open(mode='wb') as f:
            pickle.dump(self, f, 2)

        if self.scheduler is None:

            for job in self.jobs:
                job._run()
        else:
            self.scheduler.schedule(jobs=self.jobs)

        # Overwrite the object after run if successfull
        path = current_dir.joinpath('WrfHydroSim.pkl')
        self.pickle(str(path))

    def collect(self):
        """Collect simulation output after a run"""

        current_dir = pathlib.Path(os.curdir).absolute()

        # Overwrite sim job objects with collected objects matched on job id
        ## Create dict of index/ids so that globbed jobs match the original list order
        id_index = dict()
        for index, item in enumerate(self.jobs):
            id_index[item.job_id] = index

        ## Insert collect jobs into sim job list
        job_objs = current_dir.rglob('WrfHydroJob_postrun.pkl')
        for job_obj in job_objs:
            collect_job = pickle.load(job_obj.open(mode='rb'))
            original_idx = id_index[collect_job.job_id]
            self.jobs[original_idx] = collect_job

        self.output = SimulationOutput()
        self.output.collect_output(sim_dir=os.getcwd())

    def pickle(self,path: str):
        """Pickle sim object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)

    # Private methods
    def _validate_model_domain(self, model, domain):
        """Private method to validate that a model and a domain are compatible"""
        if model.model_config != domain.domain_config:
            raise TypeError('Model configuration ' +
                            model.model_config +
                            ' not compatible with domain configuration ' +
                            domain.domain_config)
        if model.version != domain.compatible_version:
            raise TypeError('Model version ' +
                            model.version +
                            ' not compatible with domain version ' +
                            domain.compatible_version)


    def _validate_jobs(self):
        """Private method to check that all files are present for each job"""
        counter = 0
        for job in self.jobs:
            counter += 1
            print(job.job_id)
            if counter == 0:
                ignore_restarts = False
            else:
                ignore_restarts = True

            check_input_files(hrldas_namelist=job.hrldas_namelist,
                              hydro_namelist=job.hydro_namelist,
                              sim_dir=os.getcwd(),
                              ignore_restarts=ignore_restarts)

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
            self.model = copy.deepcopy(model)

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
        self.chanobs = None
        """WrfHydroTs: Timeseries dataset of CHANOBS files"""
        self.lakeout = None
        """WrfHydroTs: Timeseries dataset of LAKEOUT files"""
        self.gwout = None
        """WrfHydroTs: Timeseries dataset of GWOUT files"""
        self.restart_hydro = None
        """list: List of HYDRO_RST WrfHydroStatic objects"""
        self.restart_lsm = None
        """list: List of RESTART WrfHydroStatic objects"""
        self.restart_nudging = None
        """list: List of nudgingLastObs WrfHydroStatic objects"""

    def collect_output(self,sim_dir: Union[str,pathlib.Path]):
        """Collect simulation output after a run
        Args:
            sim_dir: The simulation directory
        """

        current_dir = pathlib.Path(os.curdir).absolute()

        # Grab outputs as WrfHydroXX classes of file paths
        # Get channel files
        if len(list(current_dir.glob('*CHRTOUT*'))) > 0:
            self.channel_rt = WrfHydroTs(list(current_dir.glob('*CHRTOUT*')))

        if len(list(current_dir.glob('*CHANOBS*'))) > 0:
            self.chanobs = WrfHydroTs(list(current_dir.glob('*CHANOBS*')))
            # Make relative to run dir
            # for file in self.chanobs:
            #     file.relative_to(file.parent)

        # Get Lakeout files
        if len(list(current_dir.glob('*LAKEOUT*'))) > 0:
            self.lakeout = WrfHydroTs(list(current_dir.glob('*LAKEOUT*')))

        # Get gwout files
        if len(list(current_dir.glob('*GWOUT*'))) > 0:
            self.gwout = WrfHydroTs(list(current_dir.glob('*GWOUT*')))

        # Get restart files and sort by modified time
        # Hydro restarts
        self.restart_hydro = []
        for file in current_dir.glob('HYDRO_RST*'):
            file = WrfHydroStatic(file)
            self.restart_hydro.append(file)

        if len(self.restart_hydro) > 0:
            self.restart_hydro = sorted(
                self.restart_hydro,
                key=lambda file: file.stat().st_mtime_ns
            )
        else:
            self.restart_hydro = None

        # LSM Restarts
        self.restart_lsm = []
        for file in current_dir.glob('RESTART*'):
            file = WrfHydroStatic(file)
            self.restart_lsm.append(file)

        if len(self.restart_lsm) > 0:
            self.restart_lsm = sorted(
                self.restart_lsm,
                key=lambda file: file.stat().st_mtime_ns
            )
        else:
            self.restart_lsm = None

        # Nudging restarts
        self.restart_nudging = []
        for file in current_dir.glob('nudgingLastObs*'):
            file = WrfHydroStatic(file)
            self.restart_nudging.append(file)

        if len(self.restart_nudging) > 0:
            self.restart_nudging = sorted(self.restart_nudging,
                                          key=lambda file: file.stat().st_mtime_ns)
        else:
            self.restart_nudging = None
