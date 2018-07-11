from .model import Model
from .domain import Domain
from .schedulers import Scheduler
from .job import Job
from .ioutils import WrfHydroStatic, WrfHydroTs, check_input_files

from typing import Union
import copy
import os
import pathlib
import pickle

#TODO (TJM): Add in collect method to update sim object with job statuses post run and outputs
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

        self.base_hydro_namelist = {}
        """dict: base hydro namelist produced from model and domain"""

        self.base_hrldas_namelist = {}
        """dict: base hrldas namelist produced from model and domain"""

    # Public methods
    def add(self, obj: object):
        """Add an approparite object to a Simulation, such as a Model, Domain, Job, or Scheduler"""
        if isinstance(obj, Model):
            self._addmodel(obj)

        if isinstance(obj, Domain):
            self._adddomain(obj)

        if issubclass(type(obj),Scheduler):
            self._addscheduler(obj)

        if isinstance(obj,Job):
            self._addjob(obj)
        else:
            raise TypeError('obj is not of a type expected for a Simulation')

    def compose(self, symlink_domain: bool = True):
        """Compose simulation directories and files
        Args:
            symlink_domain: Symlink the domain files rather than copy
        """

        print("Composing simulation into directory:'" + os.getcwd() + "'")
        #Check that the current directory is empty
        current_dir = pathlib.Path(os.getcwd())
        current_dir_files = list(current_dir.rglob('*'))
        if len(current_dir_files) > 0:
            raise FileExistsError('Unable to compose, current working directory is not empty. '
                                  'Change working directory to an empty directory with os.chdir()')



        # Symlink in domain files
        print('Getting domain files...')
        self.domain.copy_files(dest_dir=os.getcwd(),symlink=symlink_domain)

        # Update job objects and make job directories
        print('Making job directories...')
        for job in self.jobs:
            # Add in base namelists form model and domain if none supplied with job
            job.add_hrldas_namelist(self.base_hrldas_namelist)
            job.add_hydro_namelist(self.base_hydro_namelist)

            job._make_job_dir()
            job._write_namelists() # write namelists

        # Validate jobs
        print('Validating job input files')
        self._validate_jobs()

        # Add jobs to scheduler
        if self.scheduler is not None:
            print('Adding jobs to scheduler...')
            for job in self.jobs:
                self.scheduler.add_job(job)

        # Compile model
        print('Compiling WRF-Hydro source code...')
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
            self.scheduler.schedule()

        # Overwrite the object after run if successfull
        with current_dir.joinpath('WrfHydroSim.pkl').open(mode='wb') as f:
            pickle.dump(self, f, 2)

    def collect(self):
        """Collect simulation output after a run"""
        self.output = SimulationOutput()
        self.output.collect(sim_dir=os.getcwd())

    # Private methods
    def _validate_model_domain(self, model, domain):
        """Private method to validate that a model and a domain are compatible"""
        if model.model_config != domain.domain_config:
            raise TypeError('Model configuration ' +
                            model.model_config +
                            ' not compatible with domain configuration ' +
                            domain.domain_config)
        if model.version != domain.model_version:
            raise TypeError('Model version ' +
                            model.version +
                            ' not compatible with domain versions ' +
                            str(list(domain.namelist_patches.keys())))

    def _validate_jobs(self):
        """Private method to check that all files are present for each job"""
        for job in self.jobs:
            print(job.job_id)
            check_input_files(hrldas_namelist=job.hrldas_namelist,
                                  hydro_namelist=job.hydro_namelist,
                                  sim_dir=os.getcwd())

    def _set_base_namelists(self):
        """Private method to create the base namelists which are added to each Job. The Job then
        modifies the namelist times"""

        # Create namelists
        hydro_namelist = self.model.hydro_namelists
        hrldas_namelist = self.model.hrldas_namelists

        ## Update namelists with namelist patches
        hydro_namelist['hydro_nlist'].update(self.domain.namelist_patches
                                             ['hydro_namelist']
                                             ['hydro_nlist'])

        hydro_namelist['nudging_nlist'].update(self.domain.namelist_patches
                                               ['hydro_namelist']
                                               ['nudging_nlist'])

        hrldas_namelist['noahlsm_offline'].update(self.domain.namelist_patches
                                                  ['namelist_hrldas']
                                                  ['noahlsm_offline'])
        hrldas_namelist['wrf_hydro_offline'].update(self.domain.namelist_patches
                                                    ['namelist_hrldas']
                                                    ['wrf_hydro_offline'])
        self.base_hydro_namelist = hydro_namelist
        self.base_hrldas_namelist = hrldas_namelist

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
        job = copy.deepcopy(job)
        self.jobs.append(job)

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

    def collect(self,sim_dir: Union[str,pathlib.Path]):
        """Collect simulation output after a run
        Args:
            sim_dir: The simulation directory
        """

        current_dir = pathlib.Path(os.curdir)

        # Grab outputs as WrfHydroXX classes of file paths
        # Get channel files
        if len(list(current_dir.glob('*CHRTOUT*'))) > 0:
            self.channel_rt = WrfHydroTs(list(current_dir.glob('*CHRTOUT*')))
            # Make relative to run dir
            # for file in self.channel_rt:
            #     file.relative_to(file.parent)

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

        ### LSM Restarts
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

        ### Nudging restarts
        self.restart_nudging = []
        for file in current_dir.glob('nudgingLastObs*'):
            file = WrfHydroStatic(file)
            self.restart_nudging.append(file)

        if len(self.restart_nudging) > 0:
            self.restart_nudging = sorted(self.restart_nudging,
                                          key=lambda file: file.stat().st_mtime_ns)
        else:
            self.restart_nudging = None
