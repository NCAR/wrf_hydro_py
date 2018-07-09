from .model import Model
from .domain import Domain
from .schedulers import Scheduler
from .job import Job

import copy
import os
import pathlib

class Simulation(object):
    """Class for a WRF-Hydro setup object, which is comprised of a WrfHydroModel and a
    WrfHydroDomain.
    """

    def __init__(self):
        """Instantiates a WrfHydroSetup object
        """

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

        self.sim_dir = None
        """pathlib.Path: Simulation directory path"""

        self.base_hydro_namelist = {}
        """dict: base hydro namelist produced from model and domain"""

        self.base_hrldas_namelist = {}
        """dict: base hrldas namelist produced from model and domain"""

        self.job_hydro_namelists = {}
        """dict: hydro namelists for each job keyed by job id"""

        self.job_hrldas_namelists = {}
        """dict: hrldas namelists for each job keyed by job id"""

    # Public methods
    def add(self, obj: object):
        if isinstance(obj, Model):
            self._addmodel(obj)

        if isinstance(obj, Domain):
            self._adddomain(obj)

        if issubclass(type(obj),Scheduler):
            self._addscheduler(obj)

        if isinstance(obj,Job):
            self._addjob(obj)

    def compose(self,sim_dir: pathlib.Path, symlink_domain: bool = True):
        """Compose simulation directories and files
        Args:
            sim_dir: The top-level simulation directory.
            symlink_domain: Symlink the domain files rather than copy
        """

        self.sim_dir = pathlib.Path(sim_dir)
        print("Composing simulation into directory:'" + str(self.sim_dir) + "'")
        if self.sim_dir.is_dir():
            raise IsADirectoryError(str(self.sim_dir) + 'already exists')
        else:
            self.sim_dir.mkdir()

        # Run in simulation directory, but get current directory to cd back out
        original_dir = os.getcwd()
        os.chdir(self.sim_dir)

        try:
            # Compile model, also makes sim_dir directory at compile time
            print('Compiling WRF-Hydro source code...')
            self.model.compile(compile_dir=os.getcwd())

            # Symlink in domain files
            print('Getting domain files...')
            self.domain.copy_files(dest_dir=os.getcwd(),symlink=symlink_domain)

            # Make job directories
            print('Making job directories...')
            for job in self.jobs:
                job.sim_dir = self.sim_dir

                # Add in base namelists form model and domain if none supplied with job
                job.add_hrldas_namelist(self.base_hrldas_namelist)
                job.add_hydro_namelist(self.base_hydro_namelist)

                job._make_job_dir()
                job._write_namelists() # write namelists

            # Add jobs to scheduler
            if self.scheduler is not None:
                print('Adding jobs to scheduler...')
                for job in self.jobs:
                    self.scheduler.add_job(job)

            print('Simulation successfully composed')
        finally:
            os.chdir(original_dir)

    def run(self):

        # Change to simulation directory so that all runs are relative to the sim dir.
        # This is needed so that a simulation can be run from inside the sim dir using relative
        # paths
        #
        # Run in simulation directory, but get current directory to cd back out
        original_dir = os.getcwd()
        os.chdir(self.sim_dir)

        try:
            if self.scheduler is None:

                for job in self.jobs:
                    job._run()
            else:
                self.scheduler.schedule()
        finally:
            os.chdir(original_dir)

    # Private methods
    def _validate_model_domain(self, model, domain):
        # Validate that the domain and model are compatible
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

    def _set_base_namelists(self):
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







