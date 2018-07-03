from .model import Model
from .domain import Domain
from .schedulers import Scheduler
from .job import Job

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

        self.base_hydro_namelist = {}
        """dict: base hydro namelist produced from model and domain"""

        self.base_hrldas_namelist = {}
        """dict: base hrldas namelist produced from model and domain"""

        self.job_hydro_namelists = {}
        """dict: hydro namelists for each job keyed by job id"""

        self.job_hrldas_namelists = {}
        """dict: hrldas namelists for each job keyed by job id"""

    # Public methods
    def add(self,obj:object):
        if isinstance(obj, Model):
            self._addmodel(obj)

        if isinstance(obj, Domain):
            self._adddomain(obj)

        if issubclass(type(obj),Scheduler):
            self._addscheduler(obj)

        if isinstance(obj,Job):
            self._addjob(obj)

    # Private methods
    def _validate_model_domain(self, model, domain):
        # Validate that the domain and model are compatible
        if model.model_config != domain.domain_config:
            raise TypeError('Model configuration ' +
                            model.model_config +
                            ' not compatible with domain configuration ' +
                            domain.domain_config)
        if model.version not in list(domain.namelist_patches.keys()):
            raise TypeError('Model version ' +
                            model.version +
                            ' not compatible with domain versions ' +
                            str(list(domain.namelist_patches.keys())))

    def _set_base_namelists(self):
        # Create namelists
        hydro_namelist = self.model.hydro_namelists[self.model.version][self.domain.domain_config]
        hrldas_namelist = self.model.hrldas_namelists[self.model.version][self.domain.domain_config]

        ## Update namelists with namelist patches
        hydro_namelist['hydro_nlist'].update(self.domain.namelist_patches
                                             [self.model.version]
                                             [self.domain.domain_config]
                                             ['hydro_namelist']
                                             ['hydro_nlist'])

        hydro_namelist['nudging_nlist'].update(self.domain.namelist_patches
                                               [self.model.version]
                                               [self.domain.domain_config]
                                               ['hydro_namelist']
                                               ['nudging_nlist'])

        hrldas_namelist['noahlsm_offline'].update(self.domain.namelist_patches
                                                  [self.model.version]
                                                  [self.domain.domain_config]
                                                  ['namelist_hrldas']
                                                  ['noahlsm_offline'])
        hrldas_namelist['wrf_hydro_offline'].update(self.domain.namelist_patches
                                                    [self.model.version]
                                                    [self.domain.domain_config]
                                                    ['namelist_hrldas']
                                                    ['wrf_hydro_offline'])
        self._base_hydro_namelist = hydro_namelist
        self._base_hrldas_namelist = hrldas_namelist

    def _set_job_namelists(self):

        for job in self.jobs:
            hrldas_times = job.get_hrldas_times()
            hydro_times = job.get_hydro_times()

            self.job_hrldas_namelists[job.job_id] = self.base_hrldas_namelist.update(hrldas_times)
            self.job_hydro_namelists[job.job_id] = self.base_hydro_namelist.update(hydro_times)

    def _addmodel(self, model: Model):
        """Private method to add a Model to a Simulation
        Args:
            model: The Model to add
        """
        if self.domain is not None:
            self._validate_model_domain(model, self.domain)
        self.model = model

    def _adddomain(self, domain: Domain):
        """Private method to add a Domain to a Simulation
        Args:
            domain: The Domain to add
        """
        if self.model is not None:
            self._validate_model_domain(self.model, domain)
        self.domain = domain

    def _addscheduler(self, scheduler: Scheduler):
        """Private method to add a Scheduler to a Simulation
        Args:
            scheduler: The Scheduler to add
        """
        self.scheduler = scheduler

    def _addjob(self, job: Job):
        """Private method to add a job to a Simulation
        Args:
            scheduler: The Scheduler to add
        """
        self.jobs.append(job)







