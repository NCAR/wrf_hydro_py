from .model import Model
from .domain import Domain
from .scheduler import Scheduler

class Simulation(object):
    """Class for a WRF-Hydro setup object, which is comprised of a WrfHydroModel and a
    WrfHydroDomain.
    """

    def __init__(self):
        """Instantiates a WrfHydroSetup object
        """
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

    def __validate_model_domain__(self,model,domain):
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

    def __addmodel__(self,model: Model):
        """Private method to add a Model to a Simulation
        Args:
            model: The Model to add
        """
        if self.domain is not None:
            self.__validate_model_domain__(model,self.domain)
        self.model=model

    def __adddomain__(self,domain: Domain):
        """Private method to add a Domain to a Simulation
        Args:
            domain: The Domain to add
        """
        if self.model is not None:
            self.__validate_model_domain__(self.model, domain)
        self.domain = domain

    def __addscheduler__(self,scheduler: Scheduler):
        """Private method to add a Scheduler to a Simulation
        Args:
            scheduler: The Scheduler to add
        """
        if self.model is not None:
            self.__validate_model_domain__(self.model, scheduler)
        self.domain = scheduler

    def add(self,obj:object):
        if isinstance(obj, Model):
            self.__addmodel__(obj)
        if isinstance(obj, Domain):
            self.__adddomain__(obj)
        if issubclass(type(obj),Scheduler):
            self.__addscheduler__(obj)


    def get_namelists(self):
        # Create namelists
        hydro_namelist = self.model.hydro_namelists[self.model.version][self.domain.domain_config]
        namelist_hrldas = self.model.hrldas_namelists[self.model.version][self.domain.domain_config]

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

        namelist_hrldas['noahlsm_offline'].update(self.domain.namelist_patches
                                                       [self.model.version]
                                                       [self.domain.domain_config]
                                                       ['namelist_hrldas']
                                                       ['noahlsm_offline'])
        namelist_hrldas['wrf_hydro_offline'].update(self.domain.namelist_patches
                                                         [self.model.version]
                                                         [self.domain.domain_config]
                                                         ['namelist_hrldas']
                                                         ['wrf_hydro_offline'])
        return hydro_namelist, namelist_hrldas
