import copy
import datetime
import multiprocessing
import pathlib
from typing import Union
import os
import pickle

from .ensemble_tools import mute
from .job import Job
from .schedulers import Scheduler
from .simulation import Simulation
from .ensemble import EnsembleSimulation


def translate_forcing_dirs(forcing_dir, member, init_time):
    # Rules for both forcing_dirs and restart_dirs:
    # 1) A dot or a null string (are identical pathlib.Path objects and) mean "do nothing"
    #    with respect to the default path in the domain.
    # 2) An existing path/file is kept.
    # 3) A negative integer is units hours, pointing to a previous cast in the cycle.
    # 4) Other wise, value error raised.
    if forcing_dir == pathlib.Path(''):
        forcing_dir = member.base_hrldas_namelist['noahlsm_offline']['indir']
    elif forcing_dir.exists():
        member.base_hrldas_namelist['noahlsm_offline']['indir'] = str(forcing_dir)
    elif int(str(forcing_dir)) < 0:
        forcing_cast_time = init_time + datetime.timedelta(hours=int(str(forcing_dir)))
        # The last line is a bit hacky.
        forcing_dir = pathlib.Path(
            '../cast_' +
            forcing_cast_time.strftime('%Y%m%d%H') +
            '/' +
            pathlib.Path(members[0].base_hrldas_namelist['noahlsm_offline']['indir']).name
        )
        # cant check that it exists... or that this is a cast. does this happen at
        # compose time? will there be an error if run in parallel?
        member.base_hrldas_namelist['noahlsm_offline']['indir'] = str(forcing_dir)
    else:
        raise ValueError("No such forcing directory. Note that non-negative integers are not"
                         " allowed when specifying forcing_dirs.")
    return None


def translate_restart_dirs(restart_dir, member, init_time):
    # Rules for both forcing_dirs and restart_dirs:
    # 1) A dot or a null string (are identical pathlib.Path objects and) mean "do nothing"
    #    with respect to the default path in the domain.
    # 2) An existing path/file is kept.
    # 3) A negative integer is units hours, pointing to a previous cast in the cycle.
    # 4) Other wise, value error raised.
    if restart_dir == pathlib.Path(''):
        hydro_rst_file = \
            member.base_hydro_namelist['hydro_nlist']['restart_file']
        lsm_rst_file = \
            member.base_hrldas_namelist['noahlsm_offline']['restart_filename_requested']
        # TODO: check that these match.
        restart_dir = pathlib.Path(hydro_rst_file).parent

    elif restart_dir.exists():
        member.base_hydro_namelist['hydro_nlist']['restart_file'] = \
            str(restart_dir / init_time.strftime('HYDRO_RST.%Y-%m-%d_%H:00_DOMAIN1'))
        member.base_hrldas_namelist['noahlsm_offline']['restart_filename_requested'] = \
            str(restart_dir / init_time.strftime('RESTART.%Y%m%d%H_DOMAIN1'))

    elif int(str(restart_dir)) < 0:
        forcing_cast_time = init_time + datetime.timedelta(hours=int(str(restart_dir)))
        restart_dir = pathlib.Path('../cast_' + forcing_cast_time.strftime('%Y%m%d%H'))
        for mem in members:
            member.base_hydro_namelist['hydro_nlist']['restart_file'] = \
                str(restart_dir / init_time.strftime('HYDRO_RST.%Y-%m-%d_%H:00_DOMAIN1'))
            member.base_hrldas_namelist['noahlsm_offline']['restart_filename_requested'] = \
                str(restart_dir / init_time.strftime('RESTART.%Y%m%d%H_DOMAIN1'))

    else:
        raise ValueError("No such forcing directory. Note that non-negative integers are not"
                         " allowed when specifying restart_dirs.")
    return None


def parallel_compose_casts(arg_dict):
    """Parallelizable function to compose casts of a CycleSimulation."""

    cast = copy.deepcopy(arg_dict['prototype'])
    cast.init_time = arg_dict['init_time']
    cast.run_dir = str(pathlib.Path('cast_' + cast.init_time.strftime('%Y%m%d%H')))
    cast.forcing_dir = arg_dict['forcing_dir']
    cast.restart_dir = arg_dict['restart_dir']

    if isinstance(cast, Simulation):
        translate_forcing_dirs(cast.forcing_dir, cast, cast.init_time)
        translate_restart_dirs(cast.forcing_dir, cast, cast.init_time)
    else:
        for forcing_dir, member in zip(cast.forcing_dir, cast.members):
            translate_forcing_dirs(forcing_dir, member, cast.init_time)
            translate_restart_dirs(forcing_dir, member, cast.init_time)

    job = copy.deepcopy(arg_dict['job'])
    khour = job.model_end_time - job.model_start_time
    job.model_start_time = arg_dict['init_time']
    job.model_end_time = arg_dict['init_time'] + khour
    cast.add(job)

    if arg_dict['scheduler'] is not None:
        cast.add(arg_dict['scheduler'])

    orig_dir = os.getcwd()
    os.mkdir(cast.run_dir)
    os.chdir(cast.run_dir)
    cast.compose()

    # The Simulation object clean up.
    if 'model' in dir(cast):
        del cast.model
    if 'domain' in dir(cast):
        del cast.domain
    if 'output' in dir(cast):
        del cast.output

    if isinstance(arg_dict['prototype'], Simulation):
        cast.pickle('WrfHydroSim.pkl')
    else:
        cast.pickle('WrfHydroEns.pkl')

    os.chdir(orig_dir)

    return cast


def parallel_run_casts(arg_dict):
    """Parallelizable function to run an Cycle."""
    if type(arg_dict['cast']) is str:
        os.chdir(str(pathlib.Path(arg_dict['cycle_dir']) / arg_dict['cast']))
    else:
        os.chdir(str(pathlib.Path(arg_dict['cycle_dir']) / arg_dict['cast'].run_dir))

    pkl_file = pathlib.Path("WrfHydroSim.pkl")
    if not pkl_file.exists():
        pkl_file = pathlib.Path("WrfHydroEns.pkl")

    cast_pkl = pickle.load(pkl_file.open("rb"))
    exit_status = cast_pkl.run()

    return exit_status


class CycleSimulation(object):
    """Class for a WRF-Hydro CycleSimulation object. The Cycle Simulation object is used to
    orchestrate a set of 'N' WRF-Hydro simulations, referred to as 'casts', which only differ
    in their 1) restart times and 2) their forcings.
    """

    def __init__(
        self,
        init_times: list,
        restart_dirs: list,
        forcing_dirs: list=[],
        ncores: int=1
    ):
        """ Instantiate a Cycle object. """

        self.casts = []
        """list: a list of 'casts' which are the individual simulations in the cycle object."""

        self._init_times = []
        """list: required list of datetime.datetime objects which specify the restart time of 
        each cast in the cycle."""

        self._restart_dirs = []
        """list: required list of either strings or pathlib.Path objects (do not mix) where the
        following rules are applied:
        1) A dot or a null string (are identical pathlib.Path objects and) mean "do nothing"
           with respect to the default path in the domain.
        2) An existing path/file is kept.
        3) A negative integer is units hours, pointing to a previous cast in the cycle.
        4) Other wise, value error raised.
        """

        self._forcing_dirs = []
        """list: optional list of either strings or pathlib.Path objects (do not mix). See 
        _restart_dirs for usage rules. Unlike _restart_dirs, may be a scalar applied to
        each cast in the cycle."""

        self._job = None
        """list: a list containing Job objects"""

        self._scheduler = None
        """Scheduler: A scheduler object to use for each Job in self.jobs"""

        self.ncores = ncores
        """ncores: integer number of cores for running parallelizable methods."""

        self._addinittimes(init_times)
        self._addrestartdirs(restart_dirs)
        if forcing_dirs != []:
            self._addforcingdirs(forcing_dirs)

    def __len__(self):
        return(len(self._init_times))

    # # The "canonical" name for len
    # @property
    # def N(self):
    #     return(self.__len__())

    # Metadata to store with the "cast" simulations, conceptually this
    # data belongs to the casts:
    # 1) cast time
    # 2) cast directory
    # 3) cast forcing directory
    # 4) restart dirs?
    # 4) JLM check/revise this... 
    
    def add(
        self,
        obj: Union[Simulation, EnsembleSimulation, Scheduler, Job]
    ):
        """Add an approparite object to an CycleSimulation, such as a Simulation, Job, or
        Scheduler.
        Args:
            obj: the object to add.
        """

        if isinstance(obj, Simulation):
            self._addsimulation(obj)
        elif isinstance(obj, EnsembleSimulation):
            self._addensemble(obj)
        elif issubclass(type(obj), Scheduler):
            self._addscheduler(obj)
        elif isinstance(obj, Job):
            self._addjob(obj)
        else:
            raise TypeError('obj is not of a type expected for a EnsembleSimulation')

    def _addinittimes(self, init_times: list):
        """Private method to add init times to a CycleSimulation
        Args:
            init_times: a list of datetime.datetime objects.
        """
        if not all([isinstance(ii, datetime.datetime) for ii in init_times]):
            raise ValueError('List object not all datetime.datetime objects, as expected')

        if self._forcing_dirs != [] and len(init_times) > 1:
            if len(self._forcing_dirs) != len(init_times):
                raise ValueError("Length of init_times does not match that of self._forcing_dirs.")

        self._init_times = copy.deepcopy(init_times)

    def _addforcingdirs(self, forcing_dirs: list):
        """Private method to add forcing dirs to a Cycle.
        Args:
            forcing_dirs: a list of str objects.
        """
        if not all([type(ff) in [str, pathlib.Path, pathlib.PosixPath] for ff in forcing_dirs]):
            raise ValueError('List object not all str or pathlib.Path objects, as expected.')
        if self._init_times != [] and len(forcing_dirs) > 1:
            if len(self._init_times) != len(forcing_dirs):
                raise ValueError("Length of forcing_dirs does not match that of self._init_times.")
        self._forcing_dirs = [pathlib.Path(ff) for ff in forcing_dirs]

    def _addrestartdirs(self, restart_dirs: list):
        """Private method to add init times to a CycleSimulation
        Args:
            restart_dirs: deterministic cycle takes a list of str objects, an
            ensemble cycle takes a list (for each cycle) of lists of str objects (for the 
            ensemble).
        """
        deterministic_types = [str, pathlib.Path, pathlib.PosixPath]
        ensemble_types = [list]
        if not all([type(ff) in deterministic_types + ensemble_types for ff in restart_dirs]):
            raise ValueError(
                'restart_dirs argument not as expected for a deterministic cycle ('
                '' + repr(deterministic_types) + ') or ensemble cycle ('
                '' + repr(ensemble_types) + ').'
            )

        if all([type(ff) in deterministic_types for ff in restart_dirs]):
            self._restart_dirs = [pathlib.Path(cc) for cc in restart_dirs]
        else:
            self._restart_dirs = [([pathlib.Path(ee) for ee in cc]) for cc in restart_dirs]

        # Check the length
        if self._init_times != [] and len(restart_dirs) > 1:
            if len(self._init_times) != len(restart_dirs):
                raise ValueError("Length of restart_dirs does not match that of self._init_times.")

    def _addscheduler(self, scheduler: Scheduler):
        """Private method to add a Scheduler to an CycleSimulation
        Args:
            scheduler: The Scheduler to add
        """
        self._scheduler = copy.deepcopy(scheduler)

    def _addjob(self, job: Job):
        """Private method to add a job to an CycleSimulation
        Args:
            job: The job to add
        """
        self._job = copy.deepcopy(job)
        self._job.restart = True

    def _addsimulation(
        self,
        sim: Simulation
    ):
        """Private method to add a Simulation to an EnsembleSimulation
        Args:
            sim: The Simulation to add
        """

        if type(sim) is not Simulation:
            raise ValueError("A non-Simulation object can not be "
                             "added to the cycle object as a simulation.")

        if sim.model.compile_log is None:
            raise ValueError("Only Simulations with compiled model objects "
                             "can be added to an ensemble simulation.")

        sim_copy = copy.deepcopy(sim)

        # Ensure that the jobs and scheduler are empty and None
        sim_copy.jobs = []
        sim_copy.scheduler = None

        self._simulation = sim_copy

    def _addensemble(
        self,
        ens: EnsembleSimulation
    ):
        """Private method to add a Simulation to an EnsembleSimulation
        Args:
            ens: The EnsembleSimulation to add
        """

        if type(ens) is not EnsembleSimulation:
            raise ValueError("A non-EnsembleSimulation object can not be "
                             "added to the cycle object as a simulation.")

        if not all([isinstance(ii, list) for ii in self._restart_dirs]):
            raise ValueError("An ensemble cycle simulation requires the restart_dirs to be "
                             "a list of lists.")

        ens_copy = copy.deepcopy(ens)

        # Ensure that the jobs and scheduler are empty and None
        ens_copy.jobs = []
        ens_copy.scheduler = None

        self._ensemble = ens_copy

    def compose(
        self,
        symlink_domain: bool=True,
        force: bool=False,
        check_nlst_warn: bool=False,
        rm_casts_from_memory: bool=True
    ):
        """Cycle compose (directories and files to disk)
        Args:
            symlink_domain: Symlink the domain files rather than copy
            force: Compose into directory even if not empty. This is considered bad practice but
            is necessary in certain circumstances.
            rm_casts_from_memory: Most applications will remove the casts from the
            ensemble object upon compose. Testing and other reasons may keep them around.
            check_nlst_warn: Allow the namelist checking/validation to only result in warnings.
            This is also not great practice, but necessary in certain circumstances.
        """

        if '_simulation' in dir(self):
            cast_prototype = '_simulation'
        else:
            if '_ensemble' not in dir(self):
                raise ValueError("The cycle does not contain a _simulation or an _ensemble.")
            cast_prototype = '_ensemble'
        
        if len(self) < 1:
            raise ValueError("There are no casts (init_times) to compose.")

        self.cycle_dir = os.getcwd()

        # Allowing forcing_dirs to be optional or scalar.
        if self._forcing_dirs == []:
            if cast_prototype == '_simulation':
                self._forcing_dirs = [pathlib.Path('.')] * len(self)
            else:
                self._forcing_dirs = \
                    [([pathlib.Path('.') for _ in range(len(self.__dict__[cast_prototype]))])
                     for cc in range(len(self))]
        if len(self._forcing_dirs) == 1:
            self._forcing_dirs = [self._forcing_dirs[0] for ii in self._init_times]

        # An ensemble must have a compiled model.
        if cast_prototype == '_simulation':
            # compile the model (once) before setting up the casts. 
            if self._simulation.model.compile_log is None:
                self._simulation.model.compile()

        # Set the ensemble jobs on the casts before composing (this is a loop over the jobs).
        if self.ncores == 1:

            self.casts = [
                parallel_compose_casts(
                    {'prototype': self.__dict__[cast_prototype],
                     'init_time': init_time,
                     'restart_dir': restart_dir,
                     'forcing_dir': forcing_dir,
                     'job': self._job,
                     'scheduler': self._scheduler,
                    }
                ) for init_time, restart_dir, forcing_dir in zip(
                    self._init_times,
                    self._restart_dirs,
                    self._forcing_dirs
                )
            ]

        else:

            # Set the pool for the following parallelizable operations
            with multiprocessing.Pool(self.ncores, initializer=mute) as pool:

                self.casts = pool.map(
                    parallel_compose_casts,
                    ({
                        'prototype': self.__dict__[cast_prototype],
                        'init_time': init_time,
                        'restart_dir': restart_dir,
                        'forcing_dir': forcing_dir,
                        'job': self._job,
                        'scheduler': self._scheduler,
                    } for init_time, restart_dir, forcing_dir in zip(
                        self._init_times,
                        self._restart_dirs,
                        self._forcing_dirs
                    )
                    )
                )

        # Return from indivdual compose.
        os.chdir(self.cycle_dir)

        # After successful compose, delete the members from memory and replace with
        # their relative dirs, if requested
        if rm_casts_from_memory:
            self.rm_casts()

        # Remove bloaty atts
        if '_simulation' in dir(self):
            del self._simulation
        if '_ensemble' in dir(self):
            del self._ensemble
        del self._job

    def rm_casts(self):
        """Remove members from memory, replace with their paths."""
        run_dirs = [cc.run_dir for cc in self.casts]
        self.casts = run_dirs

    def run(
        self,
        n_concurrent: int=1
    ):
        """Run the cycle of simulations."""
        #ens_dir = os.getcwd()

        if n_concurrent > 1:
            with multiprocessing.Pool(n_concurrent, initializer=mute) as pool:
                exit_codes = pool.map(
                    parallel_run_casts,
                    ({'cast': cc, 'cycle_dir': self.cycle_dir} for cc in self.casts)
                )
        else:
            # Keep the following for debugging: Run it without pool.map
            exit_codes = [
                parallel_run_casts({'cast': cc, 'cycle_dir': self.cycle_dir}) for cc in self.casts
            ]

        # Return to the cycle dir.
        os.chdir(self.cycle_dir)

        return all([ee == 0 for ee in exit_codes])

    def pickle(self, path: str):
        """Pickle ensemble sim object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)
