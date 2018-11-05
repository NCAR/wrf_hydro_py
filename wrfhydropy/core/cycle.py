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


def translate_special_paths(cast):
    # Rules for both forcing_dirs and restart_dirs:
    # 1) A dot or a null string (are identical pathlib.Path objects and) mean "do nothing"
    #    with respect to the default path in the domain.
    # 2) An existing path/file is kept.
    # 3) A negative integer is units hours, pointing to a previous cast in the cycle.
    # 4) Other wise, value error raised.

    # forcing_dirs:
    if cast.forcing_dir == pathlib.Path(''):
        cast.forcing_dir = cast.base_hrldas_namelist['noahlsm_offline']['indir']
    elif cast.forcing_dir.exists():
        cast.base_hrldas_namelist['noahlsm_offline']['indir'] = str(cast.forcing_dir)
    elif int(str(cast.forcing_dir)) < 0:
        forcing_cast_time = cast.init_time + datetime.timedelta(hours=int(str(cast.forcing_dir)))
        # The last line is a bit hacky.
        cast.forcing_dir = pathlib.Path(
            '../cast_' +
            forcing_cast_time.strftime('%Y%m%d%H') +
            '/' +
            pathlib.Path(cast.base_hrldas_namelist['noahlsm_offline']['indir']).name
        )
        # cant check that it exists... or that this is a cast. does this happen at
        # compose time? will there be an error if run in parallel?
        cast.base_hrldas_namelist['noahlsm_offline']['indir'] = str(cast.forcing_dir)
    else:
        raise ValueError("No such forcing directory. Note that non-negative integers are not"
                         " allowed when specifying forcing_dirs.")

    # restart_dirs:
    if cast.restart_dir == pathlib.Path(''):
        hydro_rst_file = \
            cast.domain.hydro_namelist_patches['hydro_nlist']['restart_file']
        lsm_rst_file = \
            cast.domain.hrldas_namelist_patches['noahlsm_offline']['restart_filename_requested']
        # TODO: check that these match.
        cast.restart_dir = pathlib.Path(hydro_rst_file).parent

    elif cast.restart_dir.exists():
        cast.domain.hydro_namelist_patches['hydro_nlist']['restart_file'] = \
            str(cast.restart_dir / cast.init_time.strftime('HYDRO_RST.%Y-%m-%d_%H:00_DOMAIN1'))
        cast.domain.hrldas_namelist_patches['noahlsm_offline']['restart_filename_requested'] = \
            str(cast.restart_dir / cast.init_time.strftime('RESTART.%Y%m%d%H_DOMAIN1'))

    elif int(str(cast.restart_dir)) < 0:
        forcing_cast_time = cast.init_time + datetime.timedelta(hours=int(str(cast.restart_dir)))
        cast.restart_dir = pathlib.Path('../cast_' + forcing_cast_time.strftime('%Y%m%d%H'))
        cast.domain.hydro_namelist_patches['hydro_nlist']['restart_file'] = \
            str(cast.restart_dir / cast.init_time.strftime('HYDRO_RST.%Y-%m-%d_%H:00_DOMAIN1'))
        cast.domain.hrldas_namelist_patches['noahlsm_offline']['restart_filename_requested'] = \
            str(cast.restart_dir / cast.init_time.strftime('RESTART.%Y%m%d%H_DOMAIN1'))

    else:
        raise ValueError("No such forcing directory. Note that non-negative integers are not"
                         " allowed when specifying restart_dirs.")


def parallel_compose_casts(arg_dict):
    """Parallelizable function to compose casts of a CycleSimulation."""

    cast = copy.deepcopy(arg_dict['simulation'])
    cast.init_time = arg_dict['init_time']
    cast.run_dir = str(pathlib.Path('cast_' + cast.init_time.strftime('%Y%m%d%H')))
    cast.forcing_dir = arg_dict['forcing_dir']
    cast.restart_dir = arg_dict['restart_dir']

    translate_special_paths(cast)

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

    del cast.model
    del cast.domain
    del cast.output

    cast.pickle('WrfHydroSim.pkl')
    os.chdir(orig_dir)

    return cast


def parallel_run_casts(arg_dict):
    """Parallelizable function to run an Cycle."""
    if type(arg_dict['cast']) is str:
        os.chdir(str(pathlib.Path(arg_dict['cycle_dir']) / arg_dict['cast']))
    else:
        os.chdir(str(pathlib.Path(arg_dict['cycle_dir']) / arg_dict['cast'].run_dir))
    cast_pkl = pickle.load(open("WrfHydroSim.pkl", "rb"))
    cast_pkl.run()
    return cast_pkl.jobs[0].exit_status


# Classes for constructing and running a wrf_hydro simulation
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
        """ Instantiates an EnsembleSimulation object. """

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

    # The "canonical" name for len
    @property
    def N(self):
        return(self.__len__())

    # Metadata to store with the "cast" simulations, conceptually this
    # data belongs to the casts:
    # 1) cast time
    # 2) cast directory
    # 3) cast forcing directory

    def add(
        self,
        obj: Union[Simulation, Scheduler, Job]
    ):
        """Add an approparite object to an EnsembleSimulation, such as a Simulation, Job, or
        Scheduler.
        Args:
            obj: the object to add.
        """
        if isinstance(obj, Simulation):
            self._addsimulation(obj)
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
        """Private method to add init times to a CycleSimulation
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
            restart_dirs: a list of str objects.
        """
        if not all([type(ff) in [str, pathlib.Path, pathlib.PosixPath] for ff in restart_dirs]):
            raise ValueError('List object not all str or pathlib.Path objects, as expected')
        if self._init_times != [] and len(restart_dirs) > 1:
            if len(self._init_times) != len(restart_dirs):
                raise ValueError("Length of restart_dirs does not match that of self._init_times.")
        self._restart_dirs = [pathlib.Path(ff) for ff in restart_dirs]

    def _addscheduler(self, scheduler: Scheduler):
        """Private method to add a Scheduler to an EnsembleSimulation
        Args:
            scheduler: The Scheduler to add
        """
        self._scheduler = copy.deepcopy(scheduler)

    def _addjob(self, job: Job):
        """Private method to add a job to an EnsembleSimulation
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
            model: The Model to add
        """

        if type(sim) is not Simulation:
            raise ValueError("A non-simulation object can not be "
                             "added to the cycle object as a simulation.")

        if sim.model.compile_log is None:
            raise ValueError("Only simulations with compiled model objects "
                             "can be added to an ensemble simulation.")

        sim_copy = copy.deepcopy(sim)

        # Ensure that the jobs and scheduler are empty and None
        sim_copy.jobs = []
        sim_copy.scheduler = None

        self._simulation = sim_copy

    def compose(
        self,
        symlink_domain: bool=True,
        force: bool=False,
        check_nlst_warn: bool=False,
        rm_casts_from_memory: bool=True
    ):
        """Ensemble compose simulation directories and files
        Args:
            symlink_domain: Symlink the domain files rather than copy
            force: Compose into directory even if not empty. This is considered bad practice but
            is necessary in certain circumstances.
            rm_casts_from_memory: Most applications will remove the casts from the
            ensemble object upon compose. Testing and other reasons may keep them around.
            check_nlst_warn: Allow the namelist checking/validation to only result in warnings.
            This is also not great practice, but necessary in certain circumstances.
        """

        if len(self) < 1:
            raise ValueError("There are no casts (init_times) to compose.")

        self.cycle_dir = os.getcwd()

        # Allowing forcing_dirs to be optional or scalar.
        if self._forcing_dirs == []:
            self._forcing_dirs = [pathlib.Path('.')] * len(self)
        if len(self._forcing_dirs) == 1:
            self._forcing_dirs = [self._forcing_dirs[0] for ii in self._init_times]

        # compile the model (once) before setting up the casts.
        if self._simulation.model.compile_log is None:
            self._simulation.model.compile()

        # Set the ensemble jobs on the casts before composing (this is a loop over the jobs).
        if self.ncores == 1:
            
            self.casts = [
                parallel_compose_casts(
                    {'simulation': self._simulation,
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
                        'simulation': self._simulation,
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
        del self._simulation
        del self._job

    def rm_casts(self):
        """Remove members from memory, replace with their paths."""
        run_dirs = [cc.run_dir for cc in self.casts]
        self.casts = run_dirs

    def run(
        self,
        n_concurrent: int=1
    ):
        """Run the ensemble of simulations."""
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
