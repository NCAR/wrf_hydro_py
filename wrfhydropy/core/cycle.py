import copy
import datetime
import multiprocessing
import pathlib
from typing import Union
import os
import pickle

# For testing coverage reports
try:
    from pytest_cov.embed import cleanup_on_sigterm
except ImportError:
    pass
else:
    cleanup_on_sigterm()

from .ensemble_tools import mute
from .job import Job
from .schedulers import Scheduler
from .simulation import Simulation
from .ensemble import EnsembleSimulation
from .teams import parallel_teams_run, assign_teams


def integer_coercable(val):
    try:
        int(str(val))
        integer_coercable = True
    except ValueError:
        integer_coercable = False
    return integer_coercable


def translate_forcing_dirs(forcing_dir, member, init_time):
    # Rules for both forcing_dirs and restart_dirs:
    # 1) A dot or a null string (are identical pathlib.Path objects and) mean "do nothing"
    #    with respect to the default path in the domain.
    # 2) An existing path/file is kept.
    # 3) A negative integer is units hours, pointing to a previous cast in the cycle.
    # 4) Other wise, value error raised.
    if integer_coercable(forcing_dir):
        if int(str(forcing_dir)) > 0:
            raise ValueError('Only non-negative integers can be used to specify forcing_dirs')

        forcing_cast_time = init_time + datetime.timedelta(hours=int(str(forcing_dir)))
        # The last line is a bit hacky.
        if not hasattr(member, 'number'):
            forcing_dir = pathlib.Path(
                '../cast_' +
                forcing_cast_time.strftime('%Y%m%d%H') +
                '/' +
                pathlib.Path(member.base_hrldas_namelist['noahlsm_offline']['indir']).name
            )

        else:
            forcing_dir = pathlib.Path(
                '../../cast_' +
                forcing_cast_time.strftime('%Y%m%d%H') +
                '/' + member.run_dir + '/' +
                pathlib.Path(member.base_hrldas_namelist['noahlsm_offline']['indir']).name
            )

    else:
        if forcing_dir == pathlib.Path(''):
            return None
        elif forcing_dir.exists():
            forcing_dir = forcing_dir.resolve()
        else:
            raise ValueError("No such forcing directory: " + str(forcing_dir))

    member.base_hrldas_namelist['noahlsm_offline']['indir'] = str(forcing_dir)

    return None


def translate_restart_dirs(restart_dir, member, init_time):
    # Rules for both forcing_dirs and restart_dirs:
    # 1) A dot or a null string (are identical pathlib.Path objects and) mean "do nothing"
    #    with respect to the default path in the domain.
    # 2) An existing path/file is used/kept (a non-existent path is not, should give error).
    # 3) A negative integer is units hours, pointing to a previous cast in the cycle.
    # 4) Other wise, value error raised.
    if integer_coercable(restart_dir):
        if int(str(restart_dir)) > 0:
            raise ValueError('Only non-negative integers can be used to specify restart_dirs')

        forcing_cast_time = init_time + datetime.timedelta(hours=int(str(restart_dir)))
        if not hasattr(member, 'number'):
            restart_dir = pathlib.Path('../cast_' + forcing_cast_time.strftime('%Y%m%d%H'))
        else:
            restart_dir = pathlib.Path(
                '../../cast_' + forcing_cast_time.strftime('%Y%m%d%H') + '/' + member.run_dir
            )

    else:
        if restart_dir == pathlib.Path(''):
            return None
        elif restart_dir.exists():
            restart_dir = restart_dir.resolve()
        else:
            raise ValueError("No such restart directory: " + str(restart_dir))

    member.base_hrldas_namelist['noahlsm_offline']['restart_filename_requested'] = \
        str(restart_dir / init_time.strftime('RESTART.%Y%m%d%H_DOMAIN1'))
    member.base_hydro_namelist['hydro_nlist']['restart_file'] = \
        str(restart_dir / init_time.strftime('HYDRO_RST.%Y-%m-%d_%H:00_DOMAIN1'))
    if 'nudging_nlist' in member.base_hydro_namelist.keys() and \
       'nudginglastobsfile' in member.base_hydro_namelist['nudging_nlist'].keys():
        member.base_hydro_namelist['nudging_nlist']['nudginglastobsfile'] = \
            str(restart_dir / init_time.strftime('nudgingLastObs.%Y-%m-%d_%H:%M:%S.nc'))

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
        translate_restart_dirs(cast.restart_dir, cast, cast.init_time)
    else:
        for forcing_dir, restart_dir, member in zip(
                cast.forcing_dir, cast.restart_dir, cast.members
        ):
            translate_forcing_dirs(forcing_dir, member, cast.init_time)
            translate_restart_dirs(restart_dir, member, cast.init_time)

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
    if isinstance(cast, Simulation):
        cast.compose()
    else:
        cast.compose(rm_members_from_memory=arg_dict['rm_members_from_memory'])

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
        os.chdir(str(pathlib.Path(arg_dict['compose_dir']) / arg_dict['cast']))
    else:
        os.chdir(str(pathlib.Path(arg_dict['compose_dir']) / arg_dict['cast'].run_dir))

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
        forcing_dirs: list = [],
        ncores: int = 1
    ):
        """ Instantiate a Cycle object.
        Args:
            init_times: A required list of datetime.datetime objects which specify the
                restart time of each cast in the cycle. (Same for deterministic
                and ensemble cycle simultions).
            restart_dirs:
                Deterministic: a required list of either strings or pathlib.Path objects.
                Ensemble: a required list of lists. The outer list is for the cycles
                    "casts" requested in init_times. The inner list is for each ensemble member
                    in the cast.
                The following rules are applied to the individual entires:
                1) A dot or a null string (are identical pathlib.Path objects and) mean
                   "do nothing" with respect to the default path in the domain.
                2) An existing path/file is used/kept (a non-existent path is not, gives
                   an error).
                3) A negative integer in units hours, pointing to a previous cast in the
                   cycle.
                4) Other wise, value error raised.
            forcing_dirs: optional
                Deterministic: list of either strings or pathlib.Path objects
                Ensemble: A list of lists, as for restart_dirs.
                See restart_dirs for usage rules.
            ncores: integer number of cores for running parallelizable methods (not the
                casts themselves). For an ensemble cycle, setting this value > 1 will
                force the ensemble.ncores = 1.
        """

        self.casts = []
        """list: a list of 'casts' which are the individual simulations in the cycle object."""

        self._init_times = []
        """list: required list of datetime.datetime objects which specify the restart time of
        each cast in the cycle."""

        self._restart_dirs = []
        """list: required list of either strings or pathlib.Path objects where the
        following rules are applied:
        1) A dot or a null string (are identical pathlib.Path objects and) mean "do nothing"
           with respect to the default path in the domain.
        2) An existing path/file is kept.
        3) A negative integer is units hours, pointing to a previous cast in the cycle.
        4) Other wise, value error raised.
        """

        self._forcing_dirs = []
        """list: optional list of either strings or pathlib.Path objects. See
        _restart_dirs for usage rules."""

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
    # JLM todo: check/revise this...

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
            raise TypeError('Object is not of a type expected for a CycleSimulation.')

    def _addinittimes(self, init_times: list):
        """Private method to add init times to a CycleSimulation
        Args:
            init_times: a list of datetime.datetime objects.
        """
        if not all([isinstance(ii, datetime.datetime) for ii in init_times]):
            raise ValueError('List object not all datetime.datetime objects, as expected')
        self._init_times = copy.deepcopy(init_times)

    def _add_restart_forcing_dirs(self, dirs_list, identifier):
        """Private method to common to adding forcing and restart directories
        Args:
            dirs_list: deterministic: a list of dirs,  ensemble: a list of lists of dirs
            identifier: string for error messages to identify if restart or forcing dirs
                are problematic.
        """
        # Check the length
        def check_len_init(the_list):
            if len(self._init_times) != len(the_list):
                raise ValueError("Length of " + identifier + " does not match that of init_times.")

        def int_to_str(var):
            if type(var) is int:
                return str(var)
            return var

        deterministic_types = [str, int, pathlib.Path, pathlib.PosixPath]
        ensemble_types = [list]

        if all([type(ii) in deterministic_types for ii in dirs_list]):
            check_len_init(dirs_list)
            return_list = [pathlib.Path(int_to_str(cc)) for cc in dirs_list]

        elif all([type(ii) in ensemble_types for ii in dirs_list]):
            check_len_init(dirs_list)
            return_list = []
            for rr in dirs_list:
                if len(rr) != len(dirs_list[0]):
                    raise ValueError("Inconsistent ensemble length by implied by " + identifier)
                # The ensemble length is unknown, it's implied by len(rr)
                if all([type(ii) in deterministic_types for ii in rr]):
                    return_list.append([pathlib.Path(int_to_str(cc)) for cc in rr])
                else:
                    raise ValueError("Types in ensemble " + identifier + " argument "
                                     "are not appropriate.")

        else:
            raise ValueError("Types in " + identifier + " argument are not appropriate.")

        return return_list

    def _addforcingdirs(self, forcing_dirs: list):
        """Private method to add forcing dirs to a Cycle.
        Args:
            forcing_dirs: a list of str objects.
        """
        self._forcing_dirs = self._add_restart_forcing_dirs(forcing_dirs, 'forcing_dirs')

    def _addrestartdirs(self, restart_dirs: list):
        """Private method to add init times to a CycleSimulation
        Args:
            restart_dirs: deterministic cycle takes a list of str objects, an
            ensemble cycle takes a list (for each cycle) of lists of str objects (for the
            ensemble).
        """
        self._restart_dirs = self._add_restart_forcing_dirs(restart_dirs, 'restart_dirs')

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
        if not all([isinstance(ii, list) for ii in self._restart_dirs]) or \
           not all([isinstance(ii, list) for ii in self._forcing_dirs]):
            raise ValueError("An ensemble cycle simulation requires the restart_dirs to be "
                             "a list of lists.")
        common_msg = "Ensemble to add has inconsistent length with existing cycle"
        if len(self._restart_dirs) > 0 and len(ens) != len(self._restart_dirs[0]):
            raise ValueError(common_msg + " restart_dirs")
        if len(self._forcing_dirs) > 0 and len(ens) != len(self._forcing_dirs[0]):
            raise ValueError(common_msg + " forcing_dirs")

        ens_copy = copy.deepcopy(ens)
        # Ensure that the jobs and scheduler are empty and None
        ens_copy.jobs = []
        ens_copy.scheduler = None
        # Dont let multiprocessing use multiprocessing.
        if self.ncores > 1:
            ens_copy.ncores = 1
        self._ensemble = ens_copy

    def compose(
        self,
        symlink_domain: bool = True,
        force: bool = False,
        check_nlst_warn: bool = False,
        rm_casts_from_memory: bool = True,
        rm_members_from_memory: bool = True
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
        current_dir = pathlib.Path(os.getcwd())
        current_dir_files = list(current_dir.rglob('*'))
        if len(current_dir_files) > 0 and force is False:
            raise FileExistsError('Unable to compose, current working directory is not empty. \n'
                                  'Change working directory to an empty directory with os.chdir()')

        if '_simulation' in dir(self):
            cast_prototype = '_simulation'
        else:
            if '_ensemble' not in dir(self):
                raise ValueError("The cycle does not contain a _simulation or an _ensemble.")
            cast_prototype = '_ensemble'

        if len(self) < 1:
            raise ValueError("There are no casts (init_times) to compose.")

        self._compose_dir = pathlib.Path(os.getcwd())

        # Allowing forcing_dirs to be optional.
        if self._forcing_dirs == []:
            if cast_prototype == '_simulation':
                self._forcing_dirs = [pathlib.Path('.')] * len(self)
            else:
                self._forcing_dirs = \
                    [([pathlib.Path('.') for _ in range(len(self.__dict__[cast_prototype]))])
                     for cc in range(len(self))]

        # An ensemble must have a compiled model.
        if cast_prototype == '_simulation':
            # compile the model (once) before setting up the casts.
            if self._simulation.model.compile_log is None:
                comp_dir = self._compose_dir / 'compile'
                self._simulation.model.compile(comp_dir)

        # Set the ensemble jobs on the casts before composing (this is a loop over the jobs).
        if self.ncores == 1:

            self.casts = [
                parallel_compose_casts(
                    {
                        'prototype': self.__dict__[cast_prototype],
                        'init_time': init_time,
                        'restart_dir': restart_dir,
                        'forcing_dir': forcing_dir,
                        'job': self._job,
                        'scheduler': self._scheduler,
                        'rm_members_from_memory': rm_members_from_memory,
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
                        'rm_members_from_memory': rm_members_from_memory,
                    } for init_time, restart_dir, forcing_dir in zip(
                        self._init_times,
                        self._restart_dirs,
                        self._forcing_dirs
                    )
                    )
                )

        # Return from indivdual compose.
        os.chdir(self._compose_dir)

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
        n_concurrent: int = 1,
        teams: bool = False,
        teams_exe_cmd: str = None,
        teams_exe_cmd_nproc: int = None,
        teams_node_file: dict = None,
        env: dict = None,
        teams_dict: dict = None
    ):
        """Run the cycle of simulations.
        Inputs:
            n_concurrent: int = 1, Only used for non-team runs.
            teams: bool = False, Use teams?
            teams_exe_cmd: str, The mpi-specific syntax needed. For
                example: 'mpirun --host {hostname} -np {nproc} {cmd}'
            teams_exe_cmd_nproc: int, The number of cores per model/wrf_hydro
                simulation to be run.
            teams_node_file: dict = None, Optional file that acts like a node
                file. It is not currently implemented but the key specifies the
                scheduler format that the file follows. An example pbs node
                file is in tests/data and this argument is used here to test
                without a sched.
            env: dict = None, optional envionment to pass to the run.
            teams_dict: dict, Skip the arguments if you already have a
                teams_dict to use (backwards compatibility)
        Outputs: 0 for success.
        """

        # Save the ensemble object out to the ensemble directory before run
        # The object does not change with the run.
        path = pathlib.Path(self._compose_dir).joinpath('WrfHydroCycle.pkl')
        self.pickle(path)

        if teams or teams_dict is not None:
            if teams_dict is None and teams_exe_cmd is None:
                raise ValueError("The teams_exe_cmd is required for using teams.")

            if teams_dict is None:
                teams_dict = assign_teams(
                    self,
                    teams_exe_cmd=teams_exe_cmd,
                    teams_exe_cmd_nproc=teams_exe_cmd_nproc,
                    teams_node_file=teams_node_file,
                    env=env
                )

            with multiprocessing.Pool(len(teams_dict), initializer=mute) as pool:
                exit_codes = pool.map(
                    parallel_teams_run,
                    (
                        {'obj_name': 'casts',
                         'team_dict': team_dict,
                         'compose_dir': self._compose_dir,
                         'env': env}
                        for (key, team_dict) in teams_dict.items()
                    )
                )

            # # Keep around for serial testing/debugging
            # exit_codes = [
            #     parallel_teams_run(
            #         {'obj_name': 'casts',
            #          'team_dict': team_dict,
            #          'compose_dir': self._compose_dir,
            #          'env': env})
            #     for (key, team_dict) in teams_dict.items()
            # ]

            exit_code = int(not all([list(ee.values())[0] == 0 for ee in exit_codes]))

        elif n_concurrent > 1:
            with multiprocessing.Pool(n_concurrent, initializer=mute) as pool:
                exit_codes = pool.map(
                    parallel_run_casts,
                    ({'cast': cc, 'compose_dir': self._compose_dir} for cc in self.casts)
                )
            exit_code = int(not all([ee == 0 for ee in exit_codes]))

        else:
            # Keep the following for debugging: Run it without pool.map
            exit_codes = [
                parallel_run_casts({'cast': cc, 'compose_dir': self._compose_dir})
                for cc in self.casts
            ]
            exit_code = int(not all([ee == 0 for ee in exit_codes]))

        # Return to the cycle dir.
        os.chdir(self._compose_dir)
        return exit_code

    def pickle(self, path: str):
        """Pickle ensemble sim object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)
