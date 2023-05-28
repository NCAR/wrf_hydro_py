import ast
from boltons.iterutils import remap, get_path
import copy
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

from .ensemble_tools import DeepDiffEq, dictify, get_sub_objs, mute
from .job import Job
from .schedulers import Scheduler
from .simulation import Simulation
from .teams import parallel_teams_run, assign_teams
from .outputdiffs import check_unprocessed_diffs


def parallel_compose_addjobs(arg_dict):
    """Parallelizable function to add jobs to EnsembleSimuation."""
    for jj in arg_dict['jobs']:
        arg_dict['member'].add(jj)
    return arg_dict['member']


def parallel_compose_addscheduler(arg_dict):
    """Parallelizable function to add a scheduler to EnsembleSimuation."""
    arg_dict['member'].add(arg_dict['scheduler'])
    return arg_dict['member']


def parallel_compose(arg_dict):
    """Parallelizable function to compose an EnsembleSimuation."""
    os.chdir(str(arg_dict['ens_dir']))
    mem = arg_dict['member']
    os.mkdir(str(mem.run_dir))
    os.chdir(str(mem.run_dir))
    mem.compose(**arg_dict['args'])
    mem.pickle_sub_objs()
    mem.pickle('WrfHydroSim.pkl')
    return mem


def parallel_run(arg_dict):
    """Parallelizable function to run an EnsembleSimuation."""
    if type(arg_dict['member']) is str:
        os.chdir(str(pathlib.Path(arg_dict['ens_dir']) / arg_dict['member']))
    else:
        os.chdir(str(pathlib.Path(arg_dict['ens_dir']) / arg_dict['member'].run_dir))
    mem_pkl = pickle.load(open("WrfHydroSim.pkl", "rb"))

    mem_pkl.run()
    return mem_pkl.jobs[0].exit_status


# Classes for constructing and running a wrf_hydro simulation
class EnsembleSimulation(object):
    """Class for a WRF-Hydro EnsembleSimulation object. The Ensemble Simulation object is used to
    orchestrate a set of 'N' WRF-Hydro simulations. It requires members with pre-compiled models
    and there are set and get methods across the ensemble (member_diffs & set_member_diffs). Jobs
    and scheduler set on the EnsembleSimulation object are set on all the members.
    """

    def __init__(
        self,
        ncores: int = 1
    ):
        """ Instantiates an EnsembleSimulation object. """

        self.members = []
        """list: a list of simulations which are the members of the ensemble."""

        self.__member_diffs = {}
        """dict: a dictionary containing the differences across all the members attributes."""

        self.jobs = []
        """list: a list containing Job objects"""

        self.scheduler = None
        """Scheduler: A scheduler object to use for each Job in self.jobs"""

        self.ncores = ncores
        """ncores: integer number of cores for running parallelizable methods."""

    def __len__(self):
        return(len(self.members))

    # The "canonical" name for len
    @property
    def N(self):
        return(self.__len__())

    # Metadata to store with the "member" simulations, conceptually this
    # data belongs to the members:
    # 1) member number
    # 2) member_dir

    def add(
        self,
        obj: Union[list, Scheduler, Job]
    ):
        """Add an approparite object to an EnsembleSimulation, such as a Simulation, Job, or
        Scheduler.
        Args:
            obj: the object to add.
        """
        if isinstance(obj, list) or isinstance(obj, Simulation):
            self._addsimulation(obj)
        elif issubclass(type(obj), Scheduler):
            self._addscheduler(obj)
        elif isinstance(obj, Job):
            self._addjob(obj)
        else:
            raise TypeError('obj is not of a type expected for a EnsembleSimulation')

    def _addscheduler(self, scheduler: Scheduler):
        """Private method to add a Scheduler to an EnsembleSimulation
        Args:
            scheduler: The Scheduler to add
        """
        self.scheduler = copy.deepcopy(scheduler)

    def _addjob(self, job: Job):
        """Private method to add a job to an EnsembleSimulation
        Args:
            job: The job to add
        """
        job = copy.deepcopy(job)
        # Postpone the following until compose and do it on the
        # individual members.
        # job._add_hydro_namelist(self.base_hydro_namelist)
        # job._add_hrldas_namelist(self.base_hrldas_namelist)
        self.jobs.append(job)

    def _addsimulation(
        self,
        sims: Union[list, Simulation]
    ):
        """Private method to add a Simulation to an EnsembleSimulation
        Args:
            model: The Model to add
        """

        if(type(sims) is Simulation):
            sims = [copy.deepcopy(sims)]

        for mm in sims:

            if type(mm) is not Simulation:
                raise ValueError("A non-simulation object can not be "
                                 "added as a simulation to the ensemble members.")

            if mm.model.compile_log is None:
                raise ValueError("Only simulations with compiled model objects "
                                 "can be added to an ensemble simulation.")

            # If copying an existing ensemble member, delete "number",
            # the detector for all ensemble metadata.
            mm_copy = copy.deepcopy(mm)
            if hasattr(mm, 'number'):
                delattr(mm_copy, 'number')

            # Ensure that the jobs and scheduler are empty and None
            mm_copy.jobs = []
            mm_copy.scheduler = None

            self.members.append(mm_copy)

        # Put refs to these properties in the ensemble objects
        for imem, mem in enumerate(self.members):
            if not hasattr(mem, 'number'):
                mem.number = "%03d" % (imem,)
                mem.run_dir = 'member_' + mem.number

    # A quick way to setup a basic ensemble from a single sim.
    def replicate_member(
        self,
        N: int,
        copy_members: bool = True
    ):
        if self.N > 1:
            raise ValueError('The ensemble must only have one member to replicate.')
        else:
            for nn in range(1, N):
                self.add(self.members[0])

    # -------------------------------------------------------
    # The member_diffs attribute has getter (@property) and setter methods.
    # The get method summarizes all differences across all the attributes of the
    #   members list attribute and (should) only report member attributes when there
    #   is at least one difference between members.
    # The setter method is meant as a convenient way to specify the differences in
    #   member attributes across the ensemble.

    @property
    def member_diffs(self):
        """Get method for ensemble member differences. Only differences are reported."""

        if len(self) == 1:
            print('Ensemble is of length 1, no differences.')
            return {}

        mem_0_ref_dict = dictify(self.members[0])

        # TODO(JLM): Could this be parallelized?
        all_diff_keys = set({})
        for imem, mem in enumerate(self.members):
            if imem == 0:
                continue
            mem_ii_ref_dict = dictify(mem)
            diff = DeepDiffEq(mem_0_ref_dict, mem_ii_ref_dict, eq_types={pathlib.PosixPath})

            unprocessed_diffs = diff.pop('unprocessed', [])
            if unprocessed_diffs:
                check_unprocessed_diffs(unprocessed_diffs)

            diff_keys = list(diff['values_changed'].keys())
            all_diff_keys = all_diff_keys | set([ss.replace('root', '') for ss in diff_keys])

        # This translates hierarchical dict entries to tuples.
        diff_tuples = [ss.replace('][', ',') for ss in list(all_diff_keys)]
        diff_tuples = [ss.replace('[', '(') for ss in list(diff_tuples)]
        diff_tuples = [ss.replace(']', ')') for ss in list(diff_tuples)]
        diff_tuples = [ast.literal_eval(ss) for ss in list(diff_tuples)]

        self.__member_diffs = {}
        for dd in diff_tuples:
            self.__member_diffs[dd] = [get_path(dictify(mm), dd) for mm in self.members]

        return(self.__member_diffs)

    def set_member_diffs(
        self,
        att_tuple: tuple,
        values: list
    ):
        """Set method for ensemble member differences. (Currently fails silently when
        requested fields are not found.)"""
        if type(values) is not list:
            values = [values]

        if len(values) == 1:
            the_value = values[0]
            values = [the_value for ii in range(len(self))]

        if len(values) != len(self):
            raise ValueError("The number of values supplied does not equal the number of members.")

        def update_obj_dict(obj, att_tuple):

            def visit(path, key, value):
                superpath = path + (key,)

                if superpath != att_tuple[0:len(superpath)]:
                    return True
                if len(superpath) == len(att_tuple):
                    return key, new_value
                return True

            the_remap = remap(obj.__dict__, visit)
            obj.__dict__.update(the_remap)
            for ss in get_sub_objs(obj.__dict__):
                att_tuple_0 = att_tuple
                att_tuple = att_tuple[1:]
                if len(att_tuple) > 0:
                    update_obj_dict(obj.__dict__[ss], att_tuple)
                att_tuple = att_tuple_0

        # TODO(JLM): This can be parallelized.
        for imem, mem in enumerate(self.members):
            new_value = values[imem]
            update_obj_dict(mem, att_tuple)

    def compose(
        self,
        symlink_domain: bool = True,
        force: bool = False,
        check_nlst_warn: bool = False,
        rm_members_from_memory: bool = True
    ):
        """Ensemble compose simulation directories and files
        Args:
            symlink_domain: Symlink the domain files rather than copy
            force: Compose into directory even if not empty. This is considered bad practice but
            is necessary in certain circumstances.
            rm_members_from_memory: Most applications will remove the members from the
            ensemble object upon compose. Testing and other reasons may keep them around.
            check_nlst_warn: Allow the namelist checking/validation to only result in warnings.
            This is also not great practice, but necessary in certain circumstances.
        """

        if len(self) < 1:
            raise ValueError("There are no member simulations to compose.")

        if self.ncores > 1:
            # Set the pool for the following parallelizable operations
            with multiprocessing.Pool(processes=self.ncores, initializer=mute) as pool:

                # Set the ensemble jobs on the members before composing (this is a loop
                # over the jobs).
                self.members = pool.map(
                    parallel_compose_addjobs,
                    ({'member': mm, 'jobs': self.jobs} for mm in self.members)
                )

                # Set the ensemble scheduler (not a loop)
                if self.scheduler is not None:
                    self.members = pool.map(
                        parallel_compose_addscheduler,
                        ({'member': mm, 'scheduler': self.scheduler} for mm in self.members)
                    )

        else:
            # Set the ensemble jobs on the members before composing (this is a loop
            # over the jobs).
            self.members = [
                parallel_compose_addjobs({'member': mm, 'jobs': self.jobs})
                for mm in self.members
            ]

            # Set the ensemble scheduler (not a loop)
            if self.scheduler is not None:
                self.members = [
                    parallel_compose_addscheduler({'member': mm, 'scheduler': self.scheduler})
                    for mm in self.members
                ]

        # Ensemble compose
        ens_dir = pathlib.Path(os.getcwd())
        self._compose_dir = ens_dir.resolve()
        ens_dir_files = list(ens_dir.rglob('*'))
        if len(ens_dir_files) > 0 and force is False:
            raise FileExistsError(
                'Unable to compose ensemble, current working directory is not empty and force '
                'is False. \nChange working directory to an empty directory with os.chdir()'
            )

        if self.ncores > 1:
            with multiprocessing.Pool(processes=self.ncores, initializer=mute) as pool:
                self.members = pool.map(
                    parallel_compose,
                    ({
                        'member': mm,
                        'ens_dir': ens_dir,
                        'args': {
                            'symlink_domain': symlink_domain,
                            'force': force,
                            'check_nlst_warn': check_nlst_warn
                        }
                    } for mm in self.members)
                )
        else:
            # Keep the following for debugging: Run it without pool.map
            self.members = [
                parallel_compose(
                    {
                        'member': mm,
                        'ens_dir': ens_dir,
                        'args': {
                            'symlink_domain': symlink_domain,
                            'force': force,
                            'check_nlst_warn': check_nlst_warn
                        }
                    }
                ) for mm in self.members
            ]

        # Return to the ensemble dir.
        os.chdir(ens_dir)

        # After successful compose, delete the members from memory and replace with
        # their relative dirs, if requested
        if rm_members_from_memory:
            self.rm_members()

    def rm_members(self):
        """Remove members from memory, replace with their paths."""
        run_dirs = [mm.run_dir for mm in self.members]
        self.members = run_dirs

    def restore_members(self, ens_dir: pathlib.Path = None, recursive: bool = True):
        """Restore members from disk, replace paths with the loaded pickle."""
        if ens_dir is not None:
            self._compose_dir = ens_dir
        if not hasattr(self, '_compose_dir'):
            raise ValueError('API change: please specify the ens_dir argument '
                             'to point to the ensemble location using a pathlib.Path.')
        if all([isinstance(mm, str) for mm in self.members]):
            member_sims = [
                pickle.load(self._compose_dir.joinpath(mm + '/WrfHydroSim.pkl').open('rb'))
                for mm in self.members
            ]
            self.members = member_sims
        if recursive:
            for mm in self.members:
                mm.restore_sub_objs()

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
        """Run the ensemble of simulations.
        Inputs:
            n_concurrent: int = 1, Only used for non-team runs.
            teams: bool = False, Use teams? See parallel_teams_run for
                details.
            teams_exe_cmd: str, The mpi-specific syntax needed. For
                example: 'mpirun --hosts {nodelist} -np {nproc} {cmd}'
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
        Returns: 0 for success.
        """
        ens_dir = os.getcwd()

        # Save the ensemble object out to the ensemble directory before run
        # The object does not change with the run.
        path = pathlib.Path(ens_dir).joinpath('WrfHydroEns.pkl')
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
                    env=env)

            with multiprocessing.Pool(len(teams_dict), initializer=mute) as pool:
                exit_codes = pool.map(
                    parallel_teams_run,
                    (
                        {'obj_name': 'members',
                         'team_dict': team_dict,
                         'compose_dir': ens_dir,
                         'env': env}
                        for (key, team_dict) in teams_dict.items()
                    )
                )

            # # Keep around for serial testing/debugging
            # exit_codes = [
            #     parallel_teams_run(
            #         {'obj_name': 'members',
            #          'team_dict': team_dict,
            #          'compose_dir': ens_dir,
            #          'env': env})
            #     for (key, team_dict) in teams_dict.items()
            # ]

            exit_code = int(not all([list(ee.values())[0] == 0 for ee in exit_codes]))

        elif n_concurrent > 1:

            with multiprocessing.Pool(n_concurrent, initializer=mute) as pool:
                exit_codes = pool.map(
                    parallel_run,
                    ({'member': mm, 'ens_dir': ens_dir} for mm in self.members)
                )
            exit_code = int(not all([ee == 0 for ee in exit_codes]))

        else:

            # Keep the following for debugging: Run it without pool.map
            exit_codes = [
                parallel_run({'member': mm, 'ens_dir': ens_dir}) for mm in self.members
            ]
            exit_code = int(not all([ee == 0 for ee in exit_codes]))

        os.chdir(ens_dir)
        return exit_code

    def pickle(self, path: str):
        """Pickle ensemble sim object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)

    def collect(self, output=True):
        for mm in self.members:
            mm.collect(output=output)
