import ast
from boltons.iterutils import remap, get_path
import copy
#import datetime
import multiprocessing
import pathlib
from typing import Union
import os
#import pickle
#import shlex
#import subprocess
#import time
#import uuid

from .ensemble_tools import DeepDiffEq, dictify, get_sub_objs

from .job import Job
from .schedulers import Scheduler
from .simulation import Simulation

#from .job_tools import solve_model_start_end_times


def parallel_compose_addjobs(arg_dict):
    for jj in arg_dict['jobs']:
        arg_dict['member'].add(jj)
    return arg_dict['member']


def parallel_compose_addscheduler(arg_dict):
    arg_dict['member'].add(arg_dict['scheduler'])
    return arg_dict['member']


def parallel_compose(arg_dict):
    os.chdir(str(arg_dict['ens_dir']))
    os.mkdir(str(arg_dict['member'].run_dir))
    os.chdir(str(arg_dict['member'].run_dir))
    arg_dict['member'].compose()
    return arg_dict['member']


# Classes for constructing and running a wrf_hydro simulation
class EnsembleSimulation(object):
    """ TODO
    """

    def __init__(
        self,
        ncores: int=1
    ):
        """ TODO """

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
    # # Removed these two until it's obvious we need them
    # # 3) description
    # # 4) forcing_source_dir

    def add(
        self,
        obj: Union[list, Scheduler, Job]
    ):
        """Add an approparite object to an EnsembleSimulation, such as a Simulation, Job, or
        Scheduler"""
        if isinstance(obj, list) or isinstance(obj, Simulation):
            self._addsimulation(obj)
        elif issubclass(type(obj), Scheduler):
            self._addscheduler(obj)
        elif isinstance(obj, Job):
            self._addjob(obj)
        else:
            raise TypeError('obj is not of a type expected for a EnsembleSimulation')

    def _addscheduler(self, scheduler: Scheduler):
        """Private method to add a Scheduler to a Simulation
        Args:
            scheduler: The Scheduler to add
        """
        self.scheduler = copy.deepcopy(scheduler)

    def _addjob(self, job: Job):
        """Private method to add a job to a Simulation
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
        """Private method to add a Model to a Simulation
        Args:
            model: The Model to add
        """

        if(type(sims) is Simulation):
            sims = [copy.deepcopy(sims)]

        for mm in sims:

            if type(mm) is not Simulation:
                raise ValueError("A non-simulation object can not be "
                                 "added to the ensemble members")

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
        for mm in range(len(self.members)):
            if not hasattr(self.members[mm], 'number'):
                self.members[mm].number = "%03d" % (mm,)
                self.members[mm].run_dir = 'member_' + self.members[mm].number

    # A quick way to setup a basic ensemble from a single sim.
    def replicate_member(
        self,
        N: int,
        copy_members: bool=True
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

        if len(self) == 1:
            print('Ensemble is of length 1, no differences.')
            return {}

        mem_0_ref_dict = dictify(self.members[0])

        # TODO(JLM): Could this be parallelized?
        all_diff_keys = set({})
        for ii in range(1, len(self)):
            mem_ii_ref_dict = dictify(self.members[ii])
            diff = DeepDiffEq(mem_0_ref_dict, mem_ii_ref_dict, eq_types={pathlib.PosixPath})

            unexpected_diffs = set(diff.keys()) - set(['values_changed'])
            if len(unexpected_diffs):
                unexpected_diffs1 = {uu: diff[uu] for uu in list(unexpected_diffs)}
                raise ValueError(
                    'Unexpected attribute differences between ensemble members:',
                    unexpected_diffs1
                )

            diff_keys = list(diff['values_changed'].keys())
            all_diff_keys = all_diff_keys | set([ss.replace('root', '') for ss in diff_keys])

        # TODO(JLM): What is this doing? Comment.
        # Without digging in, i think it is translating hierarchical dict entries to tuples.
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
        for mm in range(len(self)):
            new_value = values[mm]
            update_obj_dict(self.members[mm], att_tuple)

    def compose(
        self,
        symlink_domain: bool=True,
        force: bool = False,
        rm_members_from_memory: bool = True
    ):
        """Ensemble compose simulation directories and files
        Args:
            symlink_domain: Symlink the domain files rather than copy
            force: Compose into directory even if not empty. This is considered bad practice but
            is necessary in certain circumstances.
            rm_members_from_memory: Most applications will remove the members from the 
            ensemble object upon compose. Testing and other reasons may keep them around.
        """

        if len(self) < 1:
            raise ValueError("There are no member simulations to compose.")
        
        # Set the pool for the following parallelizable operations
        pool = multiprocessing.Pool(self.ncores)

        # 1) Set the ensemble jobs on the members before composing (this is a loop over the jobs).
        self.members = pool.map(
            parallel_compose_addjobs,
            ({'member': mm, 'jobs': self.jobs} for mm in self.members)
        )

        # 2) Set the ensemble scheduler (not a loop)
        self.members = pool.map(
            parallel_compose_addscheduler,
            ({'member': mm, 'scheduler': self.scheduler} for mm in self.members)
        )

        # 3) Ensemble compose
        current_dir = pathlib.Path(os.getcwd())
        current_dir_files = list(current_dir.rglob('*'))
        if len(current_dir_files) > 0 and force is False:
            raise FileExistsError('Unable to compose, current working directory is not empty and '
                                  'force is False. '
                                  'Change working directory to an empty directory with os.chdir()')
        ens_dir = current_dir
        self.members = pool.map(
            parallel_compose,
            ({'member': mm, 'ens_dir': ens_dir} for mm in self.members)
        )
#        parallel_compose({'member': self.members[0], 'ens_dir': ens_dir})

        return True
