import ast
from boltons.iterutils import remap, get_path
import copy
import datetime
import pathlib
import pickle
import shlex
import subprocess
import time
import uuid

from .wrfhydroclasses import WrfHydroRun
from .ensemble_tools import DeepDiffEq, dictify, get_sub_objs

from .job_tools import \
    get_user, \
    solve_model_start_end_times


def copy_member(
    member,
    do_copy: bool
):
    if do_copy:
        return(copy.deepcopy(member))
    else:
        return(member)


# ########################
# Classes for constructing and running a wrf_hydro simulation
class WrfHydroEnsembleSetup(object):
    """Class for a WRF-Hydro model, which consitutes the model source code and compiled binary.
    """
    def __init__(
        self,
        members: list,
        ensemble_dir: str=''
    ):
        """Instantiate a WrfHydroEnsembleSim object.
        Args:
            members: 
            ensemble_dir: Optional, 
        Returns:
            A WrfHydroEnsembleSim object.
        """
        self.__members = []
        self.members = members
        self.__diffs_dict = {}
        """list: of WrfHydroSim objects."""

    def __len__(self):
        return( len(self.members) )

    # The "canonical" name for len
    @property
    def N(self):
        return(self.__len__())

    # Data to store with the "member" simulations, conceptually this
    # data belongs to the members:
    # 1) member number
    # 2) description
    # 3) member_dir
    # 4) forcing_source_dir
    #
    # Ensemblize the individual members.

    @property
    def members(self):
        return(self.__members)

    @members.setter
    def members(
        self,
        new_members: list, 
        copy_members: bool=True
    ):

        if( type(new_members) is not list ):
            new_members = [ new_members ]

        for nn in new_members:
            self.__members.append(copy_member(nn, copy_members))
            # If copying an existing ensemble member, nuke the metadata
            # number is the detector for all ensemble metadata.
            if hasattr(nn, 'number'):
                delattr(self.__members[len(self.__members)-1], 'number')

        # Put refs to these properties in the ensemble objects
        for mm in range(len(self.__members)):
            if not hasattr(self.__members[mm], 'number'):
                self.__members[mm].number = "%03d" % (mm,)
                self.__members[mm].description = ''
                self.__members[mm].run_dir = 'member_' + self.__members[mm].number
                self.__members[mm].forcing_source_dir = ''


    # A quick way to setup a basic ensemble from a single sim.
    def replicate_member(self,
                         N: int,
                         copy_members: bool=True):
        if self.N > 1:
            print('WTF mate?')
        else:
            self.members = [ self.members[0] for nn in range(N-1) ]


    # The diffs_dict attribute has getter (@property) and setter methods.
    # The get method summarizes all differences across all the attributes of the
    #   members list attribute and (should) only report member attributes when there
    #   is at least one difference between members.
    # The setter method is meant as a convenient way to specify the differences in
    #   member attributes across the ensemble.


    @property        
    def diffs_dict(self):

        if len(self) == 1:
            print('Ensemble is of lenght 1, no differences.')
            return {}

        mem_0_ref_dict = dictify(self.members[0])

        all_diff_keys=set({})
        for ii in range(1,len(self)):
            mem_ii_ref_dict = dictify(self.members[ii])
            diff = DeepDiffEq(mem_0_ref_dict, mem_ii_ref_dict, eq_types={pathlib.PosixPath})

            unexpected_diffs = set(diff.keys()) - set(['values_changed'])
            if len(unexpected_diffs):
                unexpected_diffs1 = { uu: diff0[uu] for uu in list(unexpected_diffs) }
                raise ValueError(
                    'Unexpected attribute differences between ensemble members:',
                    unexpected_diffs1
                )

            diff_keys = list(diff['values_changed'].keys())
            all_diff_keys = all_diff_keys | set([ ss.replace('root','') for ss in diff_keys ])

        diff_tuples = [ss.replace('][',',') for ss in list(all_diff_keys)]
        diff_tuples = [ss.replace('[','(') for ss in list(diff_tuples)]
        diff_tuples = [ss.replace(']',')') for ss in list(diff_tuples)]
        diff_tuples = [ast.literal_eval(ss) for ss in list(diff_tuples)]

        self.__diffs_dict = {}
        for dd in diff_tuples:
            self.__diffs_dict[dd] = [ get_path(dictify(mm), dd) for mm in self.members ]

        return(self.__diffs_dict)


    def set_diffs_dict(
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
                #print(superpath) #

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

        for ii in range(len(self)):
            new_value = values[ii]
            #print(new_value)
            update_obj_dict(self.members[ii], att_tuple)


class WrfHydroEnsembleRun(object):
    def __init__(
        self,
        ens_setup: WrfHydroEnsembleSetup,
        run_dir: str,
        rm_existing_run_dir = False,
        mode: str='r',
        jobs: list=None
    ):

        self.ens_setup = copy.deepcopy(ens_setup)
        """WrfHydroSetup: The WrfHydroSetup object used for the run"""

        # TODO(JLM): check all the setup members have to have rundirs with same path as run_dir
        self.run_dir = pathlib.PosixPath(run_dir)
        """pathlib.PosixPath: The location of where the jobs will be executed."""

        self.jobs_completed = []
        """Job: A list of previously executed jobs for this run."""
        self.jobs_pending = []
        """Job: A list of jobs *scheduled* to be executed for this run
            with prior job dependence."""
        self.job_active = None
        """Job: The job currently executing."""
        self.object_id = None
        """str: A unique id to join object to run directory."""
        self.members = []
        """List: ensemble of Run Objects."""

        # #################################

        # Make run_dir directory if it does not exist.
        if self.run_dir.is_dir() and not rm_existing_run_dir:
            raise ValueError("Run directory already exists and rm_existing_run_dir is False.")

        if self.run_dir.exists():
            shutil.rmtree(str(self.run_dir))
            self.run_dir.mkdir(parents=True)

        # This sets up the runs. Writes WrfHydroRun.pkl objects to each dir.
        for mm in self.ens_setup.members:
            self.members.append(WrfHydroRun(
                mm,
                run_dir = mm.run_dir,
                deepcopy_setup=False
            )
        )

        if jobs:
            self.add_jobs(jobs)
        else:
            self.collect_output()
            self.pickle()


    def add_jobs(
        self,
        jobs: list
    ):
        """Add an Ensemble Run Job (array)."""

        # Dont tamper with the passed object, let it remain a template in the calling level.
        jobs = copy.deepcopy(jobs)

        if type(jobs) is not list:
            jobs = [jobs]

        for jj in jobs:

            # Attempt to add the job
            if jj.scheduler:

                # A scheduled job can be appended to the jobs.pending list if
                # 1) there are no active or pending jobs
                # 2) if it is (made) dependent on the last active or pending job.

                # Get the job id of the last active or pending job.
                last_job_id = None
                if self.job_active:
                    last_job_id = self.job_active.sched_job_id
                if len(self.jobs_pending):
                    last_job_id = self.jobs_pending[-1].scheduler.sched_job_id

                # Check the dependency on a previous job
                if last_job_id is not None:
                    if jj.scheduler.afterok is not None and jj.scheduler.afterok != last_job_id:
                        raise ValueError("The job's dependency/afterok conflicts with reality.")
                    jj.scheduler.afterok = last_job_id
                else: 
                    if jj.scheduler.afterok is not None:
                        raise ValueError("The job's dependency/afterok conflicts with reality.")


            # Set submission-time job variables here.
            jj.user = get_user()
            job_submission_time = datetime.datetime.now()
            jj.job_submission_time = str(job_submission_time)
            jj.job_date_id = 'job_' + str(len(self.jobs_completed) +
                                          bool(self.job_active) +
                                          len(self.jobs_pending))

            # alternative" '{date:%Y-%m-%d-%H-%M-%S-%f}'.format(date=job_submission_time)
            jj.scheduler.array_size = len(self.members)

            for mm in self.members:
                mm.add_jobs(jj)

            self.jobs_pending.append(jj)

        self.collect_output()
        self.pickle()


    def run_jobs(self):

        # make sure all jobs are either scheduled or interactive?
        
        if self.job_active is not None:
            raise ValueError("There is an active ensemble run.")

        if self.jobs_pending[0].scheduler:

            run_dir = self.run_dir

            # submit the jobs_pending.
            job_afterok = None
            hold = True

            # For each the job arrays,
            for ii, _ in enumerate(self.jobs_pending):

                #  For all the members,
                for mm in self.members:

                    # Set the dependence into all the members jobs,
                    jj = mm.jobs_pending[ii]
                    jj.scheduler.afterok = job_afterok

                    # Write everything except the submission script,
                    # (Job has is_job_array == TRUE)
                    jj.schedule(mm.run_dir, hold=hold)


                # Submit the array job for all the members, using the last member [-1] to do so.
                job_afterok = jj.schedule(self.run_dir, hold=hold, submit_array=True)
                # Keep that info in the object.
                self.jobs_pending[ii].sched_job_id = job_afterok
                # This is the "sweeper" job for job arrays.
                #job_afterok = self.jobs_pending[ii].collect_job_array(str_to_collect_ensemble)
                # Only hold the first job array
                hold = False

            self.job_active = self.jobs_pending.pop(0)
            self.pickle()
            self.members[-1].jobs_pending[0].release()
            self.destruct()
            return run_dir

        else:

            for jj in range(0, len(self.jobs_pending)):

                self.job_active = self.jobs_pending.pop(0)
                self.job_active.run(self.run_dir)
                self.jobs_completed.append(self.job_active)
                self.job_active = None
                self.collect_output()
                self.pickle()


    str_to_collect_ensemble = (
        "import pickle \n"
        "import sys \n"
        "import wrfhydropy \n"
        "ens_run = pickle.load(open('WrfHydroEnsembleRun.pkl', 'rb')) \n",
        "ens_run.collect_ensemble_runs() \n"
        "sys.exit()"
    )[0]


    def collect_ensemble_runs(
        self
    ):
        """Collect a completed job array. """
        def n_jobs_not_complete(run_dir):
            the_cmd = '/bin/bash -c "ls member_*/.job_not_complete 2> /dev/null | wc -l"'
            return subprocess.run(shlex.split(the_cmd), cwd=run_dir).returncode
        while n_jobs_not_complete(self.run_dir) != 0:
            time.sleep(6)

        if self.job_active:
            self.jobs_completed.append(self.job_active)
            self.job_active = None

        for ii, _ in enumerate(self.members):
            self.members[ii] = self.members[ii].unpickle()

        self.collect_output()
        self.pickle()


    def collect_output(self):
        for mm in self.members:
            mm.collect_output()



    def pickle(self):
        """Pickle the Run object into its run directory. Collect output first."""

        # create a UID for the run and save in file
        self.object_id = str(uuid.uuid4())
        with open(self.run_dir.joinpath('.uid'), 'w') as f:
            f.write(self.object_id)

        # Save object to run directory
        # Save the object out to the compile directory
        with open(self.run_dir.joinpath('WrfHydroEnsembleRun.pkl'), 'wb') as f:
            pickle.dump(self, f, 2)


    def unpickle(self):
        """ Load run object from run directory after a scheduler job. """
        with open(self.run_dir.joinpath('WrfHydroEnsembleRun.pkl'), 'rb') as f:
            return(pickle.load(f))


    def destruct(self):
        """ Pickle first. This gets rid of everything but the methods."""
        self.pickle()
        print("Jobs have been submitted to  the scheduler: This run object will now self destruct.")
        self.__dict__ = {}
