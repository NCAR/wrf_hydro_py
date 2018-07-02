import math
import warnings
from abc import ABC, abstractmethod

#This is maybe a little too smart, don't auto pick scheduler, it should be expplicity suplied via
# the scheduler object
# def get_sched_name():
#     """Tries to find qsub, then sbatch. Returns "PBS" if qsub
#     is found, else returns "slurm" if sbatch is found, else returns
#     "other" if neither is found. """
#     if find_executable("qsub") is not None:
#         return "PBS"
#     elif find_executable("sbatch") is not None:
#         return "slurm"
#     else:
#         return None


class Scheduler(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def add_job(self):
        pass

    @abstractmethod
    def submit(self):
        pass



class PBSCheyenne(Scheduler):
    """A PBS/torque scheduler Job object.

    Initialize either with all the parameters, or with 'qsubstr' a PBS submit script as a string.
    If 'qsubstr' is given, all other arguments are ignored and set using Job.read().

    Variables
        On cheyenne, PBS attributes are described in `man qsub` and `man pbs_resources`.
        See also: https://www2.cisl.ucar.edu/resources/computational-systems/cheyenne/running-jobs/submitting-jobs-pbs
        A dictionary can be constructed in advance from specification files by the function
        get_sched_args_from_specs().

    Var Name    default            example         Notes
      PBS usage on Chyenne
    -------------------------------------------------------------------------------------------
    name                           "my_job"
      -N
    account                        "NRAL0017"
      -A
    email_when                     "a","abe"       "a"bort, "b"efore, "e"nd
      -m
    email_who   "${USER}@ucar.edu" "johndoe@ucar.edu"
      -M
    queue       "regular"           "regular"
      -q
    walltime    "12:00"             "10:00:00"      Seconds coerced. Appropriate run times are best.
      -l walltime=
    afterok                         "12345:6789"   Begin after successful completion of job1:job2:etc.
      -W depends=afterok:

    array_size  -J              None               16             integer
    grab_env    -V              None               True           logical

    Sepcify: nproc, nnodes, nproc + nnodes, nproc + ppn,  nnodes + ppn
    nproc                                          500             Number of procs
    nodes                                          4               Number of nodes
    ppn                        Default:            24              Number of procs/node
                               machine_spec_file.cores_per_node

    modules

    -*-*-*-*-*-*-*-  FOLLOWING NOT TESTED ON CHEYENNE  -*-*-*-*-*-*-*-

    pmem        -l pmem=                          "2GB"           Default is no restriction.
    exetime     -a                                "1100"          Not tested
    """

    def __init__(
            self,
            job_name: str,
            account: str,
            nproc: int = None,
            nnodes: int = None,
            ppn: int = None,
            email_when: str = None,
            email_who: str = None,
            queue: str = 'regular',
            walltime: str = "12:00:00",
            wait_for_complete: bool = True,
            monitor_freq_s: int = None,
            afterok: str = None,
            array_size: int = None,
            pmem: str = None,
            grab_env: str = False,
            exetime: str = None,
            job_id: str = None
    ):

        # Declare attributes.
        # Required
        self.job_name = job_name
        self.account = account

        # Defaults in arglist
        self.email_when = email_when
        self.queue = queue
        self.afterok = afterok
        self.array_size = array_size
        self.email_who = email_who
        self.walltime = walltime

        # Construction
        self._nproc = nproc
        self._nnodes = nnodes
        self._ppn = ppn
        self._job_id = job_id
        self.wait_for_complete = wait_for_complete
        self.monitor_freq_s = monitor_freq_s

        # TODO(JLM): Deal with setting ppn from machine_spec_file.
        self.solve_nodes_cores()

        self.nproc_last_node = (self.nproc - (self.nnodes * self.ppn)) % self.ppn
        if self.nproc_last_node > 0:
            if self.nproc_last_node >= self.ppn:
                raise ValueError('nproc - (nnodes * ppn) = {0} >= ppn'.format(self.nproc_last_node))

        # TODO(JLM): the term job here is a bit at odds with where I'm putting the attributes
        # sched_id (this requires some refactoring with job_tools)? job_script seems ok, however.
        # sched_job_id is set at submission
        self.sched_job_id = None

        self.job_script = None

        # PBS has a silly stream buffer that 1) has a limit, 2) cant be seen until the job ends.
        # Separate and standardize the stdout/stderr of the exe_cmd and the scheduler.

        # The path to the model stdout&stderr
        self._stdout_exe = "{run_dir}/{job_date_id}.{sched_job_id}.stdout"
        self._stderr_exe = "{run_dir}/{job_date_id}.{sched_job_id}.stderr"

        # Tracejob file which holds performance information
        self._tracejob_file = "{run_dir}/{job_date_id}.{sched_job_id}." + self.sched_name + ".tracejob"

        # Dot files for the pbs stdout&stderr files, both temp and final.
        # The initial path to the PBS stdout&stderr, during the job
        self._stdout_pbs_tmp = "{run_dir}/.{job_date_id}." + self.sched_name + ".stdout"
        self._stderr_pbs_tmp = "{run_dir}/.{job_date_id}." + self.sched_name + ".stderr"
        # The eventual path to the " + self.sched_name + " stdout&stderr, after the job
        self._stdout_pbs = "{run_dir}/.{job_date_id}.{sched_job_id}." + self.sched_name + ".stdout"
        self._stderr_pbs = "{run_dir}/.{job_date_id}.{sched_job_id}." + self.sched_name + ".stderr"

        # A three state variable. If "None" then script() can be called.
        # bool(None) is False so
        # None = submitted = True while not_submitted = False
        self.not_submitted = True

        # A status that depends on job being submitted and the .job_not_complete file
        # not existing being missing.
        self._sched_job_complete = False

    def add_job(self):
        pass
    def submit(self):
        pass

    def solve_nodes_cores(self):
        if None not in [self._nproc, self._nnodes, self._ppn]:
            warnings.warn("Not currently checking consistency of nproc, nnodes, ppn.")
            return

        if not self._nproc and self._nnodes and self._ppn:
            self._nproc = self._nnodes * self._ppn
        if not self._nnodes and self._nproc and self._ppn:
            self._nnodes = math.ceil(self._nproc / self._ppn)
        if not self._ppn and self._nnodes and self._nproc:
            self._ppn = math.ceil(self._nproc / self._nnodes)

        if None in [self._nproc, self._nnodes, self._ppn]:
            raise ValueError("Not enough information to solve all of nproc, nnodes, ppn.")

    @property
    def nproc(self):
        self.solve_nodes_cores()
        return self._nproc

    @nproc.setter
    def nproc(self, value):
        self._nproc = value

    @property
    def nnodes(self):
        self.solve_nodes_cores()
        return self._nnodes

    @nnodes.setter
    def nnodes(self, value):
        self._nnodes = value

    @property
    def ppn(self):
        self.solve_nodes_cores()
        return self._ppn

    @ppn.setter
    def ppn(self, value):
        self._ppn = value