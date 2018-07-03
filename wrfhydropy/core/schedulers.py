import math
import warnings
from abc import ABC, abstractmethod
from .job import Job
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
    def add_job(self,
                job: Job):
        pass

    @abstractmethod
    def schedule(self):
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

    def _compose_scheduled_python_script(
            py_run_cmd: str,
            model_exe_cmd: str
    ):
        jobstr = "#!/usr/bin/env python3\n"
        jobstr += "\n"

        jobstr += "import argparse\n"
        jobstr += "import datetime\n"
        jobstr += "import os\n"
        jobstr += "import pickle\n"
        jobstr += "import sys\n"
        jobstr += "import wrfhydropy\n"
        jobstr += "\n"

        jobstr += "parser = argparse.ArgumentParser()\n"
        jobstr += "parser.add_argument('--sched_job_id',\n"
        jobstr += "                    help='The numeric part of the scheduler job ID.')\n"
        jobstr += "parser.add_argument('--job_date_id',\n"
        jobstr += "                    help='The date-time identifier created by Schduler obj.')\n"
        jobstr += "args = parser.parse_args()\n"
        jobstr += "\n"

        jobstr += "print('sched_job_id: ', args.sched_job_id)\n"
        jobstr += "print('job_date_id: ', args.job_date_id)\n"
        jobstr += "\n"

        jobstr += "run_object = pickle.load(open('WrfHydroRun.pkl', 'rb'))\n"
        jobstr += "\n"

        jobstr += "# The lowest index jobs_pending should be the job to run. Verify that it \n"
        jobstr += "# has the same job_date_id as passed first, then set job to active.\n"
        jobstr += "if run_object.job_active:\n"
        jobstr += "    msg = 'There is an active job conflicting with this scheduled job.'\n"
        jobstr += "    raise ValueError(msg)\n"
        jobstr += "if not run_object.jobs_pending[0].job_date_id == args.job_date_id:\n"
        jobstr += "    msg = 'The first pending job does not match the passed job_date_id.'\n"
        jobstr += "    raise ValueError(msg)\n"
        jobstr += "\n"

        jobstr += "# Promote the job to active.\n"
        jobstr += "run_object.job_active = run_object.jobs_pending.pop(0)\n"

        jobstr += "# Set some run-time attributes of the job.\n"
        jobstr += "run_object.job_active.py_exe_cmd = \"" + py_run_cmd + "\"\n"
        jobstr += "run_object.job_active.scheduler.sched_job_id = args.sched_job_id\n"
        jobstr += "run_object.job_active.exe_cmd = \"" + model_exe_cmd + "\"\n"
        jobstr += "# Pickle before running the job.\n"
        jobstr += "run_object.pickle()\n"
        jobstr += "\n"

        jobstr += "print(\"Running the model.\")\n"
        jobstr += "run_object.job_active.job_start_time = str(datetime.datetime.now())\n"
        jobstr += "run_object.job_active.run(run_object.run_dir)\n"
        jobstr += "run_object.job_active.job_end_time = str(datetime.datetime.now())\n"
        jobstr += "\n"

        jobstr += "run_object.job_active._sched_job_complete = True\n"
        jobstr += "run_object.jobs_completed.append(run_object.job_active)\n"
        jobstr += "run_object.job_active = None\n"

        jobstr += "print(\"Collecting model output.\")\n"
        jobstr += "run_object.collect_output()\n"
        jobstr += "print(\"Job completed.\")\n"
        jobstr += "\n"

        jobstr += "run_object.pickle()\n"
        jobstr += "\n"
        jobstr += "sys.exit(0)\n"

        return jobstr

    def add_job(self,
                job: Job):

        if not job.submit_array:
            # Write python to be executed by the bash script given to the scheduler.
            # Execute the model from python script and the python script from the bash script:
            # swap their execution commands.

            model_exe_cmd = job.exe_cmd
            py_script_name = str(job.run_dir / (self.job_date_id + ".wrfhydropy.py"))
            # I think it's preferable to call the abs path, but in a job array that dosent work.
            if self.scheduler.array_size:
                py_script_name_call = self.job_date_id + ".wrfhydropy.py"
            else:
                py_script_name_call = py_script_name

            py_run_cmd = "python " + py_script_name_call + \
                         " --sched_job_id $sched_job_id --job_date_id $job_date_id"

            # This needs to happen before composing the scripts.
            self.scheduler.not_submitted = False

            # The python script
            selfstr = self._compose_scheduled_python_script(py_run_cmd, model_exe_cmd)
            with open(py_script_name, "w") as myfile:
                myfile.write(selfstr)

            # The bash submission script which calls the python script.
            self.exe_cmd = py_run_cmd

    def schedule(self):
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