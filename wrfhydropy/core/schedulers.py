import math
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
            account: str,
            email_who: str = None,
            nproc: int = 36,
            nnodes: int = 2,
            ppn: int = None,
            email_when: str = 'abe',
            queue: str = 'regular',
            walltime: str = "12:00:00"):


        # Declare attributes.
        ## property construction
        self._sim_dir = None
        self._nproc = nproc
        self._nnodes = nnodes
        self._ppn = ppn

        # Attribute
        self.jobs = []

        ## Scheduler options dict
        ## TODO: Make this more elegant than hard coding for maintenance sake
        self.scheduler_opts = {'account':account,
                               'email_when':email_when,
                               'email_who':email_who,
                               'queue':queue,
                               'walltime':walltime}

    def add_job(self,job: Job):
        self.jobs.append(job)

    def schedule(self):
        self._write_job_pbs()


    def _write_job_pbs(self):
        """ Write bash PBS and python scripts for submitting each job """
        for job in self.jobs:

            # Write PBS script
            jobstr = ""
            jobstr += "#!/bin/sh\n"
            jobstr += "#PBS -N {0}\n".format(job.job_id)
            jobstr += "#PBS -A {0}\n".format(self.scheduler_opts['account'])
            jobstr += "#PBS -q {0}\n".format(self.scheduler_opts['queue'])

            if self.scheduler_opts['email_who'] is not None:
                jobstr += "#PBS -M {0}\n".format(self.scheduler_opts['email_who'])
                jobstr += "#PBS -m {0}\n".format(self.scheduler_opts['email_when'])
            jobstr += "\n"

            jobstr += "#PBS -l walltime={0}\n".format(self.scheduler_opts['walltime'])
            jobstr += "\n"

            prcstr = "select={0}:ncpus={1}:mpiprocs={1}\n"
            prcstr = prcstr.format(self.nnodes, self.ppn)


            jobstr += "#PBS -l " + prcstr
            jobstr += "\n"

            jobstr += "# Not using PBS standard error and out files to capture model output\n"
            jobstr += "# but these hidden files might catch output and errors from the scheduler.\n"
            jobstr += "#PBS -o {0}\n".format(job.job_dir)
            jobstr += "#PBS -e {0}\n".format(job.job_dir)
            jobstr += "\n"

            # End PBS Header

            # if job.modules:
            #    jobstr += 'module purge\n'
            #    jobstr += 'module load {0}\n'.format(job.modules)
            #    jobstr += "\n"

            jobstr += "# CISL suggests users set TMPDIR when running batch jobs on Cheyenne.\n"
            jobstr += "export TMPDIR=/glade/scratch/$USER/temp\n"
            jobstr += "mkdir -p $TMPDIR\n"
            jobstr += "\n"

            if self.scheduler_opts['queue'] == 'share':
                jobstr += "export MPI_USE_ARRAY=false\n"

            jobstr += 'python run_job.py --job_id {0}\n'.format(job.job_id)

            pbs_file = job.job_dir.joinpath(job.job_id + '.pbs')
            with pbs_file.open(mode='w') as f:
                f.write(jobstr)

            # Write the python run script for the job
            job._write_run_script()

    def _solve_nodes_cores(self):
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
        self._solve_nodes_cores()
        return self._nproc

    @nproc.setter
    def nproc(self, value):
        self._nproc = value

    @property
    def nnodes(self):
        self._solve_nodes_cores()
        return self._nnodes

    @nnodes.setter
    def nnodes(self, value):
        self._nnodes = value

    @property
    def ppn(self):
        self._solve_nodes_cores()
        return self._ppn

    @ppn.setter
    def ppn(self, value):
        self._ppn = value

