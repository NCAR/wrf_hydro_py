# Note: All other imports for individual schedulers should be done in the respective scheduler
# class functions so that imports can be isolated to relevant schedulers

from abc import ABC, abstractmethod


class Scheduler(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def schedule(self, jobs):
        pass


class PBSCheyenne(Scheduler):

    """A Scheduler object compatible with PBS on the NCAR Cheyenne system."""
    def __init__(
            self,
            account: str,
            nproc: int,
            nnodes: int,
            mem: int = None,
            ppn: int = None,
            queue: str = 'regular',
            walltime: str = "12:00:00",
            email_who: str = None,
            email_when: str = 'abe',
            custom: dict = {}
    ):
        """Initialize an PBSCheyenne object.
        Args:
            account: The account string
            nproc: Number of processors to request
            nnodes: Number of nodes to request
            ppn: Number of processors per node
            mem: Memory in GB usage/request on node (109 for fat nodes).
            email_who: Email address for PBS notifications
            email_when: PBS email frequency options. Options include 'a' for on abort,
            'b' for before each job, and 'e' for after each job.
            queue: The queue to use, options are 'regular', 'premium', and 'shared'
            walltime: The wall clock time in HH:MM:SS format, max time is 12:00:00
        """

        # Declare attributes.
        # property construction
        self._sim_dir = None
        self._nproc = nproc
        self._nnodes = nnodes
        self._ppn = ppn

        # Scheduler options dict
        # TODO: Make this more elegant than hard coding for maintenance sake
        self.scheduler_opts = {
            'account': account,
            'email_when': email_when,
            'email_who': email_who,
            'queue': queue,
            'walltime': walltime,
            'mem': mem,
            'custom': custom
        }

    def schedule(self, jobs: list):
        """Schedule one or more jobs using the scheduler scheduler
            Args:
                jobs: list of jobs to schedule
        """
        import subprocess
        import shlex
        import pathlib
        import os

        current_dir = pathlib.Path(os.curdir)

        # TODO: Find a way to protect the job order so that once someone executes schedule...
        # they can't change the order, may not be an issue except for if scheduling fails
        # somewhere

        self._write_job_pbs(jobs=jobs)

        # Make lists to store pbs scripts and pbs job ids to get previous dependency
        pbs_jids = []
        pbs_scripts = []

        qsub_str = "/bin/bash -c '"
        for job_num, option in enumerate(jobs):

            # This gets the pbs script name and pbs jid for submission
            # the obs jid is stored in a list so that the previous jid can be retrieved for
            # dependency
            job_id = jobs[job_num].job_id
            pbs_scripts.append(str(jobs[job_num].job_dir) + "/job_" + job_id + ".pbs")
            pbs_jids.append("job_" + job_id)

            # If first job, schedule using hold
            if job_num == 0:
                qsub_str += pbs_jids[job_num] + "=`qsub -h " + pbs_scripts[job_num] + "`;"
            # Else schedule using job dependency on previous pbs jid
            else:
                qsub_str += pbs_jids[job_num] + "=`qsub -W depend=afterok:${" + pbs_jids[
                    job_num-1] + "} " + pbs_scripts[job_num] + "`;"

        qsub_str += "qrls ${" + pbs_jids[0] + "};"
        qsub_str += "'"

        # Just for debugging purposes
        print("qsub_str: ", qsub_str)
        # This stacks up dependent jobs in PBS in the same order as the job list
        subprocess.run(shlex.split(qsub_str),
                       cwd=str(current_dir))

    def _write_job_pbs(self, jobs):
        """Private method to write bash PBS scripts for submitting each job """
        import copy
        import sys

        # Get the current pytohn executable to handle virtual environments in the scheduler
        python_path = sys.executable

        for job in jobs:
            # Copy the job because the exe cmd is edited below
            job = copy.deepcopy(job)
            custom = self.scheduler_opts['custom']
            
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

            if '-l' not in custom or (
                    '-l' in custom and 'walltime' not in custom['-l']):
                jobstr += "#PBS -l walltime={0}\n".format(self.scheduler_opts['walltime'])

            if '-l' not in custom or (
                    '-l' in custom and 'select' not in custom['-l']):
                prcstr = "select={0}:ncpus={1}:mpiprocs={1}"
                prcstr = prcstr.format(self.nnodes, self.ppn)
                if self.scheduler_opts['mem'] is not None:
                    prcstr = prcstr + ":mem={0}GB"
                    prcstr = prcstr.format(self.scheduler_opts['mem'])
                    prcstr = prcstr
                jobstr += "#PBS -l " + prcstr + "\n"
                jobstr += "\n"

            if '-l' in custom:
                jobstr += "#PBS -l " + custom['-l'] + "\n"
                jobstr += "\n"

            jobstr += "# Not using PBS standard error and out files to capture model output\n"
            jobstr += "# but these files might catch output and errors from the scheduler.\n"
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

            jobstr += "{0} run_job.py --job_id {1}\n".format(python_path, job.job_id)
            jobstr += "exit $?\n"

            pbs_file = job.job_dir.joinpath("job_" + job.job_id + ".pbs")
            with pbs_file.open(mode='w') as f:
                f.write(jobstr)

            # Write the python run script for the job
            if '{nproc}' in job._exe_cmd:
                # If the job exe uses "nproc" then apply the schedulers value.
                job._exe_cmd = job._exe_cmd.format(**{'nproc': self.nproc})
            else:
                # regression tests use "{0}" format, try that here too
                job._exe_cmd = job._exe_cmd.format(self.nproc)

            job._write_run_script()

    def _solve_nodes_cores(self):
        """Private method to solve the number of nodes and cores if not all three specified"""

        import math

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
