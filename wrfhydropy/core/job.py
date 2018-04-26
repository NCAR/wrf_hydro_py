import datetime
import math
import os
import pathlib
import shlex
import subprocess
import warnings
from .job_tools import touch, submit_scheduler, PBSError, \
    get_sched_name, get_machine, get_user, seconds, core_dir, default_job_spec
#from .wrfhydroclasses import WrfHydroRun

class Scheduler(object):
    """A PBS/torque or slurm scheduler Job object.

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
        nproc: int=None,
        nnodes: int=None,
        ppn: int=None,
        email_when: str="a",
        email_who: str="${USER}@ucar.edu",
        queue: str='regular',
        walltime: str="12:00",
        wait_for_complete: bool=True,
        monitor_freq_s: int=None,
        afterok: str=None,
        array_size: int=None,
        pmem: str=None,
        grab_env: str=False,
        exetime: str=None,
        job_date_id: str=None
    ):

        # Declare attributes.
        # Required
        self.job_name   = job_name
        self.account    = account

        # Defaults in arglist
        self.email_when = email_when
        self.queue      = queue
        self.afterok = afterok
        self.array_size = array_size
        self.grab_env   = grab_env
        # TODO JLM: Should probably stash the grabbed argument in this case.
        # TODO JLM: is there a better variable name than grab_env?

        # Automagically set from environment
        self.sched_name = get_sched_name()
        # TODO(JLM): remove this testing comment/hack below when not testing it.
        #self.sched_version = int(re.split("[\+\ \.]", get_version())[2])

        # Extra Coercion 
        self.email_who  = os.path.expandvars(email_who)
        self.walltime   = ':'.join((walltime+':00').split(':')[0:3])

        # Construction
        self._nproc      = nproc
        self._nnodes     = nnodes
        self._ppn        = ppn

        self._job_date_id = job_date_id
        
        self.wait_for_complete = wait_for_complete
        self.monitor_freq_s = monitor_freq_s
        
        # Set attributes.

        # Try to get a default scheduler?
        
        # Check for required inputs
        # TODO(JLM): Deal with setting ppn from machine_spec_file.
        self.solve_nodes_cores()

        # Extra Coercion 
        self.email_who  = os.path.expandvars(email_who)
        self.walltime   = ':'.join((walltime+':00').split(':')[0:3])

        self.nproc_last_node = (self.nproc - (self.nnodes * self.ppn)) % self.ppn
        if self.nproc_last_node > 0:
            if self.nproc_last_node >= self.ppn:
                raise ValueError('nproc - (nnodes * ppn) = {0} >= ppn'.format(self.nproc_last_node))

        # Currently unsupported.
        self.pmem = pmem
        self.exetime = exetime

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


    def solve_nodes_cores(self):
        if None not in [self._nproc, self._nnodes, self._ppn]:
            warnings.warn("Not currently checking consistency of nproc, nnodes, ppn.")
            return

        if not self._nproc  and self._nnodes and self._ppn:
            self._nproc  = self._nnodes * self._ppn
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

    # Getting a really bad code smell right here... 
    def stdout_exe(self, *args):
        return self.eval_std_file_vars(self._stdout_exe, *args)
    def stderr_exe(self, *args):
        return self.eval_std_file_vars(self._stderr_exe, *args)
    def tracejob_file(self, *args):
        return self.eval_std_file_vars(self._tracejob_file, *args)
    def stdout_pbs(self, *args):
        return self.eval_std_file_vars(self._stdout_pbs, *args)
    def stderr_pbs(self, *args):
        return self.eval_std_file_vars(self._stderr_pbs, *args)
    def stdout_pbs_tmp(self, *args):
        return self.eval_std_file_vars(self._stdout_pbs_tmp, *args)
    def stderr_pbs_tmp(self, *args):
        return self.eval_std_file_vars(self._stderr_pbs_tmp, *args)

    def eval_std_file_vars(self, the_str, run_dir, job_date_id):
        if self.not_submitted:
            return(the_str)
        dict = {'run_dir': run_dir,
                'job_date_id': job_date_id,
                'sched_job_id': self.sched_job_id}
        if dict['sched_job_id'] is None: dict['sched_job_id'] = '${sched_job_id}'
        return(the_str.format(**dict))


    def string(self, run_dir, job_date_id, modules, exe_cmd):
        """ Write Job as a string suitable for self.sched_name """

        # Warn if any submit-time values are undefined.
        if self.not_submitted:
                warnings.warn('Submit-time values are not established, dummy values in {key}.')

        if self.sched_name.lower() == "slurm":
            ###Write this Job as a string suitable for slurm
            ### NOT USED:
            ###    exetime
            ###    priority
            ###    auto
            jobstr = "#!/bin/bash\n"

            ## FROM DART for job arrays.
            #JOBNAME=$SLURM_JOB_NAME
            #JOBID=$SLURM_JOBID
            #ARRAY_INDEX=$SLURM_ARRAY_TASK_ID
            #NODELIST=$SLURM_NODELIST
            #LAUNCHCMD="mpirun -np $SLURM_NTASKS -bind-to core"
            
            jobstr += "#SBATCH -J {0}\n".format(self.job_name)
            if self.account is not None:
                jobstr += "#SBATCH -A {0}\n".format(self.account)
            jobstr += "#SBATCH -t {0}\n".format(self.walltime)
            jobstr += "#SBATCH -n {0}\n".format(self.nnodes*self.ppn)
            if self.pmem is not None:
                jobstr += "#SBATCH --mem-per-cpu={0}\n".format(self.pmem)
            if self.qos is not None:
                jobstr += "#SBATCH --qos={0}\n".format(self.qos)
            if self.email != None and self.message != None:
                jobstr += "#SBATCH --mail-user={0}\n".format(self.email)
                if 'b' in self.message:
                    jobstr += "#SBATCH --mail-type=BEGIN\n"
                if 'e' in self.message:
                    jobstr += "#SBATCH --mail-type=END\n"
                if 'a' in self.message:
                    jobstr += "#SBATCH --mail-type=FAIL\n"
            # SLURM does assignment to no. of nodes automatically
            # jobstr += "#SBATCH -N {0}\n".format(self.nodes)
            if self.queue is not None:
                jobstr += "#SBATCH -p {0}\n".format(self.queue)
            jobstr += "{0}\n".format(exe_cmd)

            return jobstr

        else:

            ###Write this Job as a string suitable for PBS###
            jobstr = ""            
            jobstr += "#!/bin/sh\n"
            jobstr += "#PBS -N {0}\n".format(self.job_name)
            jobstr += "#PBS -A {0}\n".format(self.account)
            jobstr += "#PBS -q {0}\n".format(self.queue)
            jobstr += "#PBS -M {0}\n".format(self.email_who)
            jobstr += "#PBS -m {0}\n".format(self.email_when)
            jobstr += "\n"

            jobstr += "#PBS -l walltime={0}\n".format(self.walltime)
            jobstr += "\n"

            if self.nproc_last_node == 0:
                prcstr = "select={0}:ncpus={1}:mpiprocs={1}\n"
                prcstr = prcstr.format(self.nnodes, self.ppn)
            else:
                prcstr = "select={0}:ncpus={1}:mpiprocs={1}+1:ncpus={2}:mpiprocs={2}\n"
                prcstr = prcstr.format(self.nnodes-1, self.ppn, self.nproc_last_node)

            jobstr += "#PBS -l " + prcstr
            jobstr += "\n"

            jobstr += "# Not using PBS standard error and out files to capture model output\n"
            jobstr += "# but these hidden files might catch output and errors from the scheduler.\n"
            jobstr += "#PBS -o {0}\n".format(self.stdout_pbs_tmp(run_dir, job_date_id))
            jobstr += "#PBS -e {0}\n".format(self.stderr_pbs_tmp(run_dir, job_date_id))
            jobstr += "\n"

            if self.afterok:    jobstr += "#PBS -W depend=afterok:{0}\n".format(self.afterok)
            if self.array_size: jobstr += "#PBS -J 1-{0}\n".format(self.array_size)
            if self.exetime:    jobstr += "#PBS -a {0}\n".format(self.exetime)
            if self.pmem:       jobstr += "#PBS -l pmem={0}\n".format(self.pmem)
            if self.grab_env:   jobstr += "#PBS -V\n"
            if self.array_size or self.exetime or self.pmem or self.grab_env: jobstr += "\n"

            # End PBS Header

            if modules:
                jobstr += 'module purge\n'
                jobstr += 'module load {0}\n'.format(modules)
                jobstr += "\n"
            
            jobstr += "echo PBS_JOBID: $PBS_JOBID\n"
            jobstr += "sched_job_id=`echo ${PBS_JOBID} | cut -d'.' -f1`\n"
            jobstr += "echo sched_job_id: $sched_job_id\n"
            jobstr += "job_date_id={0}\n".format(job_date_id)
            jobstr += "echo job_date_id: $job_date_id\n"

            jobstr += "\n"

            jobstr += "cd {0}\n".format(run_dir)
            jobstr += "echo \"pwd:\" `pwd`\n"
            jobstr += "\n"

            jobstr += "# DART job variables for future reference\n"
            jobstr += "# JOBNAME=$PBS_JOBNAME\n"
            jobstr += "# JOBID=\"$PBS_JOBID\"\n"
            jobstr += "# ARRAY_INDEX=$PBS_ARRAY_INDEX\n"
            jobstr += "# NODELIST=`cat \"${PBS_NODEFILE}\"`\n"
            jobstr += "# LAUNCHCMD=\"mpiexec_mpt\"\n"
            jobstr += "# \n"

            jobstr += "# CISL suggests users set TMPDIR when running batch jobs on Cheyenne.\n"
            jobstr += "export TMPDIR=/glade/scratch/$USER/temp\n"
            jobstr += "mkdir -p $TMPDIR\n"
            jobstr += "\n"

            exestr  = "{0} ".format(exe_cmd)
            exestr += "2> {0} 1> {1}".format(self.stderr_exe(run_dir, job_date_id),
                                             self.stdout_exe(run_dir, job_date_id))
            jobstr += "echo \"" + exestr + "\"\n"
            jobstr += exestr + "\n"
            jobstr += "\n"
            
            jobstr += "mpi_return=$?\n"
            jobstr += "echo \"mpi_return: $mpi_return\"\n"
            jobstr += "\n"
            
            jobstr += "# Touch these files just to get the job_date_id in their file names.\n"
            jobstr += "# Can identify the files by sched_job_id and replace contents...\n"
            jobstr += "touch {0}\n".format(self.tracejob_file(run_dir, job_date_id))
            jobstr += "touch {0}\n".format(self.stdout_pbs(run_dir, job_date_id))
            jobstr += "touch {0}\n".format(self.stderr_pbs(run_dir, job_date_id))
            jobstr += "\n"

            jobstr += "# Simple, file-based method for checking if the job is done.\n"
            jobstr += "# qstat is a bad way of doing this, apparently.\n"
            jobstr += "rm .job_not_complete\n"
            jobstr += "\n"
            
            ## JLM: the tracejob execution gets called by the waiting process.
            jobstr += "exit $mpi_return\n"
            return jobstr


    def script(
        self,
        run_dir,
        job_date_id,
        modules,
        exe_cmd,
        filename: str=None
    ):
        """Write this Job as a bash script

        Keyword arguments:
        filename -- name of the script (default "submit.sh")

        """
        if self.not_submitted is not None:
            warnings.warn('script() can only be used when self.not_submitted is None. ' + \
                          'Use print(self.string()) to preview the job submission.')
            return

        if not filename:
            filename = run_dir / (job_date_id + '.' + self.sched_name + '.job')
        with open(filename, "w") as myfile:
            myfile.write(self.string(run_dir, job_date_id, modules, exe_cmd))


    def submit(
        self,
        run_dir,
        job_date_id,
        modules,
        exe_cmd
    ):
        """Submit this Job using qsub

           add: Should this job be added to the JobDB database?
           dbpath: Specify a non-default JobDB database

           Raises PBSError if error submitting the job.

        """
        try:
            self.not_submitted = None
            touch(str(run_dir) + '/.job_not_complete')
            self.script(run_dir, job_date_id, modules, exe_cmd)
            the_string = self.string(run_dir, job_date_id, modules, exe_cmd)
            self.sched_job_id = submit_scheduler(substr=bytearray(the_string, 'utf-8'),
                                                 sched_name=self.sched_name)
        except PBSError as e:
            raise e

        
    @property
    def job_complete(self):
        if self.not_submitted:
            return(False)
        return( not os.path.isfile(run_dir + '/.job_not_complete') )


class Job(object): 
    def __init__(
            self,
            nproc: int=None,
            exe_cmd: str=None,
            modules: str=None,
            scheduler: Scheduler = None,
    ):

        self.exe_cmd = exe_cmd
        """str: The command to be executed. Python {}.format() evaluation available but
        limited. Taken from the machine_spec.yaml file if not specified."""
        self._nproc = nproc
        """int: Optional, the number of processors to use. If also supplied in the scheduler
        then there will be ab error."""
        self.machine = get_machine()
        """str: The name of the machine being used."""
        self.modules = modules
        """str: The modules to be loaded prior to execution. Taken from machine_spec.yaml 
        if not present."""
        self.scheduler = scheduler
        """Scheduler: Optional, scheduler object for the job."""

        # TODO(JLM): These are optional inputs and outputs: if missing try
        # to use existing namelists.
        self.model_start_time = None
        """str?: The model time at the start of the execution."""
        self.model_end_time = None
        """str?: The model time at the end of the execution."""
        self.model_restart = None
        """bool: Look for restart files at modelstart_time?"""

        # These are only outputs/atts of the object.
        self.namelist_hrldas = None
        """dict: the HRLDAS namelist used for this job."""
        self.hydro_namelist = None
        """dict: the hydro namelist used for this job."""

        self.job_status = "created"
        """str: The status of the job object: created/submitted/running/complete."""

        # #################################
        # Attributes solved from the environment at run time or later (not now).
        self.user = None
        """str: Determine who the user is."""

        # TODO(JLM): this is admittedly a bit dodgy because sensitive info
        # might be in the environment (github authtoken?)
        # Are there parts of the env we must have?
        # self.environment = None
        
        self.job_date_id = None
        """str: The job date identifier at 'submission' time."""
        self.job_start_time = None
        """str?: The time at the start of the execution."""
        self.job_end_time = None
        """str?: The time at the end of the execution."""
        self.job_submission_time = None
        """str?: The time the job object was created."""

        self.stdout_file = None
        """pathlib.PosixPath: The standard out file."""
        self.stderr_file = None
        """pathlib.PosixPath: The standard error file."""
        self.exit_status = None
        """int: The exit value of the model execution."""
        self.tracejob_file = None
        """pathlib.PosixPath: The tracejob/performance/profiling file."""
        self.diag_files = None
        """pathlib.PosixPath: The diag files for the job."""

        # #################################
        # Setting better defaults.

        # If there is no scheduler on the machine. Do not allow a scheduler object.
        if get_sched_name() is None:
            self.scheduler = None
        else:
            # Allow coercion from a dict to a scheduler object.
            if type(self.scheduler) is dict:
                self.scheduler = Scheduler(**self.scheduler)
            
        # ###################################
        # Deal with the potential conflict between the job ncores and the scheduler ncores.
        if self._nproc and self.scheduler:
            if self.scheduler.nproc:
                if self.scheduler.nproc != nproc:
                    error_msg  = "The number of cores passed to the job does not match the "
                    error_msg += "number of cores specified in the job's scheduler."
                    raise ValueError(error_msg)
            else:
                self.scheduler.nproc = self.nproc

        # TODO(JLM): Maybe this should be done first and overwritten?
        # TODO(JLM): For missing job/scheduler properties, attempt to get defaults.
        # These are in a file in the package dir. Where?
        # A method (used by  django) about specifying the root dir of the project.
        # https://stackoverflow.com/questions/25389095/python-get-path-of-root-project-structure
        #self.build_default_job(self)
        default_job = default_job_spec()
        for ii in default_job.keys():
            if ii in self.__dict__.keys():
                if self.__dict__[ii] is None and default_job[ii] is not None:
                    warnings.warn('Using docker default for missing Job argument ' + ii)
                    self.__dict__[ii] = default_job[ii]

    @property
    def nproc(self):
        if self.scheduler:
            self._nproc = self.scheduler.nproc
            return self.scheduler.nproc
        return self._nproc
    @nproc.setter
    def nproc(self, value):
        self._nproc = value
        if self.scheduler:
            self.scheduler.nproc = value
            return self.scheduler.nproc
        return self._nproc


    def schedule(
        self,
        run_dir,
    ) -> object:
        """Scheulde a run of the wrf_hydro simulation
        Args:
            job: A Job object
        """

        # TODO(JLM): to be moved to scheduler construction.
        if self.scheduler.monitor_freq_s is None:
            self.scheduler.monitor_freq_s = int(max(seconds('00:' + self.scheduler.walltime)/100,30))

        # Write python script WrfHydroSim.schedule_run.py to be executed by the
        # scheduler. Swap the scheduler script executable (exe_cmd), for this
        # python script: call python script in the scheduler job and execute the
        # model from python (as an object method).

        model_exe_cmd = self.exe_cmd
        py_script_name = str(run_dir / (self.job_date_id + ".wrfhydropy.py"))
        py_run_cmd = "python " + py_script_name + \
                     " --sched_job_id $sched_job_id --job_date_id $job_date_id"

        # TODO(JLM): abstract this to a utility function with
        #            args: py_run_cmd, model_exe_cmd
        # Construct the script.
        jobstr  = "#!/usr/bin/env python\n"
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
        jobstr += "run_object.job_active.job_start_time = str(datetime.datetime.now())\n"
        jobstr += "\n"

        jobstr += "print(\"Collecting model output.\")\n"
        jobstr += "run_object.collect_output()\n"
        jobstr += "print(\"Job completed.\")\n"
        jobstr += "\n"

        jobstr += "run_object.job_active._sched_job_complete = True\n"
        jobstr += "run_object.jobs_completed.append(run_object.job_active)\n"
        jobstr += "run_object.job_active = None\n"
        jobstr += "run_object.pickle()\n"
        jobstr += "\n"

        with open(py_script_name, "w") as myfile:
            myfile.write(jobstr)

        # Now submit the above script to the scheduler.
        # TODO(JLM): 1) move this job to self.job_active? 2) pickle self ?
        self.exe_cmd = py_run_cmd
        # TODO(JLM):
        self.scheduler.not_submitted = False
        self.scheduler.submit(run_dir, self.job_date_id, self.modules, self.exe_cmd)
        # Waiting (if any) should happen at the calling level


    def run(self, run_dir):

        # TODO(JLM): does the job['mode'] need checked at this point?

        if self.scheduler:

            # This is for when the submitted script calls the run method.
            # Note that this exe_cmd is for a python script which executes and optionally
            # waits for the model (it's NOT direct execution of the model).
            exe_cmd = self.exe_cmd + " 2> {0} 1> {1}"
            exe_cmd = exe_cmd.format(self.scheduler.stderr_exe(run_dir, self.job_date_id),
                                     self.scheduler.stdout_exe(run_dir, self.job_date_id))
            exe_cmd = shlex.split(exe_cmd)
            subprocess.run(exe_cmd, cwd=run_dir)

        else:

            # These dont have the sched_job_id that the scheduled job output files have.
            self.stderr_file = run_dir / ("{0}.stderr".format(self.job_date_id))
            self.stdout_file = run_dir / ("{0}.stdout".format(self.job_date_id))

            # source the modules before execution.
            exe_cmd = '/bin/bash -c "'
            if self.modules:
                exe_cmd += "module purge && module load " + self.modules + " && "
            exe_cmd += self.exe_cmd.format(**{'nproc': self.nproc})
            exe_cmd += " 2> {0} 1> {1}".format(self.stderr_file, self.stdout_file)
            exe_cmd += '"'
            self.exe_cmd = exe_cmd

            # TODO(JLM): Stash the namelist files in the job at this point? No,
            # that should happen when the dates of the job(s) are established.

            self.job_status='running'
            self.job_start_time = str(datetime.datetime.now())
            self.run_log = subprocess.run(
                shlex.split(exe_cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=run_dir
            )
            self.job_end_time = str(datetime.datetime.now())
            self.job_status='completed'

        # TODO(JLM): The following be made a method which checks the run.
        #            The following should not be run if the scheduler is not waiting.
        #            Put this in collect_run?
        try:
            
            # Get diag files
            # TODO(JLM): diag_files should be scrapped orrenamed to no conflict between jobs.
            run_dir_posix = pathlib.PosixPath(run_dir)
            self.diag_files = list(run_dir_posix.glob('diag_hydro.*'))
            self.stdout_file = list(run_dir_posix.glob(self.job_date_id+'.*stdout'))[0]
            self.stderr_file = list(run_dir_posix.glob(self.job_date_id+'.*stderr'))[0]
            self.tracejob_file = list(run_dir_posix.glob(self.job_date_id+'.*tracejob'))
            if len(self.tracejob_file):
                self.tracejob_file = self.tracejob_file[0]
            else:
                self.tracejob_file = None

            self.exit_status = 1
            self.job_status='completed failure'
            # String match diag files for successfull run
            with open(run_dir.joinpath('diag_hydro.00000')) as f:
                diag_file = f.read()
                if 'The model finished successfully.......' in diag_file:
                    self.exit_status = 0
                    self.job_status='completed success'
        except Exception as e:
            warnings.warn('Could not parse diag files')
            print(e)    
