import datetime
import math
import os
import socket
import warnings
from .job_tools import touch, submit_scheduler, PBSError, get_sched_name, get_version

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
        exetime: str=None
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

        # jobID and job_date_id are set on submission
        self.jobID = None
        self.job_date_id = None

        self.job_script = None
        
        # PBS has a silly stream buffer that 1) has a limit, 2) cant be seen until the job ends.
        # Separate and standardize the stdout/stderr of the exe_cmd and the scheduler.

        # The path to the model stdout&stderr
        self._stdout_exe = "{run_dir}/{job_date_id}.{jobID}.stdout"
        self._stderr_exe = "{run_dir}/{job_date_id}.{jobID}.stderr"

        # Tracejob file which holds performance information
        self._tracejob_file = "{run_dir}/{job_date_id}.{jobID}." + self.sched_name + ".tracejob"

        # Dot files for the pbs stdout&stderr files, both temp and final.
        # The initial path to the PBS stdout&stderr, during the job
        self._stdout_pbs_tmp = "{run_dir}/.{job_date_id}." + self.sched_name + ".stdout"
        self._stderr_pbs_tmp = "{run_dir}/.{job_date_id}." + self.sched_name + ".stderr"
        # The eventual path to the " + self.sched_name + " stdout&stderr, after the job
        self._stdout_pbs = "{run_dir}/.{job_date_id}.{jobID}." + self.sched_name + ".stdout"
        self._stderr_pbs = "{run_dir}/.{job_date_id}.{jobID}." + self.sched_name + ".stderr"

        # A three state variable. If "None" then script() can be called.
        # bool(None) is False so
        # None = submitted = True while not_submitted = False
        self.not_submitted = True

        # A status that depends on job being submitted and the .job_not_complete file
        # not existing being missing.
        self._job_complete = False


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
    @property
    def nnodes(self):
        self.solve_nodes_cores()
        return self._nnodes
    @property
    def ppn(self):
        self.solve_nodes_cores()
        return self._ppn
    
    @property
    def stdout_exe(self):
        return self.eval_std_vars(self._stdout_exe)
    @property
    def stderr_exe(self):
        return self.eval_std_vars(self._stderr_exe)
    @property
    def tracejob_file(self):
        return self.eval_std_vars(self._tracejob_file)
    @property
    def stdout_pbs(self):
        return self.eval_std_vars(self._stdout_pbs)
    @property
    def stderr_pbs(self):
        return self.eval_std_vars(self._stderr_pbs)
    @property
    def stdout_pbs_tmp(self):
        return self.eval_std_vars(self._stdout_pbs_tmp)
    @property
    def stderr_pbs_tmp(self):
        return self.eval_std_vars(self._stderr_pbs_tmp)
    # TODO JLM: turn the above into pathlib.PosixPath objects.

    
    def eval_std_vars(self, theStr):
        if self.not_submitted:
            return(theStr)
        dict = {'run_dir': self.run_dir,
                'job_date_id': self.job_date_id,
                'jobID': self.jobID}
        if dict['jobID'] is None: dict['jobID'] = '${jobID}'
        return(theStr.format(**dict))


    def string(self):
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
            jobstr += "{0}\n".format(self.exe_cmd)

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
            jobstr += "#PBS -o {0}\n".format(self.stdout_pbs_tmp)
            jobstr += "#PBS -e {0}\n".format(self.stderr_pbs_tmp)
            jobstr += "\n"

            if self.afterok:    jobstr += "#PBS -W depend=afterok:{0}\n".format(self.afterok)
            if self.array_size: jobstr += "#PBS -J 1-{0}\n".format(self.array_size)
            if self.exetime:    jobstr += "#PBS -a {0}\n".format(self.exetime)
            if self.pmem:       jobstr += "#PBS -l pmem={0}\n".format(self.pmem)
            if self.grab_env:   jobstr += "#PBS -V\n"
            if self.array_size or self.exetime or self.pmem or self.grab_env: jobstr += "\n"

            # End PBS Header

            if self.modules:
                jobstr += 'module purge\n'
                jobstr += 'module load {0}\n'.format(self.modules)
                jobstr += "\n"
            
            jobstr += "echo PBS_JOBID: $PBS_JOBID\n"
            jobstr += "jobID=`echo ${PBS_JOBID} | cut -d'.' -f1`\n"
            jobstr += "echo jobID: $jobID\n"
            jobstr += "job_date_id={0}\n".format(self.job_date_id)
            jobstr += "echo job_date_id: $job_date_id\n"

            jobstr += "\n"

            jobstr += "cd {0}\n".format(self.run_dir)
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

            exestr  = "{0} ".format(self.exe_cmd)
            exestr += "2> {0} 1> {1}".format(self.stderr_exe, self.stdout_exe)
            jobstr += "echo \"" + exestr + "\"\n"
            jobstr += exestr + "\n"
            jobstr += "\n"
            
            jobstr += "mpi_return=$?\n"
            jobstr += "echo \"mpi_return: $mpi_return\"\n"
            jobstr += "\n"
            
            jobstr += "# Touch these files just to get the job_date_id in their file names.\n"
            jobstr += "# Can identify the files by jobId and replace contents...\n"
            jobstr += "touch {0}\n".format(self.tracejob_file)
            jobstr += "touch {0}\n".format(self.stdout_pbs)
            jobstr += "touch {0}\n".format(self.stderr_pbs)
            jobstr += "\n"

            jobstr += "# Simple, file-based method for checking if the job is done.\n"
            jobstr += "# qstat is a bad way of doing this, apparently.\n"
            jobstr += "rm .job_not_complete\n"
            jobstr += "\n"
            
            ## JLM: the tracejob execution gets called by the waiting process.
            jobstr += "exit $mpi_return\n"
            return jobstr


    def script(self,
               filename: str=None):
        """Write this Job as a bash script

        Keyword arguments:
        filename -- name of the script (default "submit.sh")

        """
        if self.not_submitted is not None:
            warnings.warn('script() can only be used when self.not_submitted is None. ' + \
                          'Use print(self.string()) to preview the job submission.')
            return
        
        if not filename:
            filename = self.run_dir + "/" + self.job_date_id + '.' + self.sched_name + '.job'
        with open(filename, "w") as myfile:
            myfile.write(self.string())


    def submit(self):
        """Submit this Job using qsub

           add: Should this job be added to the JobDB database?
           dbpath: Specify a non-default JobDB database

           Raises PBSError if error submitting the job.

        """
        try:
            self.job_date_id = '{date:%Y-%m-%d-%H-%M-%S-%f}'.format(date=datetime.datetime.now())
            self.not_submitted = None
            touch(self.run_dir + '/.job_not_complete')
            self.script()
            self.not_submitted = False
            self.jobID = submit_scheduler(substr=bytearray(self.string(), 'utf-8'),
                                          sched_name=self.sched_name)
        except PBSError as e:
            raise e

        
    @property
    def job_complete(self):
        if self.not_submitted:
            return(False)
        return( not os.path.isfile(self.run_dir + '/.job_not_complete') )


class Job(object): 
    def __init__(
            self,
            exe_cmd: str=None,
            nproc: int=None,
            machine: str=socket.gethostname(),
            modules: str=None,
            scheduler: Scheduler = None,
    ):

        self._exe_cmd = exe_cmd
        """str: The command to be executed. Python {}.format() evaluation available but
        limited. Taken from the machine_spec.yaml file if not specified."""
        self._nproc = nproc
        """int: Optional, the number of processors to use. If also supplied in the scheduler
        then there will be ab error."""
        self.machine = machine
        """str: The name of the machine being used (from socket.gethostname())."""
        self.modules = modules
        """str: The modules to be loaded prior to execution. Taken from machine_spec.yaml 
        if not present."""
        self.scheduler = scheduler
        """Scheduler: Optional, scheduler object for the job."""
        
        # TODO(JLM): Are these both optional inputs and outputs?
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

        # Attributes solved from the environment at job time (not now).
        self.user = None
        """str: $USER."""
        # TODO(JLM): this is admittedly a bit dodgy because sensitive info
        # might be in the environment (github authtoken?)
        # Are there parts of the env we must have?
        self.environment = None

        # Attributes solved later (not now).
        """str: All the environment variables at execution time."""
        self.job_start_time = None
        """str?: The time at the start of the execution."""
        self.job_end_time = None
        """str?: The time at the end of the execution."""
        self.job_submission_time = None
        """str?: The time the job object was created."""

        # TODO(JLM): Do i want to just capture the file names or also the contents?
        self.stdout_file = None
        """pathlib.PosixPath: The standard out file."""
        self.stderr_file = None
        """pathlib.PosixPath: The standard error file."""
        self.exit_status = None
        """int: The exit value of the model execution."""
        # TODO(JLM): Is the above actually useful?
        self.tracejob_file = None

        # TODO(JLM): The diag files will get clobbered. Scrape to a dot directory
        # after successful or unsuccessful completion? The files can also be large... 
        self.diag_files = None
        """pathlib.PosixPath: The diag files for the job."""

        # Setting better defaults.

        # If there is no scheduler on the machine. Do not allow a scheduler object.
        if get_sched_name() is None:
            self.scheduler = None
        else:
            # Allow some coercion. 
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

        

    @property
    def run_dir(self):
        if self.scheduler:
            return self.scheduler._run_dir
        return self._run_dir

    @property
    def exe_cmd(self):
        if self.scheduler:
            return self.scheduler._exe_cmd
        return self._exe_cmd

    @property
    def nproc(self):
        if self.scheduler:
            return self.scheduler.nproc
        return self._nproc

#    def build_default_job():
