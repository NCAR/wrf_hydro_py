import re
import os
import sys
import math
import datetime
import warnings
from .scheduler_tools import touch, submit_scheduler, PBSError, get_sched_name, get_version

class Job(object): 
    def __init__(
            self,
            run_dir: str,
            exe_cmd: str,
            nproc: int,
            mode: str,
            scheduler: Scheduler = None,
    ):

        if scheduler:
            self.scheduler = scheduler
            self.run_dir = self.scheduler.run_dir
            self.exe_cmd = self.scheduler.exe_cmd
            self.nproc = self.scheduler.nproc
            self.mode = mode

        else :
            self.run_dir = run_dir
            self.exe_cmd = exe_cmd
            self.nproc = nproc
            self.mode = mode
            
            

    
class Scheduler(object):
    """A qsub Job object.

    Initialize either with all the parameters, or with 'qsubstr' a PBS submit script as a string.
    If 'qsubstr' is given, all other arguments are ignored and set using Job.read().


    Variables 
        On cheyenne, PBS attributes are described in `man qsub` and `man pbs_resources`.
        See also: https://www2.cisl.ucar.edu/resources/computational-systems/cheyenne/running-jobs/submitting-jobs-pbs
        A dictionary can be constructed in advance from specification files by the function
        get_sched_args_from_specs().

    Var Name    Cheyenne       default            example         Notes
    -------------------------------------------------------------------------------------------
    name        -N                                "my_job"            
    account     -A                                "NRAL0017"
    
    email_when  -m             "a"                 "abe"
    email_who   -M             "${USER}@ucar.edu"  "johndoe@ucar.edu"

    queue       -q             "regular"           "regular"
    walltime    -l walltime=   "12:00"             "10:00:00" or "10:00" (seconds coerced)

    array_size  -J              None               16             integer
    grab_env    -V              None               True           logical

    Sepcify: np + 
    np                                             500             Number of procs
    nodes                                          4               Number of nodes
    ppn                        Default:            24              Number of procs/node
                               machine_spec_file.cores_per_node

    exe_cmd                                        "echo \"hello\" > test.txt"

    modules

    -*-*-*-*-*-*-*-  FOLLOWING NOT TESTED ON CHEYENNE  -*-*-*-*-*-*-*-

    pmem        -l pmem=                          "2GB"           Default is no restriction.
    exetime     -a                                "1100"          Not tested
    """

    def __init__(
        self,
        job_name: str,
        account: str,
        exe_cmd: str,
        run_dir: str,
        nproc: int=None,
        nnodes: int=None,
        ppn: int=None,
        email_when: str="a",
        email_who: str="${USER}@ucar.edu",
        queue: str='regular',
        walltime: str="12:00",
        wait_for_complete= bool=True,
        monitor_freq_s= int: None,
        array_size: int=None,
        modules: str=None,
        pmem: str=None,
        grab_env: str=False,
        exetime: str=None
    ):

        # Check for required inputs
        # TODO: Deal with setting ppn from machine_spec_file.
        if not nproc  and nnodes and ppn  : nproc  = nnodes * ppn
        if not nnodes and nproc  and ppn  : nnodes = math.ceil(nproc / ppn)
        if not ppn    and nnodes and nproc: ppn = math.ceil(nproc / nnodes)
        # TODO: Set nodes from nproc/n
        
        req_args = { 'job_name': job_name,
                     'account': account,
                     'nproc': nproc,
                     'nnodes': nnodes,
                     'ppn': ppn,
                     'exe_cmd': exe_cmd,
                     'run_dir': run_dir }
        
        def check_req_args(arg_name, arg):
            if not arg:
                raise ValueError(arg_name + " is a required argument.")
   
        [ check_req_args(n,a) for n, a in req_args.items() ]

        # Required
        self.job_name   = job_name
        self.account    = account
        self.exe_cmd    = exe_cmd

        # Defaults in arglist
        self.email_when = email_when
        self.queue      = queue
        self.array_size = array_size
        self.grab_env   = grab_env
        # TODO JLM: Should probably stash the grabbed argument in this case.
        # TODO JLM: is there a better variable name than grab_env?
        
        self.modules    = modules
        self.run_dir    = run_dir

        # Automagically set from environment
        self.sched_name = get_sched_name()
        self.sched_version = int(re.split("[\+\ \.]", get_version())[2])
        
        # Extra Coercion 
        self.email_who  = os.path.expandvars(email_who)
        self.walltime   = ':'.join((walltime+':00').split(':')[0:3])

        # Construction
        self.nproc      = int(nproc)
        self.nnodes     = int(nnodes)
        self.ppn        = int(ppn)

        nproc_last_node = (nproc - (nnodes * ppn)) % ppn
        self.nproc_last_node = nproc_last_node
        if nproc_last_node > 0:
            if nproc_last_node >= ppn:
                raise ValueError('nproc - (nnodes * ppn) = {0} >= ppn'.format(nproc_last_node))

        # Currently unsupported.
        self.pmem = pmem
        self.exetime = exetime

        # jobID and job_date_id are set on submission
        self.jobID = None
        self.job_date_id = None

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


    @property
    def stdout_exe(self):
        return(self.eval_std_vars(self._stdout_exe))
    @property
    def stderr_exe(self):
        return(self.eval_std_vars(self._stderr_exe))
    @property
    def tracejob_file(self):
        return(self.eval_std_vars(self._tracejob_file))
    @property
    def stdout_pbs(self):
        return(self.eval_std_vars(self._stdout_pbs))
    @property
    def stderr_pbs(self):
        return(self.eval_std_vars(self._stderr_pbs))
    @property
    def stdout_pbs_tmp(self):
        return(self.eval_std_vars(self._stdout_pbs_tmp))
    @property
    def stderr_pbs_tmp(self):
        return(self.eval_std_vars(self._stderr_pbs_tmp))
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
