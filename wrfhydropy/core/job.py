import datetime
import f90nml
import math
import os
import pathlib
import shlex
import socket
import subprocess
import warnings

from .job_tools import \
    touch, submit_scheduler, PBSError, \
    get_sched_name, get_machine, get_user, seconds, \
    core_dir, default_job_spec, \
    compose_scheduled_python_script, \
    compose_scheduled_bash_script,\
    check_file_exist_colon, \
    check_job_input_files

from .job_tools import release as jt_release


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


class Job(object): 
    def __init__(
            self,
            nproc: int,
            exe_cmd: str=None,
            modules: str=None,
            scheduler: Scheduler = None,
            model_start_time: str=None,
            model_end_time: str=None,
            model_restart: bool=True,
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

        self.model_start_time = model_start_time
        """str?: The model time at the start of the execution."""
        self.model_end_time = model_end_time
        """str?: The model time at the end of the execution."""
        self.model_restart = model_restart
        """bool: Look for restart files at modelstart_time?"""

        # These are only outputs/atts of the object.
        self.namelist_hrldas = None
        """dict: the HRLDAS namelist used for this job."""
        self.hydro_namelist = None
        """dict: the hydro namelist used for this job."""

        self.namelist_hrldas_file = None
        """dict: the file containing the HRLDAS namelist used for this job."""
        self.hydro_namelist_file = None
        """dict: the file containing the hydro namelist used for this job."""


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

        self.exit_status = None
        """int: The exit value of the model execution."""

        """pathlib.PosixPath: The tracejob/performance/profiling file."""

        """pathlib.PosixPath: The standard out file."""

        """pathlib.PosixPath: The standard error file."""

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
        default_job = default_job_spec(machine=get_machine())
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
        hold: bool=False
    ) -> object:
        """Scheulde a run of the wrf_hydro simulation
        Args:
            self: A Self object
        """

        # Deal with the shared queue here, since it affects both scripts.
        if get_machine() == 'cheyenne' and \
           self.scheduler.nnodes-1 == 0 and \
           self.scheduler.nproc_last_node <= 18:
            self.scheduler.queue = 'share'
            warnings.warn("Less than 18 procesors requested, using the 'share' queue.")
            find='mpiexec_mpt'
            repl='mpirun {hostname} -np ' + str(self.nproc)
            self.exe_cmd = self.exe_cmd.replace(find, repl)

        # Write python to be executed by the bash script given to the scheduler.
        # Execute the model from python script and the python script from the bash script:
        # swap their execution commands.

        model_exe_cmd = self.exe_cmd
        py_script_name = str(run_dir / (self.job_date_id + ".wrfhydropy.py"))
        py_run_cmd = "python " + py_script_name + \
                     " --sched_job_id $sched_job_id --job_date_id $job_date_id"

        # This needs to happen before composing the scripts.
        self.scheduler.not_submitted = False

        # The python script
        selfstr = compose_scheduled_python_script(py_run_cmd, model_exe_cmd)
        with open(py_script_name, "w") as myfile:
            myfile.write(selfstr)

        # The bash submission script which calls the python script.
        self.exe_cmd = py_run_cmd
        filename = run_dir / (self.job_date_id + '.' + self.scheduler.sched_name + '.job')
        jobstr = compose_scheduled_bash_script(run_dir=run_dir, job=self)
        with open(filename, "w") as myfile:
            myfile.write(jobstr)

        try:

            self.scheduler.sched_job_id = submit_scheduler(substr=bytearray(jobstr, 'utf-8'),
                                                           sched_name=self.scheduler.sched_name,
                                                           hold=hold)

        except PBSError as e:
            self.scheduler.not_submitted = True
            raise e

        # TODO(JLM): should make this a helper method
        touch(str(run_dir) + '/.job_not_complete')


    def release(self):
        return(jt_release(self.scheduler))


    def run(self, run_dir):

        # TODO(JLM): does the job['mode'] need checked at this point?

        # Create the namelists for the job and link to the generic namelists names.
        print('\nRunning job ' + self.job_date_id + ': ')
        print('    Wall start time: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('    Model start time: ' + self.model_start_time.strftime('%Y-%m-%d %H:%M'))
        print('    Model end time: ' + self.model_end_time.strftime('%Y-%m-%d %H:%M'))

        # Check for restart files both as specified and in the run dir..
        # Alias the mutables for some brevity
        hydro_nlst = self.hydro_namelist['hydro_nlist']
        hydro_nlst['restart_file'] = check_file_exist_colon(run_dir, hydro_nlst['restart_file'])
        nudging_nlst = self.hydro_namelist['nudging_nlist']
        if nudging_nlst:
            nudging_nlst['nudginglastobsfile'] = \
                check_file_exist_colon(run_dir, nudging_nlst['nudginglastobsfile'])

        check_job_input_files(self, run_dir)

        self.write_namelists(run_dir)

        if self.scheduler:

            # This is for when the submitted script calls the run method.
            # Note that this exe_cmd is for a python script which executes and optionally
            # waits for the model (it's NOT direct execution of the model).
            # TODO(JLM): Not sure why hostname: is in the following.
            #            This may be deprecated 5/18/2018
            #exe_cmd = self.exe_cmd.format(**{'hostname': socket.gethostname()}) + " 2> {0} 1> {1}"
            exe_cmd = self.exe_cmd.format(**{'nproc': self.nproc})
            exe_cmd += " 2> {0} 1> {1}"
            exe_cmd = exe_cmd.format(self.stderr_exe(run_dir), self.stdout_exe(run_dir))
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


    def write_namelists(self, run_dir):

        # TODO(JLM): make the setup namelists @properties without setter (protect them)
        # write hydro.namelist for the job
        self.hydro_namelist_file =  run_dir.joinpath(self.job_date_id + '.hydro.namelist')
        f90nml.write(self.hydro_namelist, self.hydro_namelist_file)
        nlst_file = run_dir.joinpath('hydro.namelist')
        if nlst_file.exists():
            nlst_file.unlink()
        nlst_file.symlink_to(self.hydro_namelist_file)

        # write namelist.hrldas
        self.namelist_hrldas_file = run_dir.joinpath(self.job_date_id + '.namelist.hrldas')
        f90nml.write(self.namelist_hrldas, self.namelist_hrldas_file)
        nlst_file = run_dir.joinpath('namelist.hrldas')
        if nlst_file.exists():
            nlst_file.unlink()
        nlst_file.symlink_to(self.namelist_hrldas_file)


    #######################################################
    # These can probably be made properties that may set on job or job.scheduler.
    def stdout_exe(self, run_dir):
        if self.scheduler:
            return self.eval_std_file_vars(self.scheduler._stdout_exe, run_dir)
        else:
            None
    def stderr_exe(self, run_dir):
        if self.scheduler:
            return self.eval_std_file_vars(self.scheduler._stderr_exe, run_dir)
        else:
            None
    def tracejob_file(self, run_dir):
        if self.scheduler:
            return self.eval_std_file_vars(self.scheduler._tracejob_file, run_dir)
        else:
            None
    def stdout_pbs(self, run_dir):
        if self.scheduler:
            return self.eval_std_file_vars(self.scheduler._stdout_pbs, run_dir)
        else:
            None
    def stderr_pbs(self, run_dir):
        if self.scheduler:
            return self.eval_std_file_vars(self.scheduler._stderr_pbs, run_dir)
        else:
            None
    def stdout_pbs_tmp(self, run_dir):
        if self.scheduler:
            return self.eval_std_file_vars(self.scheduler._stdout_pbs_tmp, run_dir)
        else:
            None
    def stderr_pbs_tmp(self, run_dir):
        if self.scheduler:
            return self.eval_std_file_vars(self.scheduler._stderr_pbs_tmp, run_dir)
        else:
            None

    def eval_std_file_vars(self, the_str, run_dir):
        if self.scheduler.not_submitted:
            return(the_str)
        dict = {'run_dir': run_dir,
                'job_date_id': self.job_date_id,
                'sched_job_id': self.scheduler.sched_job_id}
        if dict['sched_job_id'] is None: dict['sched_job_id'] = '${sched_job_id}'
        return(the_str.format(**dict))


    def apply_model_start_end_job_namelists(
        self
    ):

        # Refs
        noah_nlst = self.namelist_hrldas['noahlsm_offline']
        hydro_nlst = self.hydro_namelist['hydro_nlist']

        # Duration
        noah_nlst['kday'] = None
        noah_nlst['khour'] = None
        duration = self.model_end_time - self.model_start_time
        if duration.seconds == 0:
            noah_nlst['kday'] = int(duration.days)
        else:
            noah_nlst['khour'] = int(duration.days*60 + duration.seconds/3600)

        # Start
        noah_nlst['start_year'] = int(self.model_start_time.year)
        noah_nlst['start_month'] = int(self.model_start_time.month)
        noah_nlst['start_day'] = int(self.model_start_time.day)
        noah_nlst['start_hour'] = int(self.model_start_time.hour)
        noah_nlst['start_min'] = int(self.model_start_time.minute)

        # Restart
        if self.model_restart:
            restart_time = self.model_start_time
        else:
            # Though it will be commented, make it obvious.
            restart_time = datetime.datetime(9999, 9, 9, 9, 9)

        lsm_restart_dirname = '.' #os.path.dirname(noah_nlst['restart_filename_requested'])
        hydro_restart_dirname = '.' #os.path.dirname(hydro_nlst['restart_file'])

        #2011082600 - no minutes
        lsm_restart_basename = 'RESTART.' + \
                               self.model_start_time.strftime('%Y%m%d%H') + '_DOMAIN1'
        #2011-08-26_00_00 - minutes
        hydro_restart_basename = 'HYDRO_RST.' + \
                                 self.model_start_time.strftime('%Y-%m-%d_%H:%M') + '_DOMAIN1'

        lsm_restart_file = lsm_restart_dirname + '/' + lsm_restart_basename
        hydro_restart_file = hydro_restart_dirname + '/' + hydro_restart_basename

        if not self.model_restart:
            lsm_restart_file = '!!! ' + lsm_restart_file
            hydro_restart_file = '!!! ' + hydro_restart_file

        noah_nlst['restart_filename_requested'] = lsm_restart_file
        hydro_nlst['restart_file'] = hydro_restart_file

