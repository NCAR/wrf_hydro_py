import datetime
import f90nml
import math
import os
import pathlib
import shlex
import socket
import subprocess
import sys
import warnings

from .job_tools import \
    touch, submit_scheduler, PBSError, \
    get_sched_name, get_machine, get_user, seconds, \
    core_dir, default_job_spec, \
    compose_scheduled_python_script, \
    compose_scheduled_bash_script, \
    check_file_exist_colon, \
    check_job_input_files

from .job_tools import release as jt_release
from .scheduler import Scheduler

class Job(object):
    def __init__(
            self,
            nproc: int,
            exe_cmd: str = None,
            modules: str = None,
            scheduler: Scheduler = None,
            model_start_time: str = None,
            model_end_time: str = None,
            model_restart: bool = True,
            entry_cmd: str = None,
            exit_cmd: str = None
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

        self.entry_cmd = entry_cmd
        """str: A command line command to execute before the model execution (after module load)."""
        self.exit_cmd = exit_cmd
        """str: A command line command to execute after the model execution (after module load)."""

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
                    error_msg = "The number of cores passed to the job does not match the "
                    error_msg += "number of cores specified in the job's scheduler."
                    raise ValueError(error_msg)
            else:
                self.scheduler.nproc = self.nproc

        # TODO(JLM): Maybe this should be done first and overwritten?
        # TODO(JLM): For missing job/scheduler properties, attempt to get defaults.
        # These are in a file in the package dir. Where?
        # A method (used by  django) about specifying the root dir of the project.
        # https://stackoverflow.com/questions/25389095/python-get-path-of-root-project-structure
        # self.build_default_job(self)
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
            hold: bool = False,
            submit_array=False
    ) -> object:
        """Scheulde a run of the wrf_hydro simulation
        Args:
            self: A Self object
        """

        if not submit_array:
            # Deal with the shared queue here, since it affects both scripts.
            if get_machine() == 'cheyenne' and \
                                    self.scheduler.nnodes - 1 == 0 and \
                            self.scheduler.nproc_last_node <= 18:
                self.scheduler.queue = 'share'
                warnings.warn("Less than 18 procesors requested, using the 'share' queue.")
                find = 'mpiexec_mpt'
                repl = 'mpirun {hostname} -np ' + str(self.nproc)
                self.exe_cmd = self.exe_cmd.replace(find, repl)

            # Write python to be executed by the bash script given to the scheduler.
            # Execute the model from python script and the python script from the bash script:
            # swap their execution commands.

            model_exe_cmd = self.exe_cmd
            py_script_name = str(run_dir / (self.job_date_id + ".wrfhydropy.py"))
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
            selfstr = compose_scheduled_python_script(py_run_cmd, model_exe_cmd)
            with open(py_script_name, "w") as myfile:
                myfile.write(selfstr)

            # The bash submission script which calls the python script.
            self.exe_cmd = py_run_cmd

        # Only complete the scheduling if not a job array or
        # if there is a specific flag to complete the job array.
        if (not self.scheduler.array_size) or (self.scheduler.array_size and submit_array):

            filename = run_dir / (self.job_date_id + '.' + self.scheduler.sched_name + '.job')
            jobstr = compose_scheduled_bash_script(run_dir=run_dir, job=self)

            with open(filename, "w") as myfile:
                myfile.write(jobstr)

            try:

                sched_job_id = submit_scheduler(
                    substr=bytearray(jobstr, 'utf-8'),
                    sched_name=self.scheduler.sched_name,
                    hold=hold
                )

            except PBSError as e:
                self.scheduler.not_submitted = True
                raise e

            self.scheduler.sched_job_id = sched_job_id

            if self.scheduler.array_size:
                # This is really convention-y. Not great.
                [touch(str(run_dir) + '/member_{0}/.job_not_complete'.format("%03d" % (ii,)))
                 for ii in range(self.scheduler.array_size)]
            else:
                touch(str(run_dir) + '/.job_not_complete')
            return sched_job_id

    def release(self):
        return (jt_release(self.scheduler))

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
            # exe_cmd = self.exe_cmd.format(**{'hostname': socket.gethostname()}) + " 2> {0} 1> {1}"
            std_base = "{0}.{1}".format(
                self.job_date_id,
                self.scheduler.sched_job_id
            )

            self.stderr_file = run_dir / (std_base + ".stderr")
            self.stdout_file = run_dir / (std_base + ".stdout")

            # source the modules before execution.
            exe_cmd = '/bin/bash -c "'
            if self.modules:
                exe_cmd += "module purge && module load " + self.modules + " && "

            if self.entry_cmd is not None:
                exe_cmd += self.entry_cmd
                exe_cmd += " 2>> {0} 1>> {1}".format(self.stderr_file, self.stdout_file)
                exe_cmd += " && "
                print("entry_cmd:", self.entry_cmd, '\n')

            exe_cmd += self.exe_cmd.format(**{'nproc': self.nproc})
            exe_cmd += " 2>> {0} 1>> {1}"
            exe_cmd = exe_cmd.format(self.stderr_file, self.stdout_file)

            if self.exit_cmd is not None:
                exe_cmd += " && " + self.exit_cmd
                exe_cmd += " 2>> {0} 1>> {1}".format(self.stderr_file, self.stdout_file)
                print("exit_cmd:", self.exit_cmd, '\n')

            exe_cmd += '"'

            self.exe_cmd = exe_cmd
            print("exe_cmd: ", exe_cmd, '\n')

            exe_cmd = shlex.split(exe_cmd)
            proc = subprocess.Popen(exe_cmd, cwd=run_dir)
            # proc.wait()

            proc = subprocess.run(exe_cmd, cwd=run_dir)

        else:

            # These dont have the sched_job_id that the scheduled job output files have.
            self.stderr_file = run_dir / ("{0}.stderr".format(self.job_date_id))
            self.stdout_file = run_dir / ("{0}.stdout".format(self.job_date_id))

            # source the modules before execution.
            exe_cmd = '/bin/bash -c "'
            if self.modules:
                exe_cmd += "module purge && module load " + self.modules + " && "

            if self.entry_cmd is not None:
                exe_cmd += self.entry_cmd
                exe_cmd += " 2>> {0} 1>> {1}".format(self.stderr_file, self.stdout_file) + " && "

            exe_cmd += self.exe_cmd.format(**{'nproc': self.nproc})
            exe_cmd += " 2>> {0} 1>> {1}".format(self.stderr_file, self.stdout_file)

            if self.exit_cmd is not None:
                exe_cmd += " && " + self.exit_cmd
                exe_cmd += " 2>> {0} 1>> {1}".format(self.stderr_file, self.stdout_file)

            exe_cmd += '"'
            self.exe_cmd = exe_cmd

            self.job_status = 'running'
            self.job_start_time = str(datetime.datetime.now())

            proc = subprocess.Popen(shlex.split(exe_cmd), cwd=run_dir)
            self.run_log = proc.wait()

            self.job_end_time = str(datetime.datetime.now())
            self.job_status = 'completed'

        # TODO(JLM): The following be made a method which checks the run.
        #            The following should not be run if the scheduler is not waiting.
        #            Put this in collect_run?

        try:

            # Get diag files
            # TODO(JLM): diag_files should be scrapped orrenamed to no conflict between jobs.
            run_dir_posix = pathlib.PosixPath(run_dir)
            self.diag_files = list(run_dir_posix.glob('diag_hydro.*'))

            # self.stdout_file = list(run_dir_posix.glob(self.job_date_id+'.*stdout'))[0]
            # self.stderr_file = list(run_dir_posix.glob(self.job_date_id+'.*stderr'))[0]

            self.tracejob_file = list(run_dir_posix.glob(self.job_date_id + '.*tracejob'))
            if len(self.tracejob_file):
                self.tracejob_file = self.tracejob_file[0]
            else:
                self.tracejob_file = None

            self.exit_status = 1
            self.job_status = 'completed failure'
            # String match diag files for successfull run
            with run_dir.joinpath('diag_hydro.00000').open() as f:
                diag_file = f.read()
                if 'The model finished successfully.......' in diag_file:
                    self.exit_status = 0
                    self.job_status = 'completed success'

        except Exception as e:

            # raise ValueError('Could not parse diag files, run_dir:' + str(run_dir) )
            print(e)

    def write_namelists(self, run_dir):

        # TODO(JLM): make the setup namelists @properties without setter (protect them)
        # write hydro.namelist for the job
        self.hydro_namelist_file = run_dir.joinpath(self.job_date_id + '.hydro.namelist')
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
            return (the_str)
        dict = {'run_dir': run_dir,
                'job_date_id': self.job_date_id,
                'sched_job_id': self.scheduler.sched_job_id}
        if dict['sched_job_id'] is None: dict['sched_job_id'] = '${sched_job_id}'
        return (the_str.format(**dict))

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
            noah_nlst.pop('khour')
        else:
            noah_nlst['khour'] = int(duration.days * 60 + duration.seconds / 3600)
            noah_nlst.pop('kday')

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

        lsm_restart_dirname = '.'  # os.path.dirname(noah_nlst['restart_filename_requested'])
        hydro_restart_dirname = '.'  # os.path.dirname(hydro_nlst['restart_file'])

        # 2011082600 - no minutes
        lsm_restart_basename = 'RESTART.' + \
                               self.model_start_time.strftime('%Y%m%d%H') + '_DOMAIN1'
        # 2011-08-26_00_00 - minutes
        hydro_restart_basename = 'HYDRO_RST.' + \
                                 self.model_start_time.strftime('%Y-%m-%d_%H:%M') + '_DOMAIN1'

        lsm_restart_file = lsm_restart_dirname + '/' + lsm_restart_basename
        hydro_restart_file = hydro_restart_dirname + '/' + hydro_restart_basename

        if not self.model_restart:
            lsm_restart_file = '!!! ' + lsm_restart_file
            hydro_restart_file = '!!! ' + hydro_restart_file

        noah_nlst['restart_filename_requested'] = lsm_restart_file
        hydro_nlst['restart_file'] = hydro_restart_file

