import pathlib
import numpy as np
import f90nml
import datetime
import shutil
import subprocess
import shlex
import warnings

from .fileutilities import check_file_exist_colon

class Job(object):
    def __init__(
            self,
            exe_cmd: str,
            job_id: str,
            entry_cmd: str = None,
            exit_cmd: str = None,
            model_start_time: np.datetime64 = None,
            model_end_time: np.datetime64 = None
    ):

        # Attributes set at instantiation through arguments
        self.exe_cmd = exe_cmd
        """str: The command to be executed."""

        self.entry_cmd = entry_cmd
        """str: A command line command to execute before the exe_cmd"""

        self.exit_cmd = exit_cmd
        """str: A command line command to execute after the exe_cmd"""

        self.job_id = job_id
        """str: The job id."""

        self.model_start_time = model_start_time
        """np.datetime64: The model time at the start of the execution."""

        self.model_end_time = model_end_time
        """np.datetime64: The model time at the end of the execution."""

        # Attributes set by class methods
        self.hrldas_times = {'noahlsm_offline':
                                 {'kday':None,
                                  'khour':None,
                                  'start_year':None,
                                  'start_month':None,
                                  'start_day': None,
                                  'start_hour':None,
                                  'start_min':None,
                                  'restart_filename_requested': None}
                             }
        """dict: the HRLDAS namelist used for this job."""

        self.hydro_times = {'hydro_nlist':
                                {'restart_file':None}
                            }
        """dict: the hydro namelist used for this job."""


        # Attributes set by Scheduler class if job is used in scheduler
        self.job_start_time = None
        """str?: The time at the start of the execution."""

        self.job_end_time = None
        """str?: The time at the end of the execution."""

        self.job_submission_time = None
        """str?: The time the job object was created."""

    def _set_hrldas_times(self):
        # Duration
        self.hrldas_times['kday'] = None
        self.hrldas_times['khour'] = None
        duration = self.model_end_time - self.model_start_time
        if duration.seconds == 0:
            self.hrldas_times['kday'] = int(duration.days)
            self.hrldas_times.pop('khour')
        else:
            self.hrldas_times['khour'] = int(duration.days * 60 + duration.seconds / 3600)
            self.hrldas_times.pop('kday')

        # Start
        self.hrldas_times['start_year'] = int(self.model_start_time.year)
        self.hrldas_times['start_month'] = int(self.model_start_time.month)
        self.hrldas_times['start_day'] = int(self.model_start_time.day)
        self.hrldas_times['start_hour'] = int(self.model_start_time.hour)
        self.hrldas_times['start_min'] = int(self.model_start_time.minute)

        lsm_restart_dirname = '.'  # os.path.dirname(noah_nlst['restart_filename_requested'])

        # Format - 2011082600 - no minutes
        lsm_restart_basename = 'RESTART.' + \
                               self.model_start_time.strftime('%Y%m%d%H') + '_DOMAIN1'

        lsm_restart_file = lsm_restart_dirname + '/' + lsm_restart_basename

        self.hrldas_times['restart_filename_requested'] = lsm_restart_file

    def _set_hydro_times(self):
        hydro_restart_dirname = '.'  # os.path.dirname(hydro_nlst['restart_file'])
        # Format - 2011-08-26_00_00 - minutes
        hydro_restart_basename = 'HYDRO_RST.' + \
                                 self.model_start_time.strftime('%Y-%m-%d_%H:%M') + '_DOMAIN1'
        hydro_restart_file = hydro_restart_dirname + '/' + hydro_restart_basename
        self.hydro_times['restart_file'] = hydro_restart_file

    def add_hydro_namelist(self, namelist: dict):
        self.hydro_namelist = namelist
        if self.model_start_time is None or self.model_end_time is None:
            warnings.warn('model start or end time was not specified in job, start end times will be '
                          'used from supplied namelist')
            self.model_start_time, self.model_end_time = self._solve_model_start_end_times()

        self._set_hydro_times()
        self.hydro_namelist.update(self.hydro_times)


    def add_hrldas_namelist(self, namelist: dict):
        self.hrldas_namelist = namelist
        if self.model_start_time is None or self.model_end_time is None:
            warnings.warn('model start or end time was not specified in job, start end times will be '
                          'used from supplied namelist')
            self.model_start_time, self.model_end_time = self._solve_model_start_end_times()
        self._set_hrldas_times()
        self.hrldas_namelist.update(self.hrldas_times)

    def write_namelists(self):
        """Write namelist dicts to FORTRAN namelist files
        Args:
            sim_dir: The top-level simulation directory. A new sub-directory for the job will be
            created and named after the job_id. Namelist files will be written into the job
            sub-directory
        """

        if not self.job_dir.is_dir():
            self.job_dir.mkdir(parents=True)

        f90nml.write(self.hydro_namelist,self.job_dir.joinpath('hydro.namelist'))
        f90nml.write(self.hrldas_namelist,self.job_dir.joinpath('namelist.hrldas'))

    def run(self):
        """Run the job
        Args:
            sim_dir: The top-level simulation directory. A new sub-directory for the job will be
            created and named after the job_id. Namelist files will be written into the job
            sub-directory
        """

        # Print some basic info about the run
        print('\nRunning job ' + self.job_id + ': ')
        print('    Wall start time: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('    Model start time: ' + self.model_start_time.strftime('%Y-%m-%d %H:%M'))
        print('    Model end time: ' + self.model_end_time.strftime('%Y-%m-%d %H:%M'))

        # Check for restart files both as specified and in the run dir..
        # Alias the mutables for some brevity
        hydro_nlst = self.hydro_namelist['hydro_nlist']
        hydro_nlst['restart_file'] = check_file_exist_colon(self._sim_dir, hydro_nlst[
            'restart_file'])
        nudging_nlst = self.hydro_namelist['nudging_nlist']
        if nudging_nlst:
            nudging_nlst['nudginglastobsfile'] = \
                check_file_exist_colon(self.sim_dir, nudging_nlst['nudginglastobsfile'])

        # Copy namelists from job_dir to sim_dir
        hydro_namelist_path = self.job_dir.joinpath('hydro.namelist').absolute()
        hrldas_namelist_path = self.job_dir.joinpath('namelist.hrldas').absolute()
        shutil.copy(str(hydro_namelist_path),str(self.sim_dir))
        shutil.copy(str(hrldas_namelist_path),str(self.sim_dir))

        # These dont have the sched_job_id that the scheduled job output files have.
        self.stderr_file = self.sim_dir / ("{0}.stderr".format(self.job_id))
        self.stdout_file = self.sim_dir / ("{0}.stdout".format(self.job_id))

        # Fromulate bash command string
        cmd_string = '/bin/bash -c "'
        if self.entry_cmd is not None:
            cmd_string += self.entry_cmd + ';'

        cmd_string += self.exe_cmd + ';'

        if self.exit_cmd is not None:
            cmd_string += self.exit_cmd

        cmd_string += '"'

        # Set start time of job execution
        self.job_start_time = str(datetime.datetime.now())

        self._proc_log = subprocess.run(shlex.split(cmd_string),
                                        cwd=self.sim_dir,
                                        stderr = open(self.stderr_file,mode='w'),
                                        stdout = open(self.stdout_file,mode='w'))

        self.job_end_time = str(datetime.datetime.now())

        # cleanup job-specific run files
        diag_files = self.sim_dir.glob('*diag*')
        for file in diag_files:
            shutil.move(str(file), str(self.job_dir))

        shutil.move(str(self.stdout_file),str(self.job_dir))
        shutil.move(str(self.stderr_file),str(self.job_dir))
        self.sim_dir.joinpath('hydro.namelist').unlink()
        self.sim_dir.joinpath('namelist.hrldas').unlink()

    def _solve_model_start_end_times(self):
        noah_namelist = self.hrldas_namelist['noahlsm_offline']
        # model_start_time
        start_noah_keys = {'year': 'start_year', 'month': 'start_month',
                           'day': 'start_day', 'hour': 'start_hour', 'minute': 'start_min'}
        start_noah_times = {kk: noah_namelist[vv] for (kk, vv) in start_noah_keys.items()}
        model_start_time = datetime.datetime(**start_noah_times)

        # model_end_time
        if 'khour' in noah_namelist.keys():
            duration = {'hours': noah_namelist['khour']}
        elif 'kday' in noah_namelist.keys():
            duration = {'days': noah_namelist['kday']}
        else:
            raise ValueError("Neither KDAY nor KHOUR in namelist.hrldas.")
        model_end_time = model_start_time + datetime.timedelta(**duration)

        return model_start_time, model_end_time

    # properties
    @property
    def sim_dir(self):
        return self._sim_dir
    @sim_dir.setter
    def sim_dir(self,path):
        self._sim_dir = pathlib.Path(path)

    @property
    def job_dir(self):
        """Path: Path to the run directory"""
        job_dir_name = '.job_' + self.job_id
        return self._sim_dir.joinpath(job_dir_name)
