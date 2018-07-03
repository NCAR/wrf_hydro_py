import pathlib
import numpy as np



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
        self.run_dir = pathlib.Path(job_id)
        """Path: Path to the run directory"""

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

    def _get_hrldas_times(self):
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

        return self.hrldas_times

    def _get_hydro_times(self):
        hydro_restart_dirname = '.'  # os.path.dirname(hydro_nlst['restart_file'])
        # Format - 2011-08-26_00_00 - minutes
        hydro_restart_basename = 'HYDRO_RST.' + \
                                 self.model_start_time.strftime('%Y-%m-%d_%H:%M') + '_DOMAIN1'
        hydro_restart_file = hydro_restart_dirname + '/' + hydro_restart_basename
        self.hydro_times['restart_file'] = hydro_restart_file

        return self.hydro_times


