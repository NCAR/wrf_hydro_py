import f90nml
import pathlib
import numpy as np

class Job(object):
    def __init__(
            self,
            run_dir: str,
            exe_cmd: str,
            entry_cmd: str = None,
            exit_cmd: str = None,
            job_id: str = None,
            model_start_time: np.datetime64 = None,
            model_end_time: np.datetime64 = None,
            find_restarts: bool = True
    ):

        self.run_dir = pathlib.Path(run_dir)
        """Path: Path to the run directory"""
        self.exe_cmd = exe_cmd
        """str: The command to be executed."""
        self.model_start_time = model_start_time
        """np.datetime64: The model time at the start of the execution."""
        self.model_end_time = model_end_time
        """np.datetime64: The model time at the end of the execution."""
        self.find_restarts = find_restarts
        """bool: Look for restart files at model_start_time?"""
        self.entry_cmd = entry_cmd
        """str: A command line command to execute before the exe_cmd"""
        self.exit_cmd = exit_cmd
        """str: A command line command to execute after the exe_cmd"""

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
        self.job_id = job_id
        """str: The job id."""
        self.job_start_time = None
        """str?: The time at the start of the execution."""
        self.job_end_time = None
        """str?: The time at the end of the execution."""
        self.job_submission_time = None
        """str?: The time the job object was created."""
        self.exit_status = None
        """int: The exit value of the model execution."""
        self.diag_files = None
        """pathlib.PosixPath: The diag files for the job."""

    def write_namelists(self):

        # TODO(JLM): make the setup namelists @properties without setter (protect them)

        # write hydro.namelist for the job
        self.hydro_namelist_file = self.run_dir.joinpath(self.job_id + '.hydro.namelist')
        f90nml.write(self.hydro_namelist, self.hydro_namelist_file)
        nlst_file = self.run_dir.joinpath('hydro.namelist')
        if nlst_file.exists():
            nlst_file.unlink()
        nlst_file.symlink_to(self.hydro_namelist_file)

        # write namelist.hrldas
        self.namelist_hrldas_file = self.run_dir.joinpath(self.job_id + '.namelist.hrldas')
        f90nml.write(self.namelist_hrldas, self.namelist_hrldas_file)
        nlst_file = self.run_dir.joinpath('namelist.hrldas')
        if nlst_file.exists():
            nlst_file.unlink()
        nlst_file.symlink_to(self.namelist_hrldas_file)

    def apply_model_start_end_job_namelists(self):
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

        noah_nlst['restart_filename_requested'] = lsm_restart_file
        hydro_nlst['restart_file'] = hydro_restart_file

