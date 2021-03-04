import copy
import datetime
import os
import pathlib
import pickle
import shutil
import subprocess
import warnings
from typing import Union

import pandas as pd

from .ioutils import _check_file_exist_colon
from .namelist import Namelist


class Job(object):
    """A Job represents run-time specific information for a given WRF-Hydro run. A Simulation
    consists of one or more jobs. For example, adding multiple Jobs can be used to split a
    Simulation into multiple runs to limit the wall-clock duration of each individual run."""
    def __init__(
            self,
            job_id: str,
            model_start_time: Union[str, datetime.datetime] = None,
            model_end_time: Union[str, datetime.datetime] = None,
            restart_freq_hr: Union[int, dict] = None,
            output_freq_hr: Union[int, dict] = None,
            restart: bool = True,
            restart_file_time: Union[str, datetime.datetime, dict] = None,
            restart_dir: Union[str, pathlib.Path, dict] = None,
            exe_cmd: str = None,
            entry_cmd: str = None,
            exit_cmd: str = None
    ):

        """Instatiate a Job object.
        Args:
        job_id: A string identify the job
        model_start_time: The model start time to use for the WRF-Hydro model run. Can be
            a pandas.to_datetime compatible string or a pandas datetime object.
        model_end_time: The model end time to use for the WRF-Hydro model run. Can be
            a pandas.to_datetime compatible string or a pandas datetime object.
        restart_freq_hr: Restart write frequency, hours. Either an int or a dict. If int: Output
            write frequency, hours. If dict, must be of the form {'hydro': int, 'hrldas': int}
            which sets them independently.  Non-positive values (those <=0) set the restart
            frequency for both models to -99999, which gives restarts at start of each month.
        output_freq_hr: Either an int or a dict. If int: Output write frequency, hours. If dict,
            must be of the form {'hydro': int, 'hrldas': int} which sets them independently.
        restart: Job is starting from a restart file. Use False for a cold start.
        restart_dir: The path in which to look for the restart files.
        restart_file_time: The time on the restart file, if not the same as the model_start_time.
            Eithera string (e.g. '2000-01-01 00') or a datetime object (datetime) or a dict
            the form {'hydro': date1, 'hrldas': date2}  where dates are either strings or datetime
            objects.
        exe_cmd: The system-specific command to execute WRF-Hydro, for example 'mpirun -np
            36 ./wrf_hydro.exe'. Can be left as None if jobs is added to a scheduler or if a
            scheduler is used in a simulation.
        entry_cmd: A command to run prior to executing WRF-Hydro, such as loading modules or
            libraries.
        exit_cmd: A command to run after completion of the job.
        """

        # Attributes set at instantiation through arguments
        self._exe_cmd = exe_cmd
        """str: The job-specfific command to be executed. If None command is taken from machine
        class"""

        self._entry_cmd = entry_cmd
        """str: A command line command to execute before the exe_cmd"""

        self._exit_cmd = exit_cmd
        """str: A command line command to execute after the exe_cmd"""

        self.job_id = job_id
        """str: The job id."""

        self.restart = restart
        """bool: Start model from a restart."""

        self.restart_file_time = restart_file_time
        """np.datetime: Time on the restart file to use, if different from model_start_time. The
           path in any supplied restart file path in the namelists is preserved while modifying 
           the date and time."""

        if self.restart_file_time is None:
            self._restart_file_time_hydro = pd.to_datetime(model_start_time)
            self._restart_file_time_hrldas = pd.to_datetime(model_start_time)
        elif (isinstance(self.restart_file_time, datetime.datetime) or
              isinstance(self.restart_file_time, str)):
            self._restart_file_time_hydro = pd.to_datetime(self.restart_file_time)
            self._restart_file_time_hrldas = pd.to_datetime(self.restart_file_time)
        elif isinstance(self.restart_file_time, dict):
            self._restart_file_time_hydro = pd.to_datetime(self.restart_file_time['hydro'])
            self._restart_file_time_hrldas = pd.to_datetime(self.restart_file_time['hrldas'])
        else:
            raise ValueError("restart_file_time is an in appropriate type.")

        self.restart_dir = restart_dir
        if self.restart_dir is None:
            self._restart_dir_hydro = None
            self._restart_dir_hrldas = None
        elif (isinstance(self.restart_dir, str) or
              isinstance(self.restart_dir, pathlib.Path)):
            self._restart_dir_hydro = pathlib.Path(self.restart_dir)
            self._restart_dir_hrldas = pathlib.Path(self.restart_dir)
        elif isinstance(self.restart_dir, dict):
            self._restart_dir_hydro = pathlib.Path(self.restart_file_time['hydro'])
            self._restart_dir_hrldas = pathlib.Path(self.restart_file_time['hrldas'])
        else:
            raise ValueError("restart_file_time is an in appropriate type.")

        self._model_start_time = pd.to_datetime(model_start_time)
        """np.datetime64: The model time at the start of the execution."""

        self._model_end_time = pd.to_datetime(model_end_time)
        """np.datetime64: The model time at the end of the execution."""

        if isinstance(restart_freq_hr, dict):
            if 'hydro' in restart_freq_hr.keys():
                restart_freq_hr_hydro = restart_freq_hr['hydro']
            else:
                restart_freq_hr_hydro = None

            if 'hrldas' in restart_freq_hr.keys():
                restart_freq_hr_hrldas = restart_freq_hr['hrldas']
            else:
                restart_freq_hr_hrldas = None

        else:
            restart_freq_hr_hydro = restart_freq_hr
            restart_freq_hr_hrldas = restart_freq_hr

        self.restart_freq_hr_hydro = restart_freq_hr_hydro
        """int: Hydro restart write frequency in hours."""
        self.restart_freq_hr_hrldas = restart_freq_hr_hrldas
        """int: Hrldas restart write frequency in hours."""

        if isinstance(output_freq_hr, dict):
            if 'hydro' in output_freq_hr.keys():
                output_freq_hr_hydro = output_freq_hr['hydro']
            else:
                output_freq_hr_hydro = None

            if 'hrldas' in output_freq_hr.keys():
                output_freq_hr_hrldas = output_freq_hr['hrldas']
            else:
                output_freq_hr_hrldas = None

        else:
            output_freq_hr_hydro = output_freq_hr
            output_freq_hr_hrldas = output_freq_hr

        self.output_freq_hr_hydro = output_freq_hr_hydro
        """int: Hydro output write frequency in hours."""
        self.output_freq_hr_hrldas = output_freq_hr_hrldas
        """int: Hrldas output write frequency in hours."""

        # property construction
        self._hrldas_times = {
            'noahlsm_offline':
            {
                'khour': None,
                'restart_frequency_hours': None,
                'output_timestep': None,
                'start_year': None,
                'start_month': None,
                'start_day': None,
                'start_hour': None,
                'start_min': None,
                'restart_filename_requested': None
            }
        }

        self._hydro_times = {
            'hydro_nlist': {'restart_file': None,
                            'rst_dt': None,
                            'out_dt': None},
            'nudging_nlist': {'nudginglastobsfile': None}
        }

        self._hydro_namelist = None
        self._hrldas_namelist = None

        self.exit_status = None
        """int: The exit status of the model job parsed from WRF-Hydro diag files"""

        # Attributes set by Scheduler class if job is used in scheduler
        self._job_start_time = None
        """str?: The time at the start of the execution."""

        self._job_end_time = None
        """str?: The time at the end of the execution."""

        self._job_submission_time = None
        """str?: The time the job object was created."""

    def _add_hydro_namelist(self, namelist: Namelist):
        """Add a hydro_namelist Namelist object to the job object
        Args:
            namelist: The Namelist to add
        """
        self._hydro_namelist = copy.deepcopy(namelist)

    def _add_hrldas_namelist(self, namelist: dict):
        """Add a hrldas_namelist Namelist object to the job object
        Args:
            namelist: The namelist dictionary to add
        """
        self._hrldas_namelist = copy.deepcopy(namelist)
        # Never use KDAY in wrfhydropy. This eliminates it entering the patch with the time info.
        self._hrldas_namelist['noahlsm_offline'].pop('kday', None)

    def clone(self, N) -> list:
        """Clone a job object N-times using deepcopy.
        Args:
            N: The number of time to clone the Job
        Returns:
            A list of Job objects
        """
        clones = []
        for ii in range(N):
            clones.append(copy.deepcopy(self))
        return clones

    def pickle(self, path: str):
        """Pickle sim object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)

    def _run(
        self,
        env: dict = None
    ):
        """Private method to run a job"""

        # Create curent dir path to use for all operations. Needed so that everything can be run
        # relative to the simulation directory
        current_dir = pathlib.Path(os.curdir)

        # Print some basic info about the run
        print('\nRunning job ' + self.job_id + ': ')
        print('    Wall start time: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('    Model start time: ' + self.model_start_time.strftime('%Y-%m-%d %H:%M'))
        print('    Model end time: ' + self.model_end_time.strftime('%Y-%m-%d %H:%M'))

        # Check for restart files both as specified and in the run dir..
        # Alias the mutables for some brevity
        hydro_nlst = self.hydro_namelist['hydro_nlist']
        hydro_nlst['restart_file'] = _check_file_exist_colon(current_dir, hydro_nlst[
            'restart_file'])
        nudging_nlst = self.hydro_namelist['nudging_nlist']
        if nudging_nlst:
            nudging_nlst['nudginglastobsfile'] = \
                _check_file_exist_colon(current_dir, nudging_nlst['nudginglastobsfile'])

        # Copy namelists from job_dir to current_dir
        hydro_namelist_path = self.job_dir.joinpath('hydro.namelist')
        hrldas_namelist_path = self.job_dir.joinpath('namelist.hrldas')
        shutil.copy(str(hydro_namelist_path), str(current_dir))
        shutil.copy(str(hrldas_namelist_path), str(current_dir))

        # These dont have the sched_job_id that the scheduled job output files have.
        self.stderr_file = current_dir / ("{0}.stderr".format(self.job_id))
        self.stdout_file = current_dir / ("{0}.stdout".format(self.job_id))

        # Fromulate bash command string
        cmd_string = '/bin/bash -c "'
        if self._entry_cmd is not None:
            cmd_string += self._entry_cmd + ';'

        # Pipe outputs to file using shell. This is required because of large stdout and stderr
        # on large domains overflows either the python or os buffer
        cmd_string += self._exe_cmd
        cmd_string += (" 2> " + str(self.stderr_file) + " 1>" + str(self.stdout_file))
        cmd_string += ';'

        if self._exit_cmd is not None:
            cmd_string += self._exit_cmd

        cmd_string += '"'

        # Set start and end times
        # 1) wall time of job execution in the job object and
        # 2) (write to) file the model start and stop times.

        file_model_start_time = current_dir / '.model_start_time'
        file_model_end_time = current_dir / '.model_end_time'
        if file_model_end_time.exists():
            file_model_end_time.unlink()
        # Write the model start time now, but only write the file .model_end_time
        # upon successful completion. See below.
        with file_model_start_time.open(mode='w') as opened_file:
            _ = opened_file.write(str(self._model_start_time))

        self.job_start_time = str(datetime.datetime.now())

        if env is None or env == 'None':
            self._proc_log = subprocess.run(
                cmd_string,
                shell=True,
                cwd=str(current_dir)
            )
        else:
            self._proc_log = subprocess.run(
                cmd_string,
                shell=True,
                cwd=str(current_dir),
                env=env
            )

        self.job_end_time = str(datetime.datetime.now())

        # String match diag files or stdout for successfull run if running on gfort or intel
        # Gfort outputs it to diag, intel outputs it to stdout
        diag_file = current_dir.joinpath('diag_hydro.00000')
        if diag_file.exists():
            # Check diag files first
            with diag_file.open() as f:
                diag_file = f.read()
                if 'The model finished successfully.......' in diag_file:
                    diag_exit_status = 0
                else:
                    diag_exit_status = 1

            # Check stdout files second
            with self.stdout_file.open() as f:
                stdout_file = f.read()
                if 'The model finished successfully.......' in stdout_file:
                    stdout_exit_status = 0
                else:
                    stdout_exit_status = 1

            if diag_exit_status == 0 or stdout_exit_status == 0:
                self.exit_status = 0

                # cleanup job-specific run files
                diag_files = current_dir.glob('*diag*')
                for file in diag_files:
                    shutil.move(str(file), str(self.job_dir))

                shutil.move(str(self.stdout_file), str(self.job_dir))
                shutil.move(str(self.stderr_file), str(self.job_dir))
                current_dir.joinpath('hydro.namelist').unlink()
                current_dir.joinpath('namelist.hrldas').unlink()
            else:
                self.exit_status = 1
        else:
            self.exit_status = 1
            self.pickle(str(self.job_dir.joinpath('WrfHydroJob_postrun.pkl')))
            raise RuntimeError('Model did not finish successfully')

        # Only write the file .model_end_time upon successful completion.
        if self.exit_status == 0:
            with file_model_end_time.open('w') as opened_file:
                _ = opened_file.write(str(self._model_end_time))

        self.pickle(str(self.job_dir.joinpath('WrfHydroJob_postrun.pkl')))

    def _write_namelists(self, mode='x'):
        """Private method to write namelist dicts to FORTRAN namelist files"""
        self.hrldas_namelist.write(str(self.job_dir.joinpath('namelist.hrldas')), mode=mode)
        self.hydro_namelist.write(str(self.job_dir.joinpath('hydro.namelist')), mode=mode)

    def _set_hrldas_times(self):
        """Private method to set model run times in the hrldas namelist"""

        if self._model_start_time is not None and self._model_end_time is not None:
            duration = self._model_end_time - self._model_start_time

            # Only use KHOUR. Never use KDAY in wrfhydropy.
            self._hrldas_times['noahlsm_offline']['khour'] = \
                int((duration.days * 24) + (duration.seconds / 3600))

            # Start
            self._hrldas_times['noahlsm_offline']['start_year'] = int(self._model_start_time.year)
            self._hrldas_times['noahlsm_offline']['start_month'] = int(self.model_start_time.month)
            self._hrldas_times['noahlsm_offline']['start_day'] = int(self._model_start_time.day)
            self._hrldas_times['noahlsm_offline']['start_hour'] = int(self._model_start_time.hour)
            self._hrldas_times['noahlsm_offline']['start_min'] = int(self._model_start_time.minute)

            if self.restart:
                if self._restart_dir_hrldas is not None:
                    lsm_restart_dirname = self._restart_dir_hrldas
                else:
                    noah_nlst = self._hrldas_namelist['noahlsm_offline']
                    if noah_nlst['restart_filename_requested'] is not None:
                        lsm_restart_dirname = os.path.dirname(
                            noah_nlst['restart_filename_requested'])
                    else:
                        lsm_restart_dirname = '.'

                # Format - 2011082600 - no minutes
                lsm_restart_basename = 'RESTART.' + \
                    self._restart_file_time_hrldas.strftime('%Y%m%d%H') + '_DOMAIN1'

                lsm_restart_file = str(pathlib.Path(lsm_restart_dirname) / lsm_restart_basename)

                self._hrldas_times[
                    'noahlsm_offline']['restart_filename_requested'] = lsm_restart_file

            # TODO(JLM): I dont love this if statement, it's a bit hacky.
            # Some of the tests call _set_hrldas/hydro_times when no namelist has been set.
            # That's why this is here.
            if self._hrldas_namelist is not None:
                noah_nlst = self._hrldas_namelist['noahlsm_offline']

                the_noahlsm_offline = self._hrldas_times['noahlsm_offline']
                if self.restart_freq_hr_hrldas is not None:
                    if self.restart_freq_hr_hrldas > 0:
                        the_noahlsm_offline['restart_frequency_hours'] = self.restart_freq_hr_hrldas
                    else:
                        the_noahlsm_offline['restart_frequency_hours'] = -99999
                else:
                    the_noahlsm_offline['restart_frequency_hours'] = \
                        noah_nlst['restart_frequency_hours']

                if self.output_freq_hr_hrldas is not None:
                    the_noahlsm_offline['output_timestep'] = self.output_freq_hr_hrldas * 3600
                else:
                    the_noahlsm_offline['output_timestep'] = noah_nlst['output_timestep']

    def _set_hydro_times(self):
        """Private method to set model run times in the hydro namelist"""

        if self._model_start_time is not None and self.restart:
            if self._restart_dir_hydro is not None:
                hydro_restart_dirname = self._restart_dir_hydro
            else:
                hydro_nlst = self._hydro_namelist['hydro_nlist']
                if hydro_nlst['restart_file'] is not None:
                    hydro_restart_dirname = os.path.dirname(hydro_nlst['restart_file'])
                else:
                    hydro_restart_dirname = '.'

            # Format - 2011-08-26_00_00 - minutes
            hydro_restart_basename = \
                'HYDRO_RST.' + self._restart_file_time_hydro.strftime('%Y-%m-%d_%H:%M') + '_DOMAIN1'

            # Format - 2011-08-26_00_00 - seconds
            nudging_restart_basename = (
                'nudgingLastObs.' +
                self._restart_file_time_hydro.strftime('%Y-%m-%d_%H:%M:%S') + '.nc')

            # Use convenience function to return name of file with or without colons in name
            # This is needed because the model outputs restarts with colons, and our distributed
            # domains do not have restarts with colons so that they can be easily shared across file
            # systems
            # hydro_restart_file = _check_file_exist_colon(os.getcwd(),hydro_restart_basename)
            # nudging_restart_file = _check_file_exist_colon(os.getcwd(),nudging_restart_basename)

            self._hydro_times['hydro_nlist']['restart_file'] = \
                str(pathlib.Path(hydro_restart_dirname) / hydro_restart_basename)
            self._hydro_times['nudging_nlist']['nudginglastobsfile'] = \
                str(pathlib.Path(hydro_restart_dirname) / nudging_restart_basename)

        # TODO(JLM): I dont love this if statement, it's a bit hacky. See comment above
        # for _set_hrldas_times.
        if self._hydro_namelist is not None:
            hydro_nlst = self._hydro_namelist['hydro_nlist']

            if self.restart_freq_hr_hydro is not None:
                if self.restart_freq_hr_hydro > 0:
                    self._hydro_times['hydro_nlist']['rst_dt'] = self.restart_freq_hr_hydro * 60
                else:
                    self._hydro_times['hydro_nlist']['rst_dt'] = -99999
            else:
                self._hydro_times['hydro_nlist']['rst_dt'] = hydro_nlst['rst_dt']

            if self.output_freq_hr_hydro is not None:
                self._hydro_times['hydro_nlist']['out_dt'] = self.output_freq_hr_hydro * 60
            else:
                self._hydro_times['hydro_nlist']['out_dt'] = hydro_nlst['out_dt']

    def _make_job_dir(self):
        """Private method to make the job directory"""
        if self.job_dir.is_dir():
            raise IsADirectoryError(str(self.job_dir) + 'already exists')
        else:
            self.job_dir.mkdir()

    def _write_run_script(self):
        """Private method to write a python script to run the job. This is used primarily for
        compatibility with job schedulers on HPC systems"""

        self.pickle(str(self.job_dir.joinpath('WrfHydroJob_prerun.pkl')))

        pystr = ""
        pystr += "# import modules\n"
        pystr += "import wrfhydropy\n"
        pystr += "import pickle\n"
        pystr += "import argparse\n"
        pystr += "import os\n"
        pystr += "import pathlib\n"

        pystr += "# Get path of this script to set working directory\n"
        pystr += "sim_dir = pathlib.Path(__file__)\n"
        pystr += "os.chdir(str(sim_dir.parent))\n"

        pystr += "parser = argparse.ArgumentParser()\n"
        pystr += "parser.add_argument('--job_id',\n"
        pystr += "                    help='The numeric part of the scheduler job ID.')\n"
        pystr += "args = parser.parse_args()\n"
        pystr += "\n"

        pystr += "#load job object\n"
        pystr += "job_file = 'job_' + args.job_id + '/WrfHydroJob_prerun.pkl'\n"
        pystr += "job = pickle.load(open(job_file,mode='rb'))\n"
        pystr += "#Run the job\n"
        pystr += "job._run()\n"

        pystr_file = 'run_job.py'
        with open(pystr_file, mode='w') as f:
            f.write(pystr)

    def _solve_model_start_end_times(self):
        """Private method ot get the model start and end times from the namelist"""
        noah_namelist = self._hrldas_namelist['noahlsm_offline']

        # model_start_time
        start_noah_keys = {'year': 'start_year', 'month': 'start_month',
                           'day': 'start_day', 'hour': 'start_hour', 'minute': 'start_min'}
        start_noah_times = {key: noah_namelist[value] for (key, value) in start_noah_keys.items()}
        model_start_time = datetime.datetime(**start_noah_times)

        # model_end_time
        if 'khour' in noah_namelist.keys():
            duration = {'hours': noah_namelist['khour']}
        else:
            raise ValueError("KHOUR is not in namelist.hrldas (wrfhydropy only uses KHOUR).")
        model_end_time = model_start_time + datetime.timedelta(**duration)

        return model_start_time, model_end_time

    def pickle(self, path: str):
        """Pickle job object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)

    @property
    def job_dir(self):
        """Path: Path to the run directory"""
        job_dir_name = 'job_' + self.job_id
        return pathlib.Path(job_dir_name)

    @property
    def hrldas_times(self):
        self._set_hrldas_times()
        return self._hrldas_times

    @property
    def hydro_times(self):
        self._set_hydro_times()
        return self._hydro_times

    @property
    def hydro_namelist(self):
        if self.model_start_time is None or self.model_end_time is None:
            warnings.warn('model start or end time was not specified in job, start end times will \
            be used from supplied namelist')
            self._model_start_time, self._model_end_time = self._solve_model_start_end_times()
        if self.restart_file_time is None and self.restart is True:
            self._restart_file_time_hydro = pd.to_datetime(self._model_start_time)
        return self._hydro_namelist.patch(self.hydro_times)

    @property
    def hrldas_namelist(self):
        if self.model_start_time is None or self.model_end_time is None:
            warnings.warn('model start or end time was not specified in job, start end times will \
            be used from supplied namelist')
            self._model_start_time, self._model_end_time = self._solve_model_start_end_times()
        if self.restart_file_time is None and self.restart is True:
            self._restart_file_time_hrldas = pd.to_datetime(self._model_start_time)
        return self._hrldas_namelist.patch(self.hrldas_times)

    @property
    def model_start_time(self):
        """datetime: The model time at the start of the execution."""
        return self._model_start_time

    @model_start_time.setter
    def model_start_time(self, value):
        self._model_start_time = pd.to_datetime(value)

    @property
    def model_end_time(self):
        """datetime: The model time at the end of the execution."""
        return self._model_end_time

    @model_end_time.setter
    def model_end_time(self, value):
        """np.datetime64: The model time at the start of the execution."""
        self._model_end_time = pd.to_datetime(value)
