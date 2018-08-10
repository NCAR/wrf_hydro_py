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
            model_start_time: Union[str,pd.datetime] = None,
            model_end_time: Union[str,pd.datetime] = None,
            restart: bool = True,
            exe_cmd: str = None,
            entry_cmd: str = None,
            exit_cmd: str = None):
        """Instatiate a Job object.
        Args:
            job_id: A string identify the job
            model_start_time: The model start time to use for the WRF-Hydro model run. Can be
            a pandas.to_datetime compatible string or a pandas datetime object.
            model_end_time: The model end time to use for the WRF-Hydro model run. Can be
            a pandas.to_datetime compatible string or a pandas datetime object.
            restart: Job is starting from a restart file. Use False for a cold start.
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

        self._model_start_time = pd.to_datetime(model_start_time)
        """np.datetime64: The model time at the start of the execution."""

        self._model_end_time = pd.to_datetime(model_end_time)
        """np.datetime64: The model time at the end of the execution."""

        ## property construction
        self._hrldas_times = {'noahlsm_offline':
                                  {'kday': None,
                                   'khour': None,
                                   'start_year': None,
                                   'start_month': None,
                                   'start_day': None,
                                   'start_hour': None,
                                   'start_min': None,
                                   'restart_filename_requested': None}
                              }

        self._hydro_times = {'hydro_nlist':
                                 {'restart_file': None},
                             'nudging_nlist':
                                 {'nudginglastobsfile': None}
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

    def pickle(self,path: str):
        """Pickle sim object to specified file path
        Args:
            path: The file path for pickle
        """
        path = pathlib.Path(path)
        with path.open(mode='wb') as f:
            pickle.dump(self, f, 2)

    def _run(self):
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
        shutil.copy(str(hydro_namelist_path),str(current_dir))
        shutil.copy(str(hrldas_namelist_path),str(current_dir))

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

        # Set start time of job execution
        self.job_start_time = str(datetime.datetime.now())

        self._proc_log = subprocess.run(cmd_string,
                                        shell = True,
                                        cwd=str(current_dir))

        self.job_end_time = str(datetime.datetime.now())

        # String match diag files or stdout for successfull run if running on gfort or intel
        # Gfort outputs it to diag, intel outputs it to stdout
        diag_file = current_dir.joinpath('diag_hydro.00000')
        if diag_file.exists():
            #Check diag files first
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

                shutil.move(str(self.stdout_file),str(self.job_dir))
                shutil.move(str(self.stderr_file),str(self.job_dir))
                current_dir.joinpath('hydro.namelist').unlink()
                current_dir.joinpath('namelist.hrldas').unlink()
            else:
                self.exit_status = 1
        else:
            self.exit_status = 1
            self.pickle(str(self.job_dir.joinpath('WrfHydroJob_postrun.pkl')))
            raise RuntimeError('Model did not finish successfully')

        self.pickle(str(self.job_dir.joinpath('WrfHydroJob_postrun.pkl')))

    def _write_namelists(self):
        """Private method to write namelist dicts to FORTRAN namelist files"""
        self.hydro_namelist.write(str(self.job_dir.joinpath('hydro.namelist')))
        self.hrldas_namelist.write(str(self.job_dir.joinpath('namelist.hrldas')))

    def _set_hrldas_times(self):
        """Private method to set model run times in the hrldas namelist"""

        if self._model_start_time is not None and self._model_end_time is not None:
            duration = self._model_end_time - self._model_start_time
            if duration.seconds == 0:
                self._hrldas_times['noahlsm_offline']['kday'] = int(duration.days)
            else:
                self._hrldas_times['noahlsm_offline']['khour'] =int(duration.days * 60 +
                                                                    duration.seconds / 3600)

            # Start
            self._hrldas_times['noahlsm_offline']['start_year'] = int(self._model_start_time.year)
            self._hrldas_times['noahlsm_offline']['start_month'] = int(self.model_start_time.month)
            self._hrldas_times['noahlsm_offline']['start_day'] = int(self._model_start_time.day)
            self._hrldas_times['noahlsm_offline']['start_hour'] = int(self._model_start_time.hour)
            self._hrldas_times['noahlsm_offline']['start_min'] = int(self._model_start_time.minute)

            if self.restart:
                lsm_restart_dirname = '.'  # os.path.dirname(noah_nlst['restart_filename_requested'])

                # Format - 2011082600 - no minutes
                lsm_restart_basename = 'RESTART.' + \
                                       self._model_start_time.strftime('%Y%m%d%H') + '_DOMAIN1'

                lsm_restart_file = lsm_restart_dirname + '/' + lsm_restart_basename

                self._hrldas_times['noahlsm_offline']['restart_filename_requested'] = lsm_restart_file

    def _set_hydro_times(self):
        """Private method to set model run times in the hydro namelist"""

        if self._model_start_time is not None and self.restart:
            # Format - 2011-08-26_00_00 - minutes
            hydro_restart_basename = 'HYDRO_RST.' + \
                                     self._model_start_time.strftime('%Y-%m-%d_%H:%M') + '_DOMAIN1'

            # Format - 2011-08-26_00_00 - seconds
            nudging_restart_basename = 'nudgingLastObs.' + \
                                     self._model_start_time.strftime('%Y-%m-%d_%H:%M:%S') + '.nc'

            # Use convenience function to return name of file with or without colons in name
            # This is needed because the model outputs restarts with colons, and our distributed
            # domains do not have restarts with colons so that they can be easily shared across file
            # systems
            #hydro_restart_file = _check_file_exist_colon(os.getcwd(),hydro_restart_basename)
            #nudging_restart_file = _check_file_exist_colon(os.getcwd(),nudging_restart_basename)

            self._hydro_times['hydro_nlist']['restart_file'] = hydro_restart_basename
            self._hydro_times['nudging_nlist']['nudginglastobsfile'] = nudging_restart_basename

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
        with open(pystr_file,mode='w') as f:
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
        elif 'kday' in noah_namelist.keys():
            duration = {'days': noah_namelist['kday']}
        else:
            raise ValueError("Neither KDAY nor KHOUR in namelist.hrldas.")
        model_end_time = model_start_time + datetime.timedelta(**duration)

        return model_start_time, model_end_time

    def pickle(self,path: str):
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
        return self._hydro_namelist.patch(self.hydro_times)

    @property
    def hrldas_namelist(self):
        if self.model_start_time is None or self.model_end_time is None:
            warnings.warn('model start or end time was not specified in job, start end times will \
            be used from supplied namelist')
            self._model_start_time, self._model_end_time = self._solve_model_start_end_times()
        return self._hrldas_namelist.patch(self.hrldas_times)

    @property
    def model_start_time(self):
        """np.datetime64: The model time at the start of the execution."""
        return self._model_start_time

    @model_start_time.setter
    def model_start_time(self,value):
        self._model_start_time = pd.to_datetime(value)

    @property
    def model_end_time(self):
        return self._model_end_time

    @model_end_time.setter
    def model_end_time(self, value):
        """np.datetime64: The model time at the start of the execution."""
        self._model_end_time = pd.to_datetime(value)

