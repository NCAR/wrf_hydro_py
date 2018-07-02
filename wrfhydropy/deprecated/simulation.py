import copy
import datetime
import pathlib
import pickle
import re
import shutil
import uuid
import warnings

from .utilities import \
    __make_relative__, \
    lock_pickle, \
    unlock_pickle

from .job_tools import \
    get_user, \
    solve_model_start_end_times

from .fileutilities import \
    WrfHydroStatic, \
    WrfHydroTs

from .job import Job

#########################
# netcdf file object classes

class WrfHydroSetup(object):
    """Class for a WRF-Hydro setup object, which is comprised of a WrfHydroModel and a
    WrfHydroDomain.
    """

    def __init__(
            self,
            wrf_hydro_model: object,
            wrf_hydro_domain: object
    ):
        """Instantiates a WrfHydroSetup object
        Args:
            wrf_hydro_model: A WrfHydroModel object
            wrf_hydro_domain: A WrfHydroDomain object
        Returns:
            A WrfHydroSetup object
        """

        # Validate that the domain and model are compatible
        if wrf_hydro_model.model_config != wrf_hydro_domain.domain_config:
            raise TypeError('Model configuration ' +
                            wrf_hydro_model.model_config +
                            ' not compatible with domain configuration ' +
                            wrf_hydro_domain.domain_config)
        if wrf_hydro_model.version not in list(wrf_hydro_domain.namelist_patches.keys()):
            raise TypeError('Model version ' +
                            wrf_hydro_model.versions +
                            ' not compatible with domain versions ' +
                            str(list(wrf_hydro_domain.namelist_patches.keys())))

        # assign objects to self
        self.model = copy.deepcopy(wrf_hydro_model)
        """WrfHydroModel: A copy of the WrfHydroModel object used for the setup"""

        self.domain = copy.deepcopy(wrf_hydro_domain)
        """WrfHydroDomain: A copy of the WrfHydroDomain object used for the setup"""

        # Create namelists
        self.hydro_namelist = \
            copy.deepcopy(self.model.hydro_namelists[self.model.version][self.domain.domain_config])
        """dict: A copy of the hydro_namelist used by the WrfHydroModel for the specified model 
        version and domain configuration"""

        self.namelist_hrldas = \
            copy.deepcopy(
                self.model.hrldas_namelists[self.model.version][self.domain.domain_config])
        """dict: A copy of the hrldas_namelist used by the WrfHydroModel for the specified model 
        version and domain configuration"""

        ## Update namelists with namelist patches
        self.hydro_namelist['hydro_nlist'].update(self.domain.namelist_patches
                                                  [self.model.version]
                                                  [self.domain.domain_config]
                                                  ['hydro_namelist']
                                                  ['hydro_nlist'])

        self.hydro_namelist['nudging_nlist'].update(self.domain.namelist_patches
                                                    [self.model.version]
                                                    [self.domain.domain_config]
                                                    ['hydro_namelist']
                                                    ['nudging_nlist'])

        self.namelist_hrldas['noahlsm_offline'].update(self.domain.namelist_patches
                                                       [self.model.version]
                                                       [self.domain.domain_config]
                                                       ['namelist_hrldas']
                                                       ['noahlsm_offline'])
        self.namelist_hrldas['wrf_hydro_offline'].update(self.domain.namelist_patches
                                                         [self.model.version]
                                                         [self.domain.domain_config]
                                                         ['namelist_hrldas']
                                                         ['wrf_hydro_offline'])

    # Dont self pickle, there's no natural location.
    def pickle(
            self,
            dir
    ):
        # create a UID for the run and save in file
        self.object_id = str(uuid.uuid4())
        with dir.joinpath('.uid').open(mode='w') as f:
            f.write(self.object_id)

        # Save object to run directory
        # Save the object out to the compile directory
        with dir.joinpath('WrfHydroSetup.pkl').open(mode='wb') as f:
            pickle.dump(self, f, 2)


class WrfHydroRun(object):
    def __init__(
            self,
            wrf_hydro_setup: WrfHydroSetup,
            run_dir: str,
            rm_existing_run_dir=False,
            deepcopy_setup=True
    ):
        """Instantiate a WrfHydroRun object. A run is a WrfHydroSetup with multiple jobs.
        Args:
            wrf_hydro_setup: A setup object.
            run_dir: Path to directory to execute the run.
            rm_existing_run_dir: Remove run directory if it exists
            deepcopy_setup: Create a deep copy of the setup object to use for the run.

        Returns:
            A WrfHydroRun object.
        """
        # Initialize all attributes and methods

        if deepcopy_setup:
            self.setup = copy.deepcopy(wrf_hydro_setup)
        else:
            self.setup = wrf_hydro_setup
        """WrfHydroSetup: The WrfHydroSetup object used for the run"""

        self.run_dir = pathlib.PosixPath(run_dir)
        """pathlib.PosixPath: The location of where the jobs will be executed."""

        self.jobs_completed = []
        """Job: A list of previously executed jobs for this run."""
        self.jobs_pending = []
        """Job: A list of jobs *scheduled* to be executed for this run 
        with prior job dependence."""
        self.job_active = None
        """Job: The job currently executing."""

        # TODO(JLM): these are properties of the run.
        self.channel_rt = list()
        """WrfHydroTs: Timeseries dataset of CHRTOUT files"""
        self.chanobs = list()
        """WrfHydroTs: Timeseries dataset of CHANOBS files"""
        self.lakeout = list()
        """WrfHydroTs: Timeseries dataset of LAKEOUT files"""
        self.gwout = list()
        """WrfHydroTs: Timeseries dataset of GWOUT files"""
        self.restart_hydro = list()
        """list: List of HYDRO_RST WrfHydroStatic objects"""
        self.restart_lsm = list()
        """list: List of RESTART WrfHydroStatic objects"""
        self.restart_nudging = list()
        """list: List of nudgingLastObs WrfHydroStatic objects"""

        self.object_id = None
        """str: A unique id to join object to run directory."""

        self._pickle_lock_file = None
        """pathlib.PosixPath: The pickle lock file path."""

        # Establish the values.

        # TODO(JLM): Check that the setup object is "complete".
        # TODO(JLM): What constitutes a complete sim object?
        #            1) compiler specified, 2) domain_config specified.
        #            3) A compiled model?

        # TODO(JLM): If adding a job to an existing run, enforce that only
        #            start times and khour/kday and associated restart file
        #            times are different? Anything else that's flexible across
        #            jobs of a single run?


        # Make run_dir directory if it does not exist.
        if self.run_dir.is_dir():
            if rm_existing_run_dir:
                shutil.rmtree(str(self.run_dir))
                self.run_dir.mkdir(parents=True)
            else:
                raise ValueError("Run directory already exists, mode='w', " +
                                 "and rm_existing_run_dir is False: clobbering not allowed.")
        else:
            self.run_dir.mkdir(parents=True)

        # Check that compile object uid matches compile directory uid
        # This is to ensure that a new model has not been compiled into that directory unknowingly
        with self.setup.model.compile_dir.joinpath('.uid').open() as f:
            compile_uid = f.read()

        if self.setup.model.object_id != compile_uid:
            raise PermissionError('object id mismatch between WrfHydroModel object and'
                                  'WrfHydroModel.compile_dir directory. Directory may have been'
                                  'used for another compile')

        # Build the inputs into the run_dir.
        # Construct all file/dir paths.
        # Convert strings to pathlib.Path objects.

        # TODO(JLM): Make symlinks the default option? Also allow copying?

        # Loop to make symlinks for each TBL file
        for from_file in self.setup.model.table_files:
            # Create file paths to symlink
            to_file = self.run_dir.joinpath(from_file.name)
            # Create symlinks
            to_file.symlink_to(from_file)

        # Symlink in exe
        wrf_hydro_exe = self.setup.model.wrf_hydro_exe
        self.run_dir.joinpath(wrf_hydro_exe.name).symlink_to(wrf_hydro_exe)

        # Symlink in forcing
        forcing_dir = self.setup.domain.forcing_dir
        self.run_dir.joinpath(forcing_dir.name). \
            symlink_to(forcing_dir, target_is_directory=True)

        # create DOMAIN directory and symlink in files
        # Symlink in hydro_files
        for file_path in self.setup.domain.hydro_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = file_path.relative_to(self.setup.domain.domain_top_dir)
            symlink_path = self.run_dir.joinpath(relative_path)
            if symlink_path.parent.is_dir() is False:
                symlink_path.parent.mkdir(parents=True)
            symlink_path.symlink_to(file_path)

        # Symlink in nudging files
        for file_path in self.setup.domain.nudging_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = file_path.relative_to(self.setup.domain.domain_top_dir)
            symlink_path = self.run_dir.joinpath(relative_path)
            if symlink_path.parent.is_dir() is False:
                symlink_path.parent.mkdir(parents=True)
            symlink_path.symlink_to(file_path)

        # Symlink in lsm files
        for file_path in self.setup.domain.lsm_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = file_path.relative_to(self.setup.domain.domain_top_dir)
            symlink_path = self.run_dir.joinpath(relative_path)
            if symlink_path.parent.is_dir() is False:
                symlink_path.parent.mkdir(parents=True)
            symlink_path.symlink_to(file_path)

        # Restart files are symlinked in to the run dir at run init.
        model_files = [*self.setup.domain.hydro_files,
                       *self.setup.domain.nudging_files,
                       *self.setup.domain.lsm_files]
        for ff in model_files:
            if re.match('.*/RESTART/.*', str(ff)):
                symlink_path = self.run_dir.joinpath(ff.name).absolute()
                symlink_path.symlink_to(ff)


    def add_jobs(
            self,
            jobs: list
    ):
        """Add jobs to the run object

        Args:
            jobs: List of Job objects
        """

        # Dont tamper with the passed object, let it remain a template in the calling level.
        jobs = copy.deepcopy(jobs)

        if type(jobs) is Job:
            jobs = [jobs]

        for jj in jobs:

            # Attempt to add the job
            if jj.scheduler:

                # A scheduled job can be appended to the jobs.pending list if
                # 1) there are no active or pending jobs
                # 2) if it is (made) dependent on the last active or pending job.

                # Get the job id of the last active or pending job.
                last_job_id = None
                if self.job_active:
                    last_job_id = self.job_active.sched_job_id
                if len(self.jobs_pending):
                    last_job_id = self.jobs_pending[-1].scheduler.sched_job_id

                # Check the dependency on a previous job
                if last_job_id is not None:
                    if jj.scheduler.afterok is not None and jj.scheduler.afterok != last_job_id:
                        raise ValueError("The job's dependency/afterok conflicts with reality.")
                    jj.scheduler.afterok = last_job_id
                else:
                    if jj.scheduler.afterok is not None:
                        raise ValueError("The job's dependency/afterok conflicts with reality.")

            # Set submission-time job variables here.
            jj.user = get_user()
            job_submission_time = datetime.datetime.now()
            jj.job_submission_time = str(job_submission_time)
            jj.job_date_id = '{date:%Y-%m-%d-%H-%M-%S-%f}'.format(date=job_submission_time)

            jj.model_start_time, jj.model_end_time = solve_model_start_end_times(
                jj.model_start_time,
                jj.model_end_time,
                self.setup
            )

            # Add a namelist to each job
            jj.namelist_hrldas = copy.deepcopy(self.setup.namelist_hrldas)
            jj.hydro_namelist = copy.deepcopy(self.setup.hydro_namelist)

            # Satisfying the model start/end times and restart options
            jj.apply_model_start_end_job_namelists()

            # Check the the resulting namelists are

            # in the job object?
            # Determine a different job_name?
            # Tag the namelists with the job name and symlink? When is the namelist written?

            # TODO(JLM):
            # Edit the namelists with model start/end times and if restarting.
            # Stash the namelists in the job.
            # This begs for consistency check across jobs: start previous job = end current job

            self.jobs_pending.append(jj)

    def run_jobs(self):

        # Make sure there are no active jobs?
        # make sure all jobs are either scheduled or interactive?

        run_dir = self.run_dir

        if self.jobs_pending[0].scheduler:

            # submit the jobs_pending.
            lock_pickle(self)
            job_afterok = None
            hold = True

            for jj in self.jobs_pending:
                jj.scheduler.afterok = job_afterok
                # TODO(JLM): why not make hold an attribute?
                jj.schedule(self.run_dir, hold=hold)
                job_afterok = jj.scheduler.sched_job_id
                hold = False

            self.pickle()
            unlock_pickle(self)
            self.jobs_pending[0].release()
            self.destruct()
            return run_dir

        else:

            for jj in range(0, len(self.jobs_pending)):
                self.job_active = self.jobs_pending.pop(0)
                self.job_active.run(self.run_dir)
                self.collect_output()
                self.jobs_completed.append(self.job_active)
                self.job_active = None
                self.pickle()

    def collect_output(self):

        if self.job_active.exit_status != 0:
            warnings.warn('Model run failed.')
            return (None)

        print('Model run succeeded.\n')
        #####################
        # Grab outputs as WrfHydroXX classes of file paths

        # Get channel files
        if len(list(self.run_dir.glob('*CHRTOUT*'))) > 0:
            self.channel_rt = WrfHydroTs(list(self.run_dir.glob('*CHRTOUT*')))
            # Make relative to run dir
            # for file in self.channel_rt:
            #     file.relative_to(file.parent)

        if len(list(self.run_dir.glob('*CHANOBS*'))) > 0:
            self.chanobs = WrfHydroTs(list(self.run_dir.glob('*CHANOBS*')))
            # Make relative to run dir
            # for file in self.chanobs:
            #     file.relative_to(file.parent)

        # Get Lakeout files
        if len(list(self.run_dir.glob('*LAKEOUT*'))) > 0:
            self.lakeout = WrfHydroTs(list(self.run_dir.glob('*LAKEOUT*')))

        # Get gwout files
        if len(list(self.run_dir.glob('*GWOUT*'))) > 0:
            self.gwout = WrfHydroTs(list(self.run_dir.glob('*GWOUT*')))

        # Get restart files and sort by modified time
        # Hydro restarts
        self.restart_hydro = []
        for file in self.run_dir.glob('HYDRO_RST*'):
            file = WrfHydroStatic(file)
            self.restart_hydro.append(file)

        if len(self.restart_hydro) > 0:
            self.restart_hydro = sorted(self.restart_hydro,
                                        key=lambda file: file.stat().st_mtime_ns)
        else:
            self.restart_hydro = None

        ### LSM Restarts
        self.restart_lsm = []
        for file in self.run_dir.glob('RESTART*'):
            file = WrfHydroStatic(file)
            self.restart_lsm.append(file)

        if len(self.restart_lsm) > 0:
            self.restart_lsm = sorted(self.restart_lsm,
                                      key=lambda file: file.stat().st_mtime_ns)
        else:
            self.restart_lsm = None

        ### Nudging restarts
        self.restart_nudging = []
        for file in self.run_dir.glob('nudgingLastObs*'):
            file = WrfHydroStatic(file)
            self.restart_nudging.append(file)

        if len(self.restart_nudging) > 0:
            self.restart_nudging = sorted(self.restart_nudging,
                                          key=lambda file: file.stat().st_mtime_ns)
        else:
            self.restart_nudging = None

        self.pickle()

    def pickle(self):
        # create a UID for the run and save in file
        self.object_id = str(uuid.uuid4())
        with self.run_dir.joinpath('.uid').open('w') as f:
            f.write(self.object_id)

        # Save object to run directory
        # Save the object out to the compile directory
        with self.run_dir.joinpath('WrfHydroRun.pkl').open('wb') as f:
            pickle.dump(self, f, 2)

    def unpickle(self):
        # Load run object from run directory after a scheduler job
        with self.run_dir.joinpath('WrfHydroRun.pkl').open('rb') as f:
            return (pickle.load(f))

    def destruct(self):
        # This gets rid of everything but the methods.
        print("Jobs have been submitted to  the scheduler: This run object will now self destruct.")
        self.__dict__ = {}

    def make_relative(self, basepath=None):
        """Make all file paths relative to a given directory, useful for opening file
        attributes in a run object after it has been moved or copied to a new directory or
        system.
        Args:
            basepath: The base path to use for relative paths. Defaults to run directory.
            This rarely needs to be defined.
        Returns:
            self with relative files paths for file-like attributes
        """
        __make_relative__(run_object=self, basepath=basepath)

