import copy
import datetime
import f90nml
import json
import os
import pathlib
import pickle
from shlex import split as shlex_split
import shutil
import subprocess
from time import sleep
import uuid
import warnings
import xarray as xr

from .utilities import compare_ncfiles, open_nwmdataset, __make_relative__
from .utilities import compare_ncfiles
from .job_tools import seconds, get_user
from .job import Job, Scheduler

#########################
# netcdf file object classes


class WrfHydroTs(list):
    def open(self, chunks: dict = None):
        """Open a WrfHydroTs object
        Args:
            self
            chunks: chunks argument passed on to xarray.DataFrame.chunk() method
        Returns:
            An xarray mfdataset object concatenated on dimension 'Time'.
        """
        return open_nwmdataset(self,chunks=chunks)


class WrfHydroStatic(pathlib.PosixPath):
    def open(self):
        """Open a WrfHydroStatic object
        Args:
            self
        Returns:
            An xarray dataset object.
        """
        return xr.open_dataset(self)


#########################
# Classes for constructing and running a wrf_hydro setup 
class WrfHydroModel(object):
    """Class for a WRF-Hydro model, which consitutes the model source code and compiled binary.
    """
    def __init__(self, source_dir: str,model_config: str):
        """Instantiate a WrfHydroModel object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/trunk/NDHMS'.
            new_compile_dir: Optional, new directory to to hold results
               of code compilation.
        Returns:
            A WrfHydroModel object.
        """
        # Instantiate all attributes and methods
        self.source_dir = None
        """pathlib.Path: pathlib.Path object for source code directory."""
        self.model_config = None
        """str: String indicating model configuration for compile options, must be one of 'NWM', 
        'Gridded', or 'Reach'."""
        self.hydro_namelists = None
        """dict: Master dictionary of all hydro.namelists stored with the source code."""
        self.hrldas_namelists = None
        """dict: Master dictionary of all namelist.hrldas stored with the source code."""
        self.compile_options = None
        """dict: Compile-time options. Defaults are loaded from json file stored with source
        code."""
        self.version = None
        """str: Source code version from .version file stored with the source code."""
        self.compile_dir = None
        """pathlib.Path: pathlib.Path object pointing to the compile directory."""
        self.compile_dir = None
        """pathlib.Path: pathlib.Path object pointing to the compile directory."""
        self.compiler = None
        """str: The compiler chosen at compile time."""
        self.configure_log = None
        """CompletedProcess: The subprocess object generated at configure."""
        self.compile_log = None
        """CompletedProcess: The subprocess object generated at compile."""
        self.object_id = None
        """str: A unique id to join object to compile directory."""
        self.table_files = None
        """list: pathlib.Paths to *.TBL files generated at compile-time."""
        self.wrf_hydro_exe = None
        """pathlib.Path: pathlib.Path to wrf_hydro.exe file generated at compile-time."""

        # Set attributes
        ## Setup directory paths
        self.source_dir = pathlib.Path(source_dir).absolute()

        ## Load master namelists
        self.hydro_namelists = \
            json.load(open(self.source_dir.joinpath('hydro_namelists.json')))

        self.hrldas_namelists = \
            json.load(open(self.source_dir.joinpath('hrldas_namelists.json')))

        ## Get code version
        with open(self.source_dir.joinpath('.version')) as f:
            self.version = f.read()

        ## Load compile options
        self.model_config = model_config
        compile_options = json.load(open(self.source_dir.joinpath('compile_options.json')))
        self.compile_options = compile_options[self.version][self.model_config]

    def compile(self, compiler: str,
                compile_dir: str = None,
                overwrite: bool = False,
                compile_options: dict = None) -> str:
        """Compiles WRF-Hydro using specified compiler and compile options.
        Args:
            compiler: The compiler to use, must be one of 'pgi','gfort',
                'ifort', or 'luna'.
            compile_dir: A non-existant directory to use for compilation.
            overwrite: Overwrite compile directory if exists.
            compile_options: Changes to default compile-time options.
        Returns:
            Success of compilation and compile directory used. Sets additional
            attributes to WrfHydroModel

        """

        # A bunch of ugly logic to check compile directory.
        if compile_dir is None:
            self.compile_dir = self.source_dir.joinpath('Run')
        else:
            self.compile_dir = pathlib.Path(compile_dir).absolute()
            if self.compile_dir.is_dir() is False:
                self.compile_dir.mkdir(parents=True)
            else:
                if self.compile_dir.is_dir() is True and overwrite is True:
                    shutil.rmtree(str(self.compile_dir))
                    self.compile_dir.mkdir()
                else:
                    raise IOError(str(self.compile_dir) + ' directory already exists')

        # Add compiler and compile options as attributes and update if needed
        self.compiler = compiler

        if compile_options is not None:
            self.compile_options.update(compile_options)

        # Get directroy for setEnvar
        compile_options_file = self.source_dir.joinpath('compile_options.sh')

        # Write setEnvar file
        with open(compile_options_file,'w') as file:
            for option, value in self.compile_options.items():
                file.write("export {}={}\n".format(option, value))

        # Compile
        self.configure_log = subprocess.run(['./configure', compiler],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            cwd=self.source_dir)

        self.compile_log = subprocess.run(['./compile_offline_NoahMP.sh',
                                           str(compile_options_file.absolute())],
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE,
                                          cwd=self.source_dir)
        # Change to back to previous working directory

        # Add in unique ID file to match this object to prevent assosciating
        # this directory with another object
        self.object_id = str(uuid.uuid4())

        with open(self.compile_dir.joinpath('.uid'),'w') as f:
            f.write(self.object_id)

        if self.compile_log.returncode == 0:
            # Open permissions on compiled files
            subprocess.run(['chmod','-R','777',str(self.source_dir.joinpath('Run'))])

            # Wrf hydro always puts files in source directory under a new directory called 'Run'
            # Copy files to new directory if its not the same as the source code directory
            if str(self.compile_dir.parent) != str(self.source_dir):
                for file in self.source_dir.joinpath('Run').glob('*.TBL'):
                    shutil.copyfile(file,str(self.compile_dir.joinpath(file.name)))

                shutil.copyfile(str(self.source_dir.joinpath('Run').joinpath('wrf_hydro.exe')),
                         str(self.compile_dir.joinpath('wrf_hydro.exe')))

                #Remove old files
                shutil.rmtree(self.source_dir.joinpath('Run'))

            # Open permissions on copied compiled files
            subprocess.run(['chmod', '-R', '777', str(self.compile_dir)])

            #Get file lists as attributes
            # Get list of table file paths
            self.table_files = list(self.compile_dir.glob('*.TBL'))

            # Get wrf_hydro.exe file path
            self.wrf_hydro_exe = self.compile_dir.joinpath('wrf_hydro.exe')

            # Save the object out to the compile directory
            with open(self.compile_dir.joinpath('WrfHydroModel.pkl'), 'wb') as f:
                pickle.dump(self, f, 2)

            print('Model successfully compiled into ' + str(self.compile_dir))
        else:
            print('Model did not successfully compile')

# WRF-Hydro Domain object
class WrfHydroDomain(object):
    """Class for a WRF-Hydro domain, which consitutes all domain-specific files needed for a
    setup.
    """
    def __init__(self,
                 domain_top_dir: str,
                 domain_config: str,
                 model_version: str,
                 namelist_patch_file: str = 'namelist_patches.json'):
        """Instantiate a WrfHydroDomain object
        Args:
            domain_top_dir: Parent directory containing all domain directories and files.
            domain_config: The domain configuration to use, options are 'NWM',
                'Gridded', or 'Reach'
            model_version: The WRF-Hydro model version
            namelist_patch_file: Filename of json file containing namelist patches
        Returns:
            A WrfHydroDomain directory object
        """

        ###Instantiate arguments to object
        # Make file paths
        self.domain_top_dir = pathlib.Path(domain_top_dir).absolute()
        """pathlib.Path: pathlib.Paths to *.TBL files generated at compile-time."""

        self.namelist_patch_file = self.domain_top_dir.joinpath(namelist_patch_file)
        """pathlib.Path: pathlib.Path to the namelist_patches json file."""

        # Load namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file, 'r'))
        """dict: Domain-specific namelist settings."""

        self.model_version = model_version
        """str: Specified source-code version for which the domain is to be used."""

        self.domain_config = domain_config
        """str: Specified configuration for which the domain is to be used, e.g. 'NWM'"""
        self.hydro_files = None
        """list: Files specified in hydro_nlist section of the domain namelist patches"""
        self.nudging_files = None
        """list: Files specified in nudging_nlist section of the domain namelist patches"""
        self.lsm_files = None
        """list: Files specified in noahlsm_offline section of the domain namelist patches"""
        ###

        # Create file paths from hydro namelist
        domain_hydro_nlist = self.namelist_patches[self.model_version][self.domain_config][
            'hydro_namelist']['hydro_nlist']

        self.hydro_files = []
        for key, value in domain_hydro_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix =='.nc':
                    self.hydro_files.append(WrfHydroStatic(file_path))
                else:
                    self.hydro_files.append(file_path)

        # Create file paths from nudging namelist
        domain_nudging_nlist = self.namelist_patches[self.model_version][self.domain_config
        ]['hydro_namelist']['nudging_nlist']

        self.nudging_files = []

        for key, value in domain_nudging_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix =='.nc':
                    self.nudging_files.append(WrfHydroStatic(file_path))
                else:
                    self.nudging_files.append(file_path)

        # Create symlinks from lsm namelist
        domain_lsm_nlist = \
            self.namelist_patches[self.model_version][self.domain_config]['namelist_hrldas'
            ]["noahlsm_offline"]

        self.lsm_files = []
        for key, value in domain_lsm_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))

            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.lsm_files.append(WrfHydroStatic(file_path))
                else:
                    self.lsm_files.append(file_path)

            if key == 'indir':
                self.forcing_dir = file_path

        self.forcing_data = WrfHydroTs(self.forcing_dir.glob('*'))


class WrfHydroSetup(object):
    """Class for a WRF-Hydro setup object, which is comprised of a WrfHydroModel and a WrfHydroDomain.
    """
    def __init__(self,
                 wrf_hydro_model: object,
                 wrf_hydro_domain: object):
        """Instantiates a WrfHydroSetup object
        Args:
            wrf_hydro_model: A WrfHydroModel object
            wrf_hydro_domain: A WrfHydroDomain object
        Returns:
            A WrfHydroSetup object
        """

        # Validate that hte domain and model are compatible
        if wrf_hydro_model.model_config != wrf_hydro_domain.domain_config:
            raise TypeError('Model configuration '+
                            wrf_hydro_model.model_config+
                            ' not compatible with domain configuration '+
                            wrf_hydro_domain.domain_config)
        if wrf_hydro_model.version not in list(wrf_hydro_domain.namelist_patches.keys()):
            raise TypeError('Model version '+
                            wrf_hydro_model.versions+
                            ' not compatible with domain versions '+
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
            copy.deepcopy(self.model.hrldas_namelists[self.model.version][self.domain.domain_config])
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




class WrfHydroRun(object):
    def __init__(
        self,
        wrf_hydro_setup: WrfHydroSetup,
        run_dir: str,
        rm_existing_run_dir = False,
        job: Job=None
    ):
        """Instantiate a WrfHydroRun object. A run is a WrfHydroSetup with multiple jobs.
        Args:
            wrf_hydro_setup: A setup object. 
            run_dir: str, where to execute the job. This is an attribute of the Run object.
            job: Optional, Job object 
        Returns:
            A WrfHydroRun object.
        """
        # TODO(JLM): Accept a list of Jobs in the job argument?

        # Initialize all attributes and methods

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
        self.channel_rt = None
        """WrfHydroTs: Timeseries dataset of CHRTOUT files"""
        self.chanobs = None
        """WrfHydroTs: Timeseries dataset of CHANOBS files"""
        self.lakeout = None
        """WrfHydroTs: Timeseries dataset of LAKEOUT files"""
        self.gwout = None
        """WrfHydroTs: Timeseries dataset of GWOUT files"""
        self.restart_hydro = None
        """list: List of HYDRO_RST WrfHydroStatic objects"""
        self.restart_lsm = None
        """list: List of RESTART WrfHydroStatic objects"""
        self.restart_nudging = None
        """list: List of nudgingLastObs WrfHydroStatic objects"""

        self.object_id = None
        """str: A unique id to join object to run directory."""

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
        if self.run_dir.is_dir() and not rm_existing_run_dir:
            raise ValueError("Run directory already exists and rm_existing_run_dir is False.")

        if self.run_dir.exists():
            shutil.rmtree(str(self.run_dir))
        self.run_dir.mkdir(parents=True)

        # Check that compile object uid matches compile directory uid
        # This is to ensure that a new model has not been compiled into that directory unknowingly
        with open(self.setup.model.compile_dir.joinpath('.uid')) as f:
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

        # write hydro.namelist
        f90nml.write(self.setup.hydro_namelist,
                     self.run_dir.joinpath('hydro.namelist'))
        # write namelist.hrldas
        f90nml.write(self.setup.namelist_hrldas,
                     self.run_dir.joinpath('namelist.hrldas'))

        if job: 
            self.add_job(job)


    def add_job(
        self,
        job: Job,
        model_start_time: str=None,
        model_end_time: str=None,
        model_restart: bool=True
    ):
        """Dispatch a run the wrf_hydro setup: either run() or schedule_run()
        If a scheduler is passed, then that run is scheduled. 
        Args:
            As for run and schedule_run().
        Returns:
            A WrfHydroRun object
        """

        # Attempt to add the job
        if job.scheduler:

            # A scheduled job can be appended to the jobs.pending list if
            # 1) there are no active or pending jobs
            # 2) if it is (made) dependent on the last active or pending job.

            # Get the job id of the last active or pending job.
            last_job_id = None
            if self.job_active:
                last_job_id = self.job_active.jobID
            if len(self.jobs_pending):
                last_job_id = self.jobs_pending[-1].jobID

            # Check the dependency on a previous job
            if last_job_id is not None:
                if job.scheduler.afterok is not None and job.scheduler.afterok != last_job_id:
                    raise ValueError("The job's specified dependency/afterok conflicts with reality.")
                job.scheduler.afterok = last_job_id
            else: 
                if job.scheduler.afterok is not None:
                    raise ValueError("The job's specified dependency/afterok conflicts with reality.")

            # Add the job later
            
        else:

            # an interactive job can be made the active job if there is no current or pending job.
            if self.job_active or len(self.jobs_pending):
                raise ValueError("Interactive jobs cannot be added when other jobs are" +
                                 " active or pending.")

            # Add the job later

        # If a job is successfully added you make it here... 

        # Set submission-time job variables here.
        job.user = get_user()
        job_submission_time = datetime.datetime.now()
        job.job_submission_time = str(job_submission_time)
        job.job_date_id = '{date:%Y-%m-%d-%H-%M-%S-%f}'.format(date=job_submission_time)

        # TODO(JLM): 
        # Edit the namelists with model start/end times and if restarting.
        # Stash the namelists in the job.
        # This begs for consistency check across jobs: start previous job = end current job

        # Add a deepcopy of the job to the run object.
        if job.scheduler:
            self.jobs_pending.append(copy.deepcopy(job))
        else:
            self.job_active = copy.deepcopy(job)

        # Archive the run object with the added job to disk/pickle.
        self.pickle()

        # TODO(JLM): 
        # self.pickle() -- # TODO(JLM):????? What happens to the state of the files??
        # can this pickle just update the jobs?
        # can jobs be added to runs with active jobs?
        # maybe all jobs have to be established first?
        # how to avoid clashes accessing the object between the run and any
        # modifications of the run? have a picke lock file?

        # Run/submit the job.
        if job.scheduler:

            # The self argument is the run object, the job object already has itself.
            self.jobs_pending[-1].schedule(self.run_dir)

            # Taking this out for complexity sakes... but leaving code here until
            # we make a firm decision about pursuing it.
            # #optional: monitor the job and self.unpickle
            # # TODO JLM: seems lke this wait can be abstracted to function. >>>
            # if wait_for_complete:

            #     #print( the scheduler name?
            #     print("Waiting for scheduled job " +
            #           str(run_object.scheduler.sched_job_id) +
            #           " to complete. /n" +
            #           "(d=dependent, q=queued, r=running : 1/" +
            #           str(monitor_freq_s) +"seconds):", end = "", flush=True)

            #     while not run_object.scheduler.job_complete:
            #         sleep(monitor_freq_s)
            #         if not os.path.isfile(run_dir + '/.job_not_complete'):
            #             sym = 'r'
            #         else:
            #             sym = 'q'
            #             ## TODO JLM: add "d" to indicate waiting for dependency.
            #             print(sym, end="", flush=True)
                        
            #         print('')
            #         # TODO JLM: seems like this wait can be abstracted to function. <<<

            #     # Part of the wait_for_complete if statement, this updates the
            #     # TODO(JLM): I DONT THINK THIS WORKS. IS THIS A CASE FOR SELF.__DICT__.UPDATE()?
            #     self = run_object.unpickle()

            # TODO(JLM): When will this job be moved to the active list and the completed list?

        else:

            self.job_active.run(self.run_dir)
            self.collect_output()
            self.jobs_completed.append(self.job_active)
            self.job_active = None


    def collect_output(self):

        if self.job_active.exit_status != 0:
            warnings.warn('Model run failed.')
            return(None)

        print('Model run succeeded.')
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

        #Get Lakeout files
        if len(list(self.run_dir.glob('*LAKEOUT*'))) > 0:
            self.lakeout = WrfHydroTs(list(self.run_dir.glob('*LAKEOUT*')))

        #Get gwout files
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
        with open(self.run_dir.joinpath('.uid'), 'w') as f:
            f.write(self.object_id)

        # Save object to run directory
        # Save the object out to the compile directory
        with open(self.run_dir.joinpath('WrfHydroRun.pkl'), 'wb') as f:
            pickle.dump(self, f, 2)


    def unpickle(self):
        # Load run object from run directory after a scheduler job
        with open(self.run_dir.joinpath('WrfHydroRun.pkl'), 'rb') as f:
            return(pickle.load(f))


    def make_relative(self,basepath = None):
        """Make all file paths relative to a given directory, useful for opening file
        attributes in a run object after it has been moved or copied to a new directory or
        system.
        Args:
            basepath: The base path to use for relative paths. Defaults to run directory.
            This rarely needs to be defined.
        Returns:
            self with relative files paths for file-like attributes
        """
        __make_relative__(run_object=self,basepath=basepath)

class DomainDirectory(object):
    """An object that represents a WRF-Hydro domain directory. Primarily used as a utility class
       for WrfHydroDomain"""
    def __init__(self,
                 domain_top_dir: str,
                 domain_config: str,
                 model_version: str,
                 namelist_patch_file: str = 'namelist_patches.json'):
        """Create a run directory of symlinks using the domain namelist patches
        Args:
            domain_top_dir: Parent directory containing all domain directories and files.
            domain_config: The domain configuration to use, options are 'NWM',
                'Gridded', or 'Reach'
            model_version: The WRF-Hydro model version
            namelist_patch_file: Filename of json file containing namelist patches
        Returns:
            A DomainDirectory directory object
        """

        ###Instantiate arguments to object
        # Make file paths
        self.domain_top_dir = Path(domain_top_dir)
        self.namelist_patch_file = self.domain_top_dir.joinpath(namelist_patch_file)

        # Load namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file, 'r'))

        self.model_version = model_version
        self.domain_config = domain_config
        ###

        # Create file paths from hydro namelist
        domain_hydro_nlist = self.namelist_patches[self.model_version][self.domain_config][
            'hydro_namelist']['hydro_nlist']

        self.hydro_files = []
        for key, value in domain_hydro_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.suffix =='.nc':
                self.hydro_files.append(WrfHydroStatic(file_path))
            else:
                self.hydro_files.append(file_path)

        # Create file paths from nudging namelist
        domain_nudging_nlist = self.namelist_patches[self.model_version][self.domain_config
        ]['hydro_namelist']['nudging_nlist']

        self.nudging_files = []
        for key, value in domain_nudging_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.suffix =='.nc':
                self.nudging_files.append(WrfHydroStatic(file_path))
            else:
                self.nudging_files.append(file_path)

        # Create symlinks from lsm namelist
        domain_lsm_nlist = \
            self.namelist_patches[self.model_version][self.domain_config]['namelist_hrldas'
            ]["noahlsm_offline"]

        self.lsm_files = []
        for key, value in domain_lsm_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))

            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.lsm_files.append(WrfHydroStatic(file_path))
                else:
                    self.lsm_files.append(file_path)

            if key == 'indir':
                self.forcing = file_path

####Classes
class RestartDiffs(object):
    def __init__(self,
                 candidate_run: WrfHydroRun,
                 reference_run: WrfHydroRun,
                 nccmp_options: list = ['--data','--metadata', '--force', '--quiet'],
                 exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                       'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt']):
        """Calculate Diffs between restart objects for two WrfHydroRun objects
        Args:
            candidate_run: The candidate WrfHydroRun object
            reference_run: The reference WrfHydroRun object
            nccmp_options: List of long-form command line options passed to nccmp,
            see http://nccmp.sourceforge.net/ for options
            exclude_vars: A list of strings containing variables names to
            exclude from the comparison
        Returns:
            A DomainDirectory directory object
        """
        # Instantiate all attributes
        self.diff_counts = None
        """dict: Counts of diffs by restart type"""
        self.hydro = None
        """list: List of pandas dataframes if possible or subprocess objects containing hydro 
        restart file diffs"""
        self.lsm = None
        """list: List of pandas dataframes if possible or subprocess objects containing lsm restart 
        file diffs"""
        self.nudging = None
        """list: List of pandas dataframes if possible or subprocess objects containing nudging 
        restart file diffs"""

        #Add a dictionary with counts of diffs
        self.diff_counts = {}

        if len(candidate_run.restart_hydro) != 0 and len(reference_run.restart_hydro) != 0:
            self.hydro = compare_ncfiles(candidate_files=candidate_run.restart_hydro,
                                         reference_files=reference_run.restart_hydro,
                                         nccmp_options = nccmp_options,
                                         exclude_vars = exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.hydro))
            self.diff_counts.update({'hydro':diff_counts})
        else:
            warnings.warn('length of candidate_sim.restart_hydro or reference_sim.restart_hydro '
                          'is 0')

        if len(candidate_run.restart_lsm) != 0 and len(reference_run.restart_lsm) != 0:
            self.lsm = compare_ncfiles(candidate_files=candidate_run.restart_lsm,
                                       reference_files=reference_run.restart_lsm,
                                       nccmp_options = nccmp_options,
                                       exclude_vars = exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.lsm))
            self.diff_counts.update({'lsm':diff_counts})
        else:
            warnings.warn('length of candidate_sim.restart_lsm or reference_sim.restart_lsm is 0')

        if len(candidate_run.restart_nudging) != 0 and len(reference_run.restart_nudging) != 0:
            self.nudging = compare_ncfiles(
                candidate_files=candidate_run.restart_nudging,
                reference_files=reference_run.restart_nudging,
                nccmp_options = nccmp_options,
                exclude_vars = exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.nudging))
            self.diff_counts.update({'nudging':diff_counts})
        else:
            warnings.warn('length of candidate_sim.restart_nudging or '
                          'reference_sim.restart_nudging is 0')
