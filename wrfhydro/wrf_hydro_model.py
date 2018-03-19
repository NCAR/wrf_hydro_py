import subprocess
from pathlib import Path, PosixPath
from shutil import copyfile, rmtree
import xarray as xr
import f90nml
import json
from copy import deepcopy
from os import chdir
from uuid import uuid4
import pickle
from warnings import warn
#########################
# netcdf file object classes

class WrfHydroTs(list):
    def open(self):
        """Open a WrfHydroTs object
        Args:
            self
        Returns:
            An xarray mfdataset object concatenated on dimension 'Time'.
        """
        return(xr.open_mfdataset(self, concat_dim='Time'))

class WrfHydroStatic(PosixPath):
    def open(self):
        """Open a WrfHydroStatic object
        Args:
            self
        Returns:
            An xarray dataset object.
        """
        return (xr.open_dataset(self))


#########################
# Classes for constructing and running a wrf_hydro simulation
class WrfHydroModel(object):
    """Class for a WRF-Hydro model, which consitutes the model source code and compiled binary.
    """
    def __init__(self, source_dir: str):
        """Instantiate a WrfHydroModel object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/trunk/NDHMS'.
            new_compile_dir: Optional, new directory to to hold results
               of code compilation.
        Returns:
            A WrfHydroModel object.
        """

        # Setup directory paths
        self.source_dir = Path(source_dir)
        """Path: Path object for source code directory."""
        # Load master namelists
        self.hydro_namelists = \
            json.load(open(self.source_dir.joinpath('hydro_namelists.json')))
        """dict: Master dictionary of all hydro.namelists stored with the source code."""
        self.hrldas_namelists = \
            json.load(open(self.source_dir.joinpath('hrldas_namelists.json')))
        """dict: Master dictionary of all namelist.hrldas stored with the source code."""

        # Load compile options
        self.compile_options = json.load(open(self.source_dir.joinpath('compile_options.json')))
        """dict: Compile-time options. Defaults are loaded from json file stored with source 
        code."""

        # Get code version
        with open(self.source_dir.joinpath('.version')) as f:
            self.version = f.read()
        """str: Source code version from .version file stored with the source code."""

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
            compile_options: Changes to default compile-time options. Defaults
                are {'WRF_HYDRO':1, 'HYDRO_D':1, 'SPATIAL_SOIL':1,
                     'WRF_HYDRO_RAPID':0, 'WRFIO_NCD_LARGE_FILE_SUPPORT':1,
                     'NCEP_WCOSS':1, 'WRF_HYDRO_NUDGING':0 }
        Returns:
            Success of compilation and compile directory used. Sets additional
            attributes to WrfHydroModel

        """

        # A bunch of ugly logic to check compile directory.
        if compile_dir is None:
            self.compile_dir = self.source_dir.joinpath('Run')
            """Path: Path object pointing to the compile directory."""
        else:
            self.compile_dir = Path(compile_dir)
            """Path: Path object pointing to the compile directory."""
            if self.compile_dir.is_dir() is False:
                self.compile_dir.mkdir(parents=True)
            else:
                if self.compile_dir.is_dir() is True and overwrite is True:
                    rmtree(str(self.compile_dir))
                    self.compile_dir.mkdir()
                else:
                    raise IOError(str(self.compile_dir) + ' directory already exists')

        # Add compiler and compile options as attributes and update if needed
        self.compiler = compiler
        """str: The compiler chosen at compile time."""

        if compile_options is not None:
            self.compile_options.update(compile_options)

        # Get directroy for setEnvar
        compile_options_file = self.source_dir.joinpath('compile_options.sh')

        # Write setEnvar file
        with open(compile_options_file,'w') as file:
            for option, value in self.compile_options.items():
                file.write("export {}={}\n".format(option, value))

        # Compile
        # Change to source code directory for compile time
        chdir(self.source_dir)
        self.configure_log = subprocess.run(['./configure', compiler],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
        """CompletedProcess: The subprocess object generated at configure."""

        self.compile_log = subprocess.run(['./compile_offline_NoahMP.sh',
                                           str(compile_options_file)],
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
        """CompletedProcess: The subprocess object generated at compile."""

        # Add in unique ID file to match this object to prevent assosciating
        # this directory with another object
        self.object_id = str(uuid4())
        """str: A unique id to join object to compile directory."""

        with open(self.compile_dir.joinpath('.uid'),'w') as f:
            f.write(self.object_id)

        if self.compile_log.returncode == 0:
            # Open permissions on compiled files
            subprocess.run(['chmod','-R','777',str(self.source_dir.joinpath('Run'))])

            # Wrf hydro always puts files in source directory under a new directory called 'Run'
            # Copy files to new directory if its not the same as the source code directory
            if str(self.compile_dir.parent) != str(self.source_dir):
                for file in self.source_dir.joinpath('Run').glob('*.TBL'):
                    copyfile(file,str(self.compile_dir.joinpath(file.name)))

                copyfile(str(self.source_dir.joinpath('Run').joinpath('wrf_hydro.exe')),
                         str(self.compile_dir.joinpath('wrf_hydro.exe')))

                #Remove old files
                rmtree(self.source_dir.joinpath('Run'))

            # Open permissions on copied compiled files
            subprocess.run(['chmod', '-R', '777', str(self.compile_dir)])

            #Get file lists as attributes
            # Get list of table file paths
            self.table_files = list(self.compile_dir.glob('*.TBL'))
            """list: Paths to *.TBL files generated at compile-time."""

            # Get wrf_hydro.exe file path
            self.wrf_hydro_exe = self.compile_dir.joinpath('wrf_hydro.exe')
            """Path: Path to wrf_hydro.exe file generated at compile-time."""

            # Save the object out to the compile directory
            with open(self.compile_dir.joinpath('WrfHydroModel.pkl'), 'wb') as f:
                pickle.dump(self, f, 2)

            print('Model successfully compiled into ' + str(self.compile_dir))
        else:
            print('Model did not successfully compile')

# WRF-Hydro Domain object
class WrfHydroDomain(object):
    """Class for a WRF-Hydro domain, which consitutes all domain-specific files needed for a
    simulation.
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
        self.domain_top_dir = Path(domain_top_dir)
        """Path: Paths to *.TBL files generated at compile-time."""

        self.namelist_patch_file = self.domain_top_dir.joinpath(namelist_patch_file)
        """Path: Path to the namelist_patches json file."""

        # Load namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file, 'r'))
        """dict: Domain-specific namelist settings."""

        self.model_version = model_version
        """str: Specified source-code version for which the domain is to be used."""

        self.domain_config = domain_config
        """str: Specified configuration for which the domain is to be used, e.g. 'NWM'"""
        ###

        # Create file paths from hydro namelist
        domain_hydro_nlist = self.namelist_patches[self.model_version][self.domain_config][
            'hydro_namelist']['hydro_nlist']

        self.hydro_files = []
        """list: Files specified in hydro_nlist section of the domain namelist patches"""
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
        """list: Files specified in nudging_nlist section of the domain namelist patches"""

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
        """list: Files specified in noahlsm_offline section of the domain namelist patches"""
        for key, value in domain_lsm_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))

            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.lsm_files.append(WrfHydroStatic(file_path))
                else:
                    self.lsm_files.append(file_path)

            if key == 'indir':
                self.forcing_dir = file_path



class WrfHydroSim(object):
    """Class for a WRF-Hydro simulation, which is comprised of a WrfHydroModel and a WrfHydroDomain.
    """
    def __init__(self, wrf_hydro_model: object,
                 wrf_hydro_domain: object):
        """Instantiates a WrfHydroSim object
        Args:
            wrf_hydro_model: A WrfHydroModel object
            wrf_hydro_domain: A WrfHydroDomain object
        Returns:
            A WrfHydroSim object
        """
        # assign objects to self
        self.model = deepcopy(wrf_hydro_model)
        """WrfHydroModel: A copy of the WrfHydroModel object used for the simulation"""

        self.domain = deepcopy(wrf_hydro_domain)
        """WrfHydroDomain: A copy of the WrfHydroDomain object used for the simulation"""

        # Create namelists
        self.hydro_namelist = \
            deepcopy(self.model.hydro_namelists[self.model.version][self.domain.domain_config])
        """dict: A copy of the hydro_namelist used by the WrfHydroModel for the specified model 
        version and domain configuration"""

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

        self.namelist_hrldas = \
            deepcopy(self.model.hrldas_namelists[self.model.version][self.domain.domain_config])
        """dict: A copy of the hrldas_namelist used by the WrfHydroModel for the specified model 
        version and domain configuration"""

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

    def run(self,
            simulation_dir: str,
            num_cores: int = 2,
            mode: str = 'r') -> object:
        """Run the wrf_hydro simulation
        Args:
            simulation_dir: The path to the directory to use for run
            num_cores: Optional, the number of cores to using default run_command
            mode: Write mode, 'w' for overwrite if directory exists, and 'r' for fail if
            directory exists
        Returns:
            A model run object
        TODO:
            Add option for custom run commands to deal with job schedulers
        """
        #Make copy of simulation object to alter and return
        simulation = deepcopy(self)
        run_object = WrfHydroRun(wrf_hydro_simulation=simulation,
                                 simulation_dir=simulation_dir,
                                 num_cores=num_cores,
                                 mode=mode)
        print('Model run succeeded')
        return run_object


class WrfHydroRun(object):
    def __init__(self,
                 wrf_hydro_simulation: WrfHydroSim,
                 simulation_dir: str,
                 num_cores: int = 2,
                 mode: str = 'r'
                 ):
        """Instantiate a WrfHydroRun object, including running the simulation
        Args:
            wrf_hydro_simulation: A WrfHydroSim object to run
            simulation_dir: The path to the directory to use for run
            num_cores: Optional, the number of cores to using default run_command
            mode: Write mode, 'w' for overwrite if directory exists, and 'r' for fail if
            directory exists
        Returns:
            A WrfHydroRun object
        TODO:
            Add option for custom run commands to deal with job schedulers
        """
        self.simulation = wrf_hydro_simulation
        """WrfHydroSim: The WrfHydroSim object used for the run"""

        # add num cores as attribute
        self.num_cores = num_cores
        """int: The number of cores used for the run"""

        # Add sim dir
        self.simulation_dir = Path(simulation_dir)
        """Path: Path to the directory used for the run"""

        # Make directory if it does not exists
        if self.simulation_dir.is_dir() is False:
            self.simulation_dir.mkdir(parents=True)
        else:
            if self.simulation_dir.is_dir() is True and mode == 'w':
                rmtree(str(self.simulation_dir))
                self.simulation_dir.mkdir(parents=True)
            elif self.simulation_dir.is_dir() is True and mode == 'r':
                raise PermissionError('Run directory already exists and mode = r')
            else:
                warn('Existing run directory will be used for simulation')

        ### Check that compile object uid matches compile directory uid
        ### This is to ensure that a new model has not been compiled into that directory unknowingly
        with open(self.model.compile_dir.joinpath('.uid')) as f:
            compile_uid = f.read()

        if self.model.object_id != compile_uid:
            raise PermissionError('object id mismatch between WrfHydroModel object and'
                                  'WrfHydroModel.compile_dir directory. Directory may have been'
                                  'used for another compile')
        ###########################################################################
        # MAKE RUN DIRECTORIES
        # Construct all file/dir paths
        # Convert strings to Path objects

        # Loop to make symlinks for each TBL file
        for from_file in self.model.table_files:
            # Create file paths to symlink
            to_file = self.simulation_dir.joinpath(from_file.name)
            # Create symlinks
            to_file.symlink_to(from_file)

        # Symlink in exe
        wrf_hydro_exe = self.model.wrf_hydro_exe
        self.simulation_dir.joinpath(wrf_hydro_exe.name).symlink_to(wrf_hydro_exe)

        # Symlink in forcing
        forcing_dir = self.domain.forcing_dir
        self.simulation_dir.joinpath(forcing_dir.name). \
            symlink_to(forcing_dir, target_is_directory=True)

        # create DOMAIN directory and symlink in files
        # Symlink in hydro_files
        for file_path in self.domain.hydro_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = file_path.relative_to(self.domain.domain_top_dir)
            symlink_path = self.simulation_dir.joinpath(relative_path)
            if symlink_path.parent.is_dir() is False:
                symlink_path.parent.mkdir(parents=True)
            symlink_path.symlink_to(file_path)

        # Symlink in nudging files
        for file_path in self.domain.nudging_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = file_path.relative_to(self.domain.domain_top_dir)
            symlink_path = self.simulation_dir.joinpath(relative_path)
            if symlink_path.parent.is_dir() is False:
                symlink_path.parent.mkdir(parents=True)
            symlink_path.symlink_to(file_path)

        # Symlink in lsm files
        for file_path in self.domain.lsm_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = file_path.relative_to(self.domain.domain_top_dir)
            symlink_path = self.simulation_dir.joinpath(relative_path)
            if symlink_path.parent.is_dir() is False:
                symlink_path.parent.mkdir(parents=True)
            symlink_path.symlink_to(file_path)

        # write hydro.namelist
        f90nml.write(self.hydro_namelist,
                     self.simulation_dir.joinpath('hydro.namelist'))
        # write namelist.hrldas
        f90nml.write(self.namelist_hrldas,
                     self.simulation_dir.joinpath('namelist.hrldas'))

        # Run the model
        chdir(self.simulation_dir)
        self.run_log = subprocess.run(['mpiexec', '-np', str(num_cores), './wrf_hydro.exe'],
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
        """CompletedProcess: The subprocess returned from the run call"""


        try:
            self.run_status = 1
            """int: exit status of the run"""
            # String match diag files for successfull run
            with open(self.simulation_dir.joinpath('diag_hydro.00000')) as f:
                diag_file = f.read()
                if 'The model finished successfully.......' in diag_file:
                    self.run_status = 0
        except Exception as e:
            warn('Could not parse diag files')
            warn(print(e))

        if self.run_status == 0:

            #####################
            # Grab outputs as WrfHydroXX classes of file paths

            # TODO TJM - Make all files fall under an 'output_files' attirbute

            ## Get diag files
            self.diag = list(self.simulation_dir.glob('diag_hydro.*'))
            """list: Paths to diag files generated at run time"""

            ## Get channel files
            if len(list(self.simulation_dir.glob('*CHRTOUT*'))) > 0:
                self.channel_rt = WrfHydroTs(list(self.simulation_dir.glob('*CHRTOUT*')))
                """WrfHydroTs: Timeseries dataset of CHRTOUT files"""

            if len(list(self.simulation_dir.glob('*CHANOBS*'))) > 0:
                self.chanobs = WrfHydroTs(list(self.simulation_dir.glob('*CHANOBS*')))
                """WrfHydroTs: Timeseries dataset of CHANOBS files"""

            ## Get restart files and sort by modified time
            ### Hydro restarts
            self.restart_hydro = []
            """list: List of HYDRO_RST WrfHydroStatic objects"""
            for file in self.simulation_dir.glob('HYDRO_RST*'):
                file = WrfHydroStatic(file)
                self.restart_hydro.append(file)

            if len(self.restart_hydro) > 0:
                self.restart_hydro = sorted(self.restart_hydro,
                                            key=lambda file: file.stat().st_mtime_ns)

            ### LSM Restarts
            self.restart_lsm = []
            """list: List of RESTART WrfHydroStatic objects"""
            for file in self.simulation_dir.glob('RESTART*'):
                file = WrfHydroStatic(file)
                self.restart_lsm.append(file)

            if len(self.restart_lsm) > 0:
                self.restart_lsm = sorted(self.restart_lsm,
                                          key=lambda file: file.stat().st_mtime_ns)

            ### Nudging restarts
            self.restart_nudging = []
            """list: List of nudgingLastObs WrfHydroStatic objects"""
            for file in self.simulation_dir.glob('nudgingLastObs*'):
                file = WrfHydroStatic(file)
                self.restart_nudging.append(file)

            if len(self.restart_nudging) > 0:
                self.restart_nudging = sorted(self.restart_nudging,
                                              key=lambda file: file.stat().st_mtime_ns)

            #####################

            # create a UID for the simulation and save in file
            self.object_id = str(uuid4())
            """str: A unique id to join object to run directory."""
            with open(self.simulation_dir.joinpath('.uid'), 'w') as f:
                f.write(self.object_id)

            # Save object to simulation directory
            # Save the object out to the compile directory
            with open(self.simulation_dir.joinpath('wrf_hydro_sim.pkl'), 'wb') as f:
                pickle.dump(self, f, 2)

            print('Model run succeeded')
        else:
            warn('Model run failed')
