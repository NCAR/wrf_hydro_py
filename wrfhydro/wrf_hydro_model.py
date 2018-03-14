import subprocess
from pathlib import Path
from shutil import copyfile, rmtree
import xarray as xr
import f90nml
import json
from copy import copy
from os import chdir
from uuid import uuid4
import pickle

#########################
# File object classes
# TODO: JLM: what is a "file object"


class wrf_hydro_ts(list):
    def open(self):
        """Open a wrf_hydro_ts object
        Args:
            self
        Returns:
            An xarray mfdataset object concatenated on dimension 'Time'.
        """
        return(xr.open_mfdataset(self, concat_dim='Time'))

    
class wrf_hydro_data(list):
    def open(self):
        """Open a wrf_hydro_data object
        Args:
            self
        Returns:
            An xarray dataset object.
        """
        return (xr.open_dataset(self))

    
#########################
# Classes for constructing and running a wrf_hydro simulation


class wrf_hydro_model(object):
    """The beginning of wrf_hydro python api
    Attributes:

    """
    def __init__(self, source_dir: str):
        """Create a wrf_hydro_model object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/trunk/NDHMS'.
            new_compile_dir: Optional, new directory to to hold results
               of code compilation.
        Returns:
            A wrf_hydro_model object.
        """

        # Setup directory paths
        self.source_dir = Path(source_dir)

        # Load master namelists
        self.hydro_namelists = \
            json.load(open(self.source_dir.joinpath('hydro_namelists.json')))
        self.hrldas_namelists = \
            json.load(open(self.source_dir.joinpath('hrldas_namelists.json')))

        # Get code version
        with open(self.source_dir.joinpath('.version')) as f:
            self.version = f.read()

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
            attributes to wrf_hydro_model

        """
        # TODO: JLM: I think compile_options should be mutually exclusive with
        #            (version, configuration) where these specfy compile options in
        #            a JSON file.
        
        # A bunch of ugly logic to check compile directory.
        if compile_dir is None:
            self.compile_dir = self.source_dir.joinpath('Run')
        else:
            self.compile_dir = Path(compile_dir)
            if self.compile_dir.is_dir() is False:
                self.compile_dir.mkdir(parents=True)
            else:
                if self.compile_dir.is_dir() is True and overwrite is True:
                    rmtree(str(self.compile_dir))
                    self.compile_dir.mkdir()
                else:
                    raise IOError(str(self.compile_dir) + ' directory already exists')

        # Add in unique ID file to match this object to prevent assosciating
        # this directory with another object
        self.object_id = str(uuid4())
        with open(self.compile_dir.joinpath('.uid'),'w') as f:
            f.write(self.object_id)

        # THIS MAY NO LONGER BE NECESSARY WITH DIRECTORY CREATION MOVED TO COMPILE TIME
        # Check to make sure the uuid file in the directory matches this object to prevent
        # one object from compiling into a directory assosciated with a different object
        # with open(self.compile_dir.joinpath('.uid')) as f:
        #     file_uid = f.read()
        # if self.object_id != file_uid:
        #     raise PermissionError('Compile directory owned by another model object. ' +
        #                           'Object id in file .uid does not match self.object_id')

        # Add compiler and compile options as attributes and update if needed
        self.compiler = compiler
        self.compile_options = {'WRF_HYDRO':1, 'HYDRO_D':1, 'SPATIAL_SOIL':1,
                                'WRF_HYDRO_RAPID':0, 'WRFIO_NCD_LARGE_FILE_SUPPORT':1,
                                'NCEP_WCOSS':1, 'WRF_HYDRO_NUDGING':0}
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
        subprocess.run(['./configure', compiler])
        subprocess.run(['./compile_offline_NoahMP.sh', str(compile_options_file)])

        # Open permissions on compiled files
        subprocess.run(['chmod','-R','777',str(self.source_dir.joinpath('Run'))])

        # Wrf hydro always puts files in source directory under a new directory called 'Run'
        # Copy files to new directory if its not the same as the source code directory
        if self.compile_dir.parent is not self.source_dir:
            for file in self.source_dir.joinpath('Run').glob('*.TBL'):
                copyfile(file,str(self.compile_dir.joinpath(file.name)))

            copyfile(str(self.source_dir.joinpath('Run').joinpath('wrf_hydro.exe')),
                     str(self.compile_dir.joinpath('wrf_hydro.exe')))

            #Remove old files
            rmtree(self.source_dir.joinpath('Run'))

        # Open permissions on copied compiled files
        subprocess.run(['chmod', '-R', '777', str(self.compile_dir)])

        # Save the object out to the compile directory
        with open(self.compile_dir.joinpath('wrf_hydro_model.pkl'), 'wb') as f:
            pickle.dump(self, f, 2)

        return('Model successfully compiled into ' + str(self.compile_dir))

    # Define a reset method
    def reset(self,confirm: str):
        """Deletes the entire contents of the compile directory and resets object to 
           pre-compile state
        Args:
            confirm: String of 'y' to confirm reset, or other to abort
        Returns:
            String indicating success of cleanup and reset of all compile-time attributes
        """
        if confirm is 'y':
            rmtree(str(self.compile_dir))
            atts_to_delete = ['compile_dir', 'object_id', 'compile_options', 'compiler']
            for att in atts_to_delete:
                self.__delattr__(att)
            return('Compile directory deleted and wrf_hydro_model object returned ' +
                   'to pre-compile state.')
        else:
            return("Confirm argument must be 'y' to proceed with reset.")

        
# WRF-Hydro Domain object
class wrf_hydro_domain(object):
    def __init__(self,domain_top_dir: str,
                 domain_config: str,
                 namelist_patch_file: str = 'namelist_patches.json',
                 forcing_dir: str = 'FORCING',
                 domain_dir: str = 'DOMAIN',
                 restart_dir: str = 'RESTART'):
        """Create a wrf_hydro_domain object.
        Args:
            domain_top_dir: Parent directory containing all domain directories and files. 
                All files and folders are
            relative to this directory
            domain_config: The domain configuration to use, options are 'NWM',
                'Gridded', or 'Reach'
            namelist_patch_file: Filename of json file containing namelist patches
            forcing_dir: Directory containing forcing data
            domain_dir: Directory containing domain files
            restart_dir: Directory containing restart files
        Returns:
            A wrf_hydro_domain object
        """

        # Set directory and file paths
        self.domain_top_dir = Path(domain_top_dir)
        self.domain_config = domain_config
        self.namelist_patch_file = self.domain_top_dir.joinpath(namelist_patch_file)
        self.forcing_dir = self.domain_top_dir.joinpath(forcing_dir)
        self.domain_dir = self.domain_top_dir.joinpath(domain_dir)
        self.restart_dir = self.domain_top_dir.joinpath(restart_dir)

        #######################
        # Validate inputs
        if self.domain_top_dir.is_dir() is False:
            raise IOError(str(self.domain_top_dir) + ' is not a directory')
        if self.forcing_dir.is_dir() is False:
            raise IOError(str(self.forcing_dir) + ' directory not found in ' +
                          str(self.domain_top_dir))
        if self.domain_dir.is_dir() is False:
            raise IOError(str(self.domain_dir) + ' directory not found in ' +
                          str(self.domain_top_dir))
        if self.restart_dir.is_dir() is False:
            raise IOError(str(self.restart_dir) + ' directory not found in ' +
                          str(self.domain_top_dir))
        if self.namelist_patch_file.is_file() is False:
            raise IOError(str(self.namelist_patch_file) + ' file not found in ' +
                          str(self.domain_top_dir))
        #######################

        # Setup file attributes
        # namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file))

        # forcing files
        self.forcing_files = wrf_hydro_ts(list(self.forcing_dir.glob('*')))
        # TODO TJM - handle non-forcing files in forcing dir?

        # restart files
        self.restart_files = wrf_hydro_ts(list(self.restart_dir.glob('*')))

        # TODO TJM - add in a search function to grab the proper routelnk
        # TODO TJM - this might need to belong in the wrf_hydro_sim since routelink
        #            needs to be pulled by model version number
        # self.route_link = self.namelist_patch_file


class wrf_hydro_simulation(object):
    def __init__(self, wrf_hydro_model: object,
                 wrf_hydro_domain: object,
                 domain_model_version = None):
        """Create a wrf_hydro_simulation object
        Args:
            wrf_hydro_model: A wrf_hydro_model object
            wrf_hydro_domain: A wrf_hydro_domain object
        Returns:
            A wrf_hydro_simulation object
        """
        # assign copies of objects to self
        self.model = copy(wrf_hydro_model)
        self.domain = copy(wrf_hydro_domain)

        # Assign domain version used if specified to version other than the wrf_hydro_model
        if domain_model_version is not None and domain_model_version != self.model.version:
            self.domain_model_version = domain_model_version

        # Create namelists
        self.hydro_namelist = \
            dict(self.model.hydro_namelists[self.model.version][self.domain.domain_config])
        self.hydro_namelist['hydro_nlist'].update(self.domain.namelist_patches[self.model.version]
            [self.domain.domain_config]['hydro_namelist']['hydro_nlist'])
        self.hydro_namelist['nudging_nlist'].update(self.domain.namelist_patches[self.model.version]
            [self.domain.domain_config]['hydro_namelist']['nudging_nlist'])

        self.namelist_hrldas = \
            dict(self.model.hrldas_namelists[self.model.version][self.domain.domain_config])
        self.namelist_hrldas['noahlsm_offline'].update(self.domain.namelist_patches
            [self.model.version][self.domain.domain_config]['namelist_hrldas']['noahlsm_offline'])
        self.namelist_hrldas['wrf_hydro_offline'].update(self.domain.namelist_patches
            [self.model.version][self.domain.domain_config]['namelist_hrldas']['wrf_hydro_offline'])


    def run(self,
            simulation_dir: str,
            num_cores: int = 2) -> str:
        """Run the wrf_hydro simulation
        Args:
            run_command: The command to execute the model. Defaults to prepared mpiexec
                         command using num_cores argument. Otherwise, supply a list that
                         can be passed to subprocess.run.
            num_cores: Optional, the number of cores to using default run_command

        Returns:
            A string indicating success of run and new attributes to the object

        TODO:
            Add option for custom run commands to deal with job schedulers
        """
        ###########################################################################
        # MAKE RUN DIRECTORIES
        # Construct all file/dir paths

        # Convert strings to Path objects
        self.simulation_dir = Path(simulation_dir)

        # Candidate compile files
        # Get list of table file paths
        table_files = list(self.model.compile_dir.glob('*.TBL'))

        # Get wrf_hydro.exe file path
        wrf_exe = self.model.compile_dir.joinpath('wrf_hydro.exe')

        # make directories and symmlink in files
        if self.simulation_dir.is_dir() is not True:
            self.simulation_dir.mkdir(parents=True)
        else:
            raise IOError(str(self.simulation_dir) + ' directory already exists')

        # Loop to make symlinks for each TBL file
        for from_file in table_files:
            # Create file paths to symlink
            to_file = self.simulation_dir.joinpath(from_file.name)
            # Create symlinks
            to_file.symlink_to(from_file)

        # Symlink in exe
        self.simulation_dir.joinpath(wrf_exe.name).symlink_to(wrf_exe)

        # Symlink in forcing
        self.simulation_dir.joinpath(self.domain.forcing_dir.name).\
            symlink_to(self.domain.forcing_dir, target_is_directory=True)
        # Symlink in DOMAIN
        self.simulation_dir.joinpath(self.domain.domain_dir.name).\
            symlink_to(self.domain.domain_dir, target_is_directory=True)
        # Symlink in RESTART
        self.simulation_dir.joinpath(self.domain.restart_dir.name).\
            symlink_to(self.domain.restart_dir, target_is_directory=True)

        # write hydro.namelist
        f90nml.write(self.hydro_namelist,
                     self.simulation_dir.joinpath('hydro.namelist'))
        # write namelist.hrldas
        f90nml.write(self.namelist_hrldas,
                     self.simulation_dir.joinpath('namelist.hrldas'))

        # Run the model
        chdir(self.simulation_dir)
        subprocess.run(['mpiexec','-np',str(num_cores),'./wrf_hydro.exe'])

        # String match diag files for successfull run
        # TODO JLM: i hate single character iterators
        with open(self.simulation_dir.joinpath('diag_hydro.00000')) as f:
            diag_file = f.read()
            if 'The model finished successfully.......' in diag_file:
                self.run_status = 0
            else:
                self.run_status = 1

        if self.run_status == 0:

            # TODO TJM - Make all files fall under an 'output_files' attirbute
            # Get diag files
            self.diag = list(self.simulation_dir.glob('diag_hydro.*'))

            # Get channel files
            self.channel_rt = wrf_hydro_ts(list(self.simulation_dir.glob('*CHRTOUT*')))

            # TODO TJM - Add additinal file types, restarts, lakes, etc.

            # create a UID for the simulation and save in file

            self.object_id = str(uuid4())
            with open(self.simulation_dir.joinpath('.uid'), 'w') as f:
                f.write(self.object_id)

            # Save object to simulation directory
            # Save the object out to the compile directory
            with open(self.simulation_dir.joinpath('wrf_hydro_sim.pkl'), 'wb') as f:
                pickle.dump(self, f, 2)

            return('Model run completed successfully')
        else:
            return ('Model run failed')

    # Define a reset method
    def reset(self, confirm: str):
        """Deletes the entire contents of the run directory and resets object to pre-run state
        Args:
            confirm: String of 'y' to confirm reset, or other to abort
        Returns:
            String indicating success of cleanup and reset of all run-time attributes
        """
        if confirm is 'y':
            rmtree(str(self.simulation_dir))
            atts_to_delete = ['simulation_dir', 'run_status', 'output_files', 'object_id']
            for att in atts_to_delete:
                self.__delattr__(att)
            return('compile directory deleted and wrf_hydro_model object returned to ' +
                   'pre-compile state')
        else:
            return("confirm argument must be 'y' to proceed with reset")


# END OF MODULE
##################################

def main():
    # Make wrfModel object
    wrfModel = wrf_hydro_model('/Volumes/d1/jmills/tempTests/wrf_hydro_nwm/trunk/NDHMS',
                               '/Volumes/d1/jmills/tempTests/Run')
    # Compile it
    # wrfModel.compile('gfort',compile_options=None)
    # Create domain object
    croton_dom_top_path = '/Volumes/d1/jmills/NCAR-docker/wrf_hydro_docker/domains/croton_NY/domain'
    domain = wrf_hydro_domain(croton_dom_top_path,
                              domain_config='NWM',
                              domain_dir='NWM/DOMAIN',
                              restart_dir='NWM/RESTART')
    wrfSim = wrf_hydro_simulation(wrfModel, domain)
    wrfSim.make_run_dir('/Volumes/d1/jmills/tempTests/sim')


    # docker testing
    # from wrf_hydro_model import *
    wrfModel = wrf_hydro_model('/home/docker/wrf_hydro_nwm/trunk/NDHMS')
    wrfModel.compile('gfort', '/home/docker/test/compile', overwrite=True)

    wrfDomain = wrf_hydro_domain('/home/docker/domain/croton_NY',
                                 domain_config='NWM',
                                 domain_dir='NWM/DOMAIN',
                                 restart_dir='NWM/RESTART')

    wrf_hydro_simulation(wrfModel, wrfDomain.run('/home/docker/test/run'))
