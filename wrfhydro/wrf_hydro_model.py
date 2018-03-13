import subprocess
from pathlib import Path
from shutil import copyfile
import xarray as xr
import f90nml
import json
from copy import copy
from os import chdir, chmod


#########################
###File object classes
#Class for timeseries files
class wrf_hydro_ts(object):
    def open(self):
        return(xr.open_mfdataset(self, concat_dim='Time'))

class wrf_hydro_data(object):
    def open(self):
        return (xr.open_dataset(self))
#########################
###Classes for constructing and running a wrf_hydro simulation
#wrf_hydro_model class
class wrf_hydro_model(object):
    """The beginning of wrf_hydro python api
    Attributes:

    """

    def __init__(self, source_dir: str, new_compile_dir: str = None):
        """Create a wrf_hydro_model object.
        Args:
            source_dir: Directory containing the source code, e.g. 'wrf_hydro_nwm/trunk/NDHMS'.
            new_compile_dir: Optional, new directory to to hold results of code compilation.
        Returns:
            A wrf_hydro_model object.
        """

        #Setup directory paths
        self.source_dir = Path(source_dir)

        if new_compile_dir is None:
            self.compile_dir = self.source_dir.joinpath('Run')
        else:
            self.compile_dir = Path(new_compile_dir)
            if self.compile_dir.is_dir() is False: self.compile_dir.mkdir(parents=True)

        #Load master namelists
        self.hydro_namelists = json.load(open(self.source_dir.joinpath('hydro_namelists.json')))
        self.hrldas_namelists = json.load(open(self.source_dir.joinpath('hrldas_namelists.json')))

        #Get code version
        with open(self.source_dir.joinpath('.version')) as f:
            self.version = f.read()

    def compile(self, compiler: str,compile_options: dict = None) -> str:
        """Compiles WRF-Hydro using specified compiler and compile options.
        Args:
            compiler: The compiler to use, must be one of 'pgi','gfort','ifort', or 'luna'
            compile_options: Changes to default compile-time options. Defaults are {'WRF_HYDRO':1,'HYDRO_D':1,
            'SPATIAL_SOIL':1,'WRF_HYDRO_RAPID':0,'WRFIO_NCD_LARGE_FILE_SUPPORT':1,'NCEP_WCOSS':1,'WRF_HYDRO_NUDGING':0}

        Returns:
            Success of compilation and compile directory used. Sets additional attributes to
            wrf_hydro_model

        """
        #Make dictionary of compiler options
        compilers = {'pgi':'1',
                     'gfort':'2',
                     'ifort':'3',
                     'luna':'4'}

        #Add compiler and compile options as attributes and update if needed
        self.compile_options = {'WRF_HYDRO':1,'HYDRO_D':1,'SPATIAL_SOIL':1,'WRF_HYDRO_RAPID':0,
                                        'WRFIO_NCD_LARGE_FILE_SUPPORT':1,'NCEP_WCOSS':1,'WRF_HYDRO_NUDGING':0}
        if compile_options is not None:
            self.compile_options.update(compile_options)
        self.compiler = compiler

        #Get directroy for setEnvar
        set_vars = self.source_dir.joinpath('set_envar.sh')

        #Write setEnvar file
        with open(set_vars,'w') as file:
            for option, value in self.compile_options.items():
                file.write("export {}={}\n".format(option, value))

        #Compile
        #Change to source code directory for compile time
        chdir(self.source_dir)

        subprocess.run(['./configure',
                        compilers[compiler]])
        subprocess.run(['./compile_offline_NoahMP.sh',
                        str(set_vars)])

        #Open permissions on compiled files
        subprocess.run(['chmod','-R','777',str(self.source_dir.joinpath('Run'))])

        #Wrf hydro always puts files in source directory under a new directory called 'Run'
        #Copy files to new directory if its not the same as the source code directory
        if self.compile_dir.parent is not self.source_dir:
            for file in self.source_dir.joinpath('Run').glob('*.TBL'):
                copyfile(file,str(self.compile_dir.joinpath(file.name)))

            copyfile(str(self.source_dir.joinpath('Run').joinpath('wrf_hydro.exe')),
                     str(self.compile_dir.joinpath('wrf_hydro.exe')))
        #Open permissions on copied compiled files
        subprocess.run(['chmod', '-R', '777', str(self.compile_dir)])

        return('Model successfully compiled into ' + str(self.compile_dir))

class wrf_hydro_domain(object):
    def __init__(self,domain_top_dir: str, domain_config: str, namelist_patch_file: str = 'namelist_patches.json',
                 forcing_dir: str = 'FORCING',domain_dir:str = 'DOMAIN',restart_dir:str = 'RESTART'):
        """Create a wrf_hydro_domain object.
        Args:
            domain_top_dir: Parent directory containing all domain directories and files. All files and folders are
            relative to this directory
            domain_config: The domain configuration to use, options are 'NWM','Gridded', or 'Reach'
            namelist_patch_file: Filename of json file containing namelist patches
            forcing_dir: Directory containing forcing data
            domain_dir: Directory containing domain files
            restart_dir: Directory containing restart files
        Returns:
            A wrf_hydro_domain object
        """
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
            raise IOError(str(self.forcing_dir) + ' directory not found in ' + str(self.domain_top_dir))
        if self.domain_dir.is_dir() is False:
            raise IOError(str(self.domain_dir) + ' directory not found in ' + str(self.domain_top_dir))
        if self.restart_dir.is_dir() is False:
            raise IOError(str(self.restart_dir) + ' directory not found in ' + str(self.domain_top_dir))
        if self.namelist_patch_file.is_file() is False:
            raise IOError(str(self.namelist_patch_file) + ' file not found in ' + str(self.domain_top_dir))
        #######################

        #Setup file attributes
        #namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file))

        #forcing files
        self.forcing_files = wrf_hydro_ts(list(self.forcing_dir.glob('*')))#JMCHECK - handle non-forcing files in forcing dir?

        #restart files
        self.restart_files = wrf_hydro_ts(list(self.forcing_dir.glob('*')))

        #TODO - add in a search function to grab the proper routelnk
        #TODO - this might need to belong in the wrf_hydro_sim since routelink needs to be pulled by model version number
        self.route_link = self.namelist_patch_file


class wrf_hydro_simulation(object):
    def __init__(self, wrf_hydro_model: object,wrf_hydro_domain: object):
        """Create a wrf_hydro_simulation object
        Args:
            wrf_hydro_model: A wrf_hydro_model object
            wrf_hydro_domain: A wrf_hydro_domain object
        Returns:
            A wrf_hydro_simulation object
        """

        #assign objects to self
        self.model = copy(wrf_hydro_model)
        self.domain = copy(wrf_hydro_domain)

        #Create namelists
        self.hydro_namelist = dict(self.model.hydro_namelists[self.model.version][self.domain.domain_config])
        self.hydro_namelist['hydro_nlist'].update(self.domain.namelist_patches[self.model.version]\
                                                      [self.domain.domain_config]['hydro_namelist']['hydro_nlist'])
        self.hydro_namelist['nudging_nlist'].update(self.domain.namelist_patches[self.model.version]\
                                                        [self.domain.domain_config]['hydro_namelist']['nudging_nlist'])

        self.namelist_hrldas = dict(self.model.hrldas_namelists[self.model.version][self.domain.domain_config])
        self.namelist_hrldas['noahlsm_offline'].update(self.domain.namelist_patches[self.model.version]\
                                                           [self.domain.domain_config]['namelist_hrldas']\
                                                           ['noahlsm_offline'])
        self.namelist_hrldas['wrf_hydro_offline'].update(self.domain.namelist_patches[self.model.version]\
                                                           [self.domain.domain_config]['namelist_hrldas']\
                                                           ['wrf_hydro_offline'])

    def make_run_dir(self,simulation_dir: str):
        """Create run directory for a wrf_hydro simulation.
        Args:
            simulation_dir: Directory to use for simulation files and output.

        Returns:
            A string indicating success of directory creation and a new attribute to the object, simulation dir
        """

        #########################
        ###Construct all file/dir paths

        # Convert strings to Path objects
        self.simulation_dir = Path(simulation_dir)

        ###Candidate compile files
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
        self.simulation_dir.joinpath(self.domain.forcing_dir.name).symlink_to(self.domain.forcing_dir,
                                                                              target_is_directory=True)
        # Symlink in DOMAIN
        self.simulation_dir.joinpath(self.domain.domain_dir.name).symlink_to(self.domain.domain_dir,
                                                                             target_is_directory=True)
        # Symlink in RESTART
        self.simulation_dir.joinpath(self.domain.restart_dir.name).symlink_to(self.domain.restart_dir,
                                                                              target_is_directory=True)

        # write hydro.namelist
        f90nml.write(self.hydro_namelist,
                     self.simulation_dir.joinpath('hydro.namelist'))
        # write namelist.hrldas
        f90nml.write(self.namelist_hrldas,
                     self.simulation_dir.joinpath('namelist.hrldas'))

        return ('Successfully created simulation directory ' + str(self.simulation_dir))


    def run(self,num_cores: int = 2) -> str:
        """Run the wrf_hydro simulation
        Args:
            run_command: The command to execute the model. Defaults to prepared mpiexec command using num_cores argument.
            Otherwise, supply a list that can be passed to subprocess.run.
            num_cores: Optional, the number of cores to using default run_command

        Returns:
            A string indicating success of run and new attributes to the object

        TODO:
            Add option for custom run commands to deal with job schedulers
        """
        chdir(self.simulation_dir)
        subprocess.run(['mpiexec','-np',str(num_cores),'./wrf_hydro.exe'])

        #String match diag files for successfull run
        with open(self.simulation_dir.joinpath('diag_hydro.00000')) as f:
            diag_file = f.read()
            if 'The model finished successfully.......' in diag_file:
                self.run_status = 0
            else:
                self.run_status = 1

        if self.run_status == 0:
            #Setup output file attributes
            #Get diag files
            self.diag_files = list(self.simulation_dir.glob('diag_hydro.*'))

            #Get channel files
            self.channel_files = wrf_hydro_ts(list(self.simulation_dir.glob('*CHRTOUT*')))

            #TODO - Add additinal file types, restarts, lakes, etc.
            return('Model run completed successfully')
        else:
            return ('Model run failed')

##################################
##################################
##################################
#END OF MODULE
##################################

def main():
    #Make wrfModel object
    wrfModel = wrf_hydro_model('/Volumes/d1/jmills/tempTests/wrf_hydro_nwm/trunk/NDHMS','/Volumes/d1/jmills/tempTests/Run')
    #Compile it
    #wrfModel.compile('gfort',compile_options=None)
    #Create domain object
    domain = wrf_hydro_domain('/Volumes/d1/jmills/NCAR-docker/wrf_hydro_docker/domains/croton_NY/domain',
                              domain_config='NWM',
                              domain_dir='NWM/DOMAIN',
                              restart_dir='NWM/RESTART')
    wrfSim=wrf_hydro_simulation(wrfModel,domain)
    wrfSim.make_run_dir('/Volumes/d1/jmills/tempTests/sim')


    #docker testing
    #from wrf_hydro_model import *
    wrfModel = wrf_hydro_model('/home/docker/wrf_hydro_nwm/trunk/NDHMS','/home/docker/test/compile')
    wrfModel.compile('gfort')

    wrfDomain = wrf_hydro_domain('/home/docker/domain/croton_NY',domain_config='NWM',domain_dir='NWM/DOMAIN',restart_dir='NWM/RESTART')

    wrfSim = wrf_hydro_simulation(wrfModel, wrfDomain)
    wrfSim.make_run_dir('/home/docker/test/run2')
    wrfSim.run()

#############################
###Classes for testing

