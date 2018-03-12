import subprocess
from pathlib import Path
from shutil import copyfile
import xarray as xr
import f90nml
import json

#wrf_hydro_model class
class wrf_hydro_model(object):
    """The beginning of wrf_hydro python api
    Attributes:

    """

    def __init__(self, source_dir,new_compile_dir=None):
        """Return a starter wrf_hydro_model object"""

        #Setup directory paths
        self.source_dir = Path(source_dir)

        if new_compile_dir is None:
            self.compile_dir = self.source_dir.joinpath('Run')
        else:
            self.compile_dir = Path(new_compile_dir)
            if self.compile_dir.is_dir() is False: self.compile_dir.mkdir(parents=True)

        #Load master namelists
        self.base_hydro_namelists = json.load(open(self.source_dir.joinpath('base_hydro_namelists.json')))
        self.base_hrldas_namelists = json.load(open(self.source_dir.joinpath('base_namelists_hrldas.json')))

    def compile(self, compiler,compile_options={'WRF_HYDRO':1,'HYDRO_D':1,'SPATIAL_SOIL':1,'WRF_HYDRO_RAPID':0,
                                        'WRFIO_NCD_LARGE_FILE_SUPPORT':1,'NCEP_WCOSS':1,'WRF_HYDRO_NUDGING':0}):

        #Make dictionary of compiler options
        compilers = {'pgi':'1',
                     'gfort':'2',
                     'ifort':'3',
                     'luna':'4'}

        #Add compiler and compile options as attributes
        self.compile_options = compile_options
        self.compiler = compiler

        #Get directroy for setEnvar
        set_vars = self.source_dir.joinpath('set_envar.sh')

        #Write setEnvar file
        with open(set_vars,'w') as file:
            for option, value in compile_options.items():
                file.write("export {}={}\n".format(option, value))

        #Compile
        configure_path = str(self.source_dir.joinpath('configure'))
        compile_noah_mp_path = str(self.source_dir.joinpath('compile_offline_NoahMP.sh'))
        subprocess.run(['bash',configure_path,
                        compilers[compiler]])
        subprocess.run(['bash',compile_noah_mp_path,
                        str(set_vars)])

        #Wrf hydro always puts files in source directory under a new directory called 'Run'
        #Copy files to new directory if its not the same as the source code directory
        if self.compile_dir.parent is not self.source_dir:
            for file in self.compile_dir.glob('*.TBL'):
                copyfile(file,str(self.compile_dir))
            copyfile(str(self.compile_dir.glob('wrf_hydro.exe')),str(self.compile_dir))

class wrf_hydro_domain(object):
    def __init__(self,domain_top_dir,namelist_patch_file='namelist_patches.json',forcing_dir='FORCING',domain_dir='DOMAIN',
                 restart_dir='RESTART'):
        """Return a starter wrf_hydro_domain object"""
        self.domain_top_dir = Path(domain_top_dir)
        self.forcing_dir = self.domain_top_dir.joinpath(forcing_dir)
        self.domain_dir = self.domain_top_dir.joinpath(domain_dir)
        self.restart_dir = self.domain_top_dir.joinpath(restart_dir)
        self.namelist_patch_file = self.domain_top_dir.joinpath(namelist_patch_file)
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

        #Load namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file))

        #Determine conifguration from namelist
        #self.configuration = self.namelist_patches['configuration']

    def open_forcing_files(self,load=False):
        forcing_files = list(self.forcing_dir.glob('*'))
        self.forcing_data=xr.open_mfdataset(forcing_files,concat_dim='Time')
        if load:
            self.forcing_data = self.forcing_data.load()
        return('Forcing data loaded to forcing_data attribute')

    def open_restart_files(self,load=False):
        restart_files = list(self.restart_dir.glob('*'))
        self.restart_data=xr.open_mfdataset(restart_files,concat_dim='Time')
        if load:
            self.restart_data = self.restart_data.load()
        return('Forcing data loaded to forcing_data attribute')


class wrf_hydro_simulation(object):
    def __init__(self, wrf_hydro_model,wrf_hydro_domain):
        """Return a starter wrf_hydro_simulation object"""

        #assign objects to self
        self.model = wrf_hydro_model
        self.domain = wrf_hydro_domain

    def make_run_dir(self,simulation_dir):
        """Function to create run directory for a wrf_hydro simulation.
        Args:
            simulation_dir: String or path-like object indicating directory for simulation files.
            domain_config_dir: The directory containing the domain configuration of interest, e.g. /domain/NWM

        Returns:
            A string indicating success of directory creation and a new attribute to the object, simulation dir

        """

        #########################
        ###Construct all file/dir paths

        # Convert strings to Path objects
        self.simulation_dir = Path(simulation_dir)

        ###Candidate compile files
        # Get list of table file paths
        candidate_table_files = list(self.model.compile_dir.glob('*.TBL'))

        # Get wrf_hydro.exe file path
        candidate_wrf_exe = self.model.compile_dir.joinpath('wrf_hydro.exe')

        # Get namelist paths
        hydro_namelist = self.domain.namelist_patches
        namelist_hrldas = self.domain.namelist_patches

        # make directories and symmlink in files
        self.simulation_dir.mkdir()

        # Loop to make symlinks for each TBL file
        for from_file in candidate_table_files:
            # Create file paths to symlink
            to_file = self.simulation_dir.joinpath(from_file.name)
            # Create symlinks
            to_file.symlink_to(from_file)
        # Symlink in exe
        self.simulation_dir.joinpath(candidate_wrf_exe.name).symlink_to(candidate_wrf_exe)

        # Symlink in forcing
        self.simulation_dir.joinpath(self.domain.forcing_dir.name).symlink_to(self.domain.forcing_dir, target_is_directory=True)
        # Symlink in DOMAIN
        self.simulation_dir.joinpath(self.domain.domain_dir.name).symlink_to(self.domain.domain_dir, target_is_directory=True)
        # Symlink in RESTART
        self.simulation_dir.joinpath(self.domain.restart_dir.name).symlink_to(self.domain.restart_dir, target_is_directory=True)
        # Symlink in hydro.namelist
        #self.simulation_dir.joinpath(hydro_namelist.name).symlink_to(hydro_namelist)
        # Symlink in namelist.hrldas
        #self.simulation_dir.joinpath(namelist_hrldas.name).symlink_to(namelist_hrldas)

        return ('Successfully created simulation directory ' + str(self.simulation_dir))


    def run(self,run_hydro_options,run_hrlds_options):
        print('placeholder')




def main():
    # Try it out
    wrfModel = wrf_hydro_model('/Volumes/d1/jmills/tempTests/wrf_hydro_nwm/trunk/NDHMS','/Volumes/d1/jmills/tempTests/Run')
    wrfModel.compile('gfort')
    domain = wrf_hydro_domain('/Volumes/d1/jmills/NCAR-docker/wrf_hydro_docker/domains/croton_NY/domain',
                              domain_dir='NWM/DOMAIN',
                              restart_dir='NWM/RESTART')

    wrfSim=wrf_hydro_simulation(wrfModel,domain)
    wrfSim.make_run_dir('/Volumes/d1/jmills/tempTests/sim')

