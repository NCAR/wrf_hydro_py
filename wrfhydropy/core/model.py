import subprocess
import pathlib
import shutil
import json
import pickle
import uuid
import warnings
import os
import shlex

def get_machine_spec(machine_name: str) -> dict:
    """Make all file paths relative to a given directory, useful for opening file
    attributes in a run object after it has been moved or copied to a new directory or
    system.
    Args:
        machine_name: Name of a known machine. Known machines include 'cheyenne' and
        'wrfhydro_docker'
    Returns:
        machine specification dictionary for use with a wrfhydropy.WrfHydroModel class
    """

    known_machines = {'cheyenne':
                          {'modules':
                               {'base':['nco/4.6.2','python/3.6.2'],
                                'ifort':['intel/16.0.3','ncarenv/1.2','ncarcompilers/0.4.1',
                                         'mpt/2.15f','netcdf/4.4.1'],
                                'gfort':['gnu/7.1.0','ncarenv/1.2','ncarcompilers/0.4.1',
                                         'mpt/2.15','netcdf/4.4.1.1']
                                },
                           'scheduler':
                               {'name':'PBS',
                                'max_walltime':'12:00'},
                           'cores_per_node':36,
                           'exe_cmd':
                               {'PBS':'mpiexec_mpt ./wrf_hydro.exe',
                                'default': 'mpirun -np %d ./wrf_hydro.exe'
                                }
                           },
                      'wrfhydro_docker':
                          {'modules':None,
                           'scheduler':None,
                           'cores_per_node': None,
                           'exe_cmd':
                               {'default': 'mpirun -ppn %d ./wrf_hydro.exe'}
                           }
                      }
    if machine_name not in known_machines.keys():
        raise LookupError(machine_name + ' is not a known machine')
    else:
        return known_machines[machine_name]

def check_machine_spec(machine_spec: dict) -> dict:
    """Make all file paths relative to a given directory, useful for opening file
    attributes in a run object after it has been moved or copied to a new directory or
    system.
    Args:
        machine_name: Name of a known machine. Known machines include 'cheyenne' and
        'wrfhydro_docker'
    Returns:
        The input machine specification
    Raises:
        KeyError if reauired keys are missing from the machine_spec dictionary
    """

    required_keys = get_machine_spec('cheyenne').keys()
    missing_keys = list(set(required_keys) - set(machine_spec.keys()))

    if machine_spec.keys() != required_keys:
        raise KeyError('Missing the following required keys: ' + ','.join(missing_keys))
    else:
        return machine_spec

def get_git_revision_hash(the_dir):

    # First test if this is even a git repo. (Have to allow for this unless the wrfhydropy
    # testing brings in the wrf_hydro_code as a repo with a .git file.)
    dir_is_repo = subprocess.call(
        ["git", "branch"],
        stderr=subprocess.STDOUT,
        stdout=open(os.devnull, 'w'),
        cwd=str(the_dir.absolute())
    )
    if dir_is_repo != 0:
        warnings.warn('The source directory is NOT a git repo: ' + str(the_dir))
        return 'not-a-repo'

    dirty = subprocess.run(
        ['git', 'diff-index', 'HEAD'],  # --quiet seems to give the wrong result.
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(the_dir.absolute())
    ).returncode
    the_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=str(the_dir.absolute()))
    the_hash = the_hash.decode('utf-8').split()[0]
    if dirty:
        the_hash += '--DIRTY--'
    return the_hash

class Model(object):
    """Class for a WRF-Hydro model, which consitutes the model source code and compiled binary.
    """

    def __init__(
            self,
            source_dir: str,
            model_config: str,
            machine_spec: [dict, str] = None,
            compiler: str = 'gfort',
            compile_options: dict = None):
        """Instantiate a WrfHydroModel object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/trunk/NDHMS'.
            model_config: The configuration of the model. Used to match a model to a domain
            configuration. Must be one of either 'NWM', 'Gridded', or 'Reach'.
            machine_spec: Optional dictionary of machine specification or string containing the
            name of a known machine. Known machine names include 'cheyenne'. For an
            example of a machine specification see the 'cheyenne' machine specification using
            wrfhydropy.get_machine_spec('cheyenne').
            compiler: The compiler to use, must be one of 'pgi','gfort',
                'ifort', or 'luna'.
            compile_options: Changes to default compile-time options.

        Returns:
            A WrfHydroModel object.
        """

        # Instantiate all attributes and methods
        self.source_dir = None
        """pathlib.Path: pathlib.Path object for source code directory."""
        self.model_config = None
        """str: String indicating model configuration for compile options, must be one of 'NWM', 
        'Gridded', or 'Reach'."""

        self.machine_spec = machine_spec
        if type(machine_spec) == str:
            self.machine_spec = get_machine_spec(machine_spec)
        if machine_spec == dict:
            self.machine_spec = check_machine_spec(machine_spec)

        """list: List of modules to use for model. Note these modules will be used for all 
        subsequent system calls for model operations."""

        self.hydro_namelists = dict()
        """dict: Master dictionary of all hydro.namelists stored with the source code."""
        self.hrldas_namelists = dict()
        """dict: Master dictionary of all namelist.hrldas stored with the source code."""
        self.compile_options = dict()
        """dict: Compile-time options. Defaults are loaded from json file stored with source 
        code."""
        self.git_hash = None
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
        self.table_files = list()
        """list: pathlib.Paths to *.TBL files generated at compile-time."""
        self.wrf_hydro_exe = None
        """pathlib.Path: pathlib.Path to wrf_hydro.exe file generated at compile-time."""

        # Set attributes
        ## Setup directory paths
        self.source_dir = pathlib.Path(source_dir).absolute()

        ## Get code version
        with self.source_dir.joinpath('.version').open() as f:
            self.version = f.read()
        print(self.version)

        ## Get model config
        self.model_config = model_config

        ## Load master namelists
        self.hydro_namelists = \
            json.load(self.source_dir.joinpath('hydro_namelists.json').open())
        self.hydro_namelists = self.hydro_namelists[self.version][self.model_config]

        self.hrldas_namelists = \
            json.load(self.source_dir.joinpath('hrldas_namelists.json').open())
        self.hrldas_namelists = self.hrldas_namelists[self.version][self.model_config]

        ## Load compile options
        compile_json = json.load(self.source_dir.joinpath('compile_options.json').open())
        self.compile_options = compile_json[self.version][self.model_config]

        # Add compiler and compile options as attributes and update if needed
        self.compiler = compiler
        if compile_options is not None:
            self.compile_options.update(compile_options)

    def get_githash(self) -> str:
        """Get the git hash if source_dir is a git repository

        Returns:
            git hash string

        """
        return get_git_revision_hash(self.source_dir)

    def compile(
            self,
            compile_dir: pathlib.Path) -> str:
        """Compiles WRF-Hydro using specified compiler and compile options.
        Args:
            compile_dir: A non-existant directory to use for compilation.
        Returns:
            Success of compilation and compile directory used. Sets additional
            attributes to WrfHydroModel
        """

        self.compile_dir = pathlib.Path(compile_dir)

        # check compile directory.
        if self.compile_dir.is_dir():
            raise IsADirectoryError(str(self.compile_dir.absolute()) + ' directory already exists')

        # MAke compile directory
        self.compile_dir.mkdir(parents=True)

        # Remove run directory if it exists in the source_dir
        source_compile_dir = self.source_dir.joinpath('Run')
        if source_compile_dir.is_dir():
            shutil.rmtree(str(source_compile_dir.absolute()))

        # Get directory for setEnvar
        compile_options_file = self.source_dir.joinpath('compile_options.sh')

        # Write setEnvar file
        with compile_options_file.open(mode='w') as file:
            for option, value in self.compile_options.items():
                file.write("export {}={}\n".format(option, value))

        # Compile
        # Create compile command for machine spec
        compile_cmd = '/bin/bash -c "'
        if self.machine_spec is not None:
            modules = ' '.join(self.machine_spec['modules'][self.compiler])
            compile_cmd += 'module purge; module load ' + modules + '; '
        compile_cmd += './configure ' + self.compiler + '; '
        compile_cmd += './compile_offline_NoahMP.sh '
        compile_cmd += str(compile_options_file.absolute())
        compile_cmd += '"'
        compile_cmd = shlex.split(compile_cmd)

        self.compile_log = subprocess.run(compile_cmd,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE,
                                          cwd=str(self.source_dir.absolute())
                                          )

        # Add in unique ID file to match this object to prevent assosciating
        # this directory with another object
        self.object_id = str(uuid.uuid4())

        with self.compile_dir.joinpath('.uid').open(mode='w') as f:
            f.write(self.object_id)

        if self.compile_log.returncode == 0:
            # Open permissions on compiled files
            subprocess.run(['chmod', '-R', '755', str(self.source_dir.joinpath('Run'))])

            # Wrf hydro always puts files in source directory under a new directory called 'Run'
            # Copy files to the specified simulation directory if its not the same as the
            # source code directory
            for file in self.source_dir.joinpath('Run').glob('*.TBL'):
                shutil.copyfile(file, str(self.compile_dir.joinpath(file.name)))

            shutil.copyfile(str(self.source_dir.joinpath('Run').joinpath('wrf_hydro.exe')),
                            str(self.compile_dir.joinpath('wrf_hydro.exe')))

            # Remove old files
            shutil.rmtree(self.source_dir.joinpath('Run'))

            # Open permissions on copied compiled files
            subprocess.run(['chmod', '-R', '755', str(self.compile_dir)])

            # Get file lists as attributes
            # Get list of table file paths
            self.table_files = list(self.compile_dir.glob('*.TBL'))

            # Get wrf_hydro.exe file path
            self.wrf_hydro_exe = self.compile_dir.joinpath('wrf_hydro.exe')

            # Save the object out to the compile directory
            with self.compile_dir.joinpath('WrfHydroModel.pkl').open(mode='wb') as f:
                pickle.dump(self, f, 2)

            print('Model successfully compiled into ' + str(self.compile_dir.absolute()))
        else:
            raise ValueError('Model did not successfully compile.')

    def copy_files(self, dest_dir: str, symlink: bool = True):
        """Copy domain files to new directory
        Args:
            dest_dir: The destination directory for files
            symlink: Symlink files instead of copy
        """

        # Convert dir to pathlib.Path
        dest_dir = pathlib.Path(dest_dir)

        # Make directory if it does not exist.
        if not dest_dir.is_dir():
            dest_dir.mkdir(parents=True)

        # Loop to make symlinks/copies for each TBL file
        for from_file in self.table_files:
            # Create file paths to symlink
            to_file = dest_dir.joinpath(from_file.name)
            # Create symlinks
            if symlink:
                to_file.symlink_to(from_file)
            else:
                shutil.copy(str(from_file),str(to_file))

        # Symlink/copy in exe
        from_file = self.wrf_hydro_exe
        to_file = dest_dir.joinpath(from_file.name)
        if symlink:
            to_file.symlink_to(from_file)
        else:
            shutil.copy(str(from_file), str(to_file))