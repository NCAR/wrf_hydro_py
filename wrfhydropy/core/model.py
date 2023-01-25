import json
import os
import pathlib
import pickle
import shlex
import shutil
import subprocess
import uuid
import warnings

from .namelist import JSONNamelist


def get_git_revision_hash(the_dir: str) -> str:
    """Get the last git revision hash from a directory if directory is a git repository
    Args:
        the_dir: String for the directory path
    Returns:
         String with the git hash if a git repo or message if not
    """

    the_dir = pathlib.Path(the_dir)

    # First test if this is even a git repo. (Have to allow for this unless the wrfhydropy
    # testing brings in the wrf_hydro_code as a repo with a .git file.)
    dir_is_repo = subprocess.run(["git", "branch"],
                                 stderr=subprocess.STDOUT,
                                 stdout=open(os.devnull, 'w'),
                                 cwd=str(the_dir.absolute()))
    if dir_is_repo.returncode != 0:
        return 'could_not_get_hash'

    dirty = subprocess.run(['git', 'diff-index', 'HEAD'],  # --quiet seems to give the wrong result.
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE,
                           cwd=str(the_dir.absolute())).returncode
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
        hydro_namelist_config_file: str=None,
        hrldas_namelist_config_file: str=None,
        compile_options_config_file: str=None,
        compiler: str = 'gfort',
        pre_compile_cmd: str = None,
        compile_options: dict = None
    ):

        """Instantiate a Model object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/src'.
            model_config: The configuration of the model. Used to match a model to a domain
                configuration. Must be a key in both the *_namelists.json of in the source directory
                and the *_namelist_patches.json in the domain directory.
            machine_spec: Optional dictionary of machine specification or string containing the
                name of a known machine. Known machine names include 'cheyenne'. For an
                example of a machine specification see the 'cheyenne' machine specification using
                wrfhydropy.get_machine_spec('cheyenne').
            hydro_namelist_config_file: Path to a hydro namelist config file external to the model
                repository. Default(None) implies using the model src/hydro_namelists.json.
            hrldas_namelist_config_file: As for hydro_namelist_config_file, but for hrldas namelist.
            compile_options_config_file: As for hydro_namelist_config_file, but for compile options.
            compiler: The compiler to use, must be one of 'pgi','gfort',
                'ifort', or 'luna'.
            compile_options: Changes to default compile-time options.
        """

        # Instantiate all attributes and methods
        # Attributes set by init args
        self.source_dir = pathlib.Path(source_dir)
        """pathlib.Path: pathlib.Path object for source code directory."""

        self.model_config = model_config.lower()
        """str: Specified configuration for which the model is to be used, e.g. 'nwm_ana'"""

        self.compiler = compiler
        """str: The compiler chosen at compile time."""

        self.pre_compile_cmd = pre_compile_cmd
        """str: Command string to be executed prior to model compilation, e.g. to load modules"""

        self.compile_options = dict()
        """dict: Compile-time options. Defaults are loaded from json file stored with source
        code."""

        # Set nameilst config file defaults while allowing None to be passed.
        self.hydro_namelist_config_file = hydro_namelist_config_file
        """Namelist: Hydro namelist file specified for model config"""
        self.hrldas_namelist_config_file = hrldas_namelist_config_file
        """Namelist: HRLDAS namelist file specified for model config."""
        self.compile_options_config_file = compile_options_config_file
        """Namelist: Compile options file specified for model config."""

        default_hydro_namelist_config_file = 'hydro_namelists.json'
        default_hrldas_namelist_config_file = 'hrldas_namelists.json'
        default_compile_options_config_file = 'compile_options.json'

        if self.hydro_namelist_config_file is None:
            self.hydro_namelist_config_file = default_hydro_namelist_config_file
        if self.hrldas_namelist_config_file is None:
            self.hrldas_namelist_config_file = default_hrldas_namelist_config_file
        if self.compile_options_config_file is None:
            self.compile_options_config_file = default_compile_options_config_file

        # Load master namelists
        self.hydro_namelists = JSONNamelist(
            str(self.source_dir.joinpath(self.hydro_namelist_config_file))
        )
        """Namelist: Hydro namelist for specified model config"""
        self.hydro_namelists = self.hydro_namelists.get_config(self.model_config)

        self.hrldas_namelists = JSONNamelist(
            str(self.source_dir.joinpath(self.hrldas_namelist_config_file))
        )
        """Namelist: HRLDAS namelist for specified model config"""
        self.hrldas_namelists = self.hrldas_namelists.get_config(self.model_config)

        # Attributes set by other methods
        self.compile_dir = None
        """pathlib.Path: pathlib.Path object pointing to the compile directory."""

        self.git_hash = self._get_githash()
        """str: The git revision hash if seld.source_dir is a git repository"""

        self.version = None
        """str: Source code version from .version file stored with the source code."""

        self.compile_dir = None
        """pathlib.Path: pathlib.Path object pointing to the compile directory."""

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
        # Get code version
        with self.source_dir.joinpath('.version').open() as f:
            self.version = f.read()

        # Load compile options
        self.compile_options = JSONNamelist(
            str(self.source_dir.joinpath(self.compile_options_config_file))
        )
        """Namelist: Hydro namelist for specified model config"""
        self.compile_options = self.compile_options.get_config(self.model_config)

        # "compile_options" is the argument to __init__
        if compile_options is not None:
            self.compile_options.update(compile_options)

        # Add compiler and compile options as attributes and update if needed
        self.compiler = compiler

    def compile(self,
                compile_dir: pathlib.Path) -> str:
        """Compiles WRF-Hydro using specified compiler and compile options.
        Args:
            compile_dir: A non-existant directory to use for compilation.
        Returns:
            Success of compilation and compile directory used. Sets additional
            attributes to WrfHydroModel
        """

        self.compile_dir = pathlib.Path(compile_dir).absolute()

        self.modules = subprocess.run('module list', shell=True, stderr=subprocess.PIPE).stderr

        # check compile directory.
        if not self.compile_dir.is_dir():
            warnings.warn(str(self.compile_dir.absolute()) + ' directory does not exist, creating')
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
        if self.pre_compile_cmd is not None:
            compile_cmd += self.pre_compile_cmd + '; '
        compile_cmd += './configure ' + self.compiler + '; '
        compile_cmd += './compile_offline_NoahMP.sh '
        compile_cmd += str(compile_options_file.absolute())
        compile_cmd += '"'
        compile_cmd = shlex.split(compile_cmd)

        self.compile_log = subprocess.run(
            compile_cmd,
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
            if len(self.table_files) == 0:
                self.table_files = list(self.source_dir.joinpath('Run').glob('*.TBL'))

            shutil.copyfile(str(self.source_dir.joinpath('Run').joinpath('wrf_hydro.exe')),
                            str(self.compile_dir.joinpath('wrf_hydro.exe')))

            # Remove old files
            # shutil.rmtree(str(self.source_dir.joinpath('Run')))

            # Open permissions on copied compiled files
            subprocess.run(['chmod', '-R', '755', str(self.compile_dir)])

            # Get file lists as attributes
            # Get list of table file paths

            # Get wrf_hydro.exe file path
            self.wrf_hydro_exe = self.compile_dir.joinpath('wrf_hydro.exe')

            # Save the object out to the compile directory
            with self.compile_dir.joinpath('WrfHydroModel.pkl').open(mode='wb') as f:
                pickle.dump(self, f, 2)

            print('Model successfully compiled into ' + str(self.compile_dir.absolute()))
        else:
            # Save the object out to the compile directory
            with self.compile_dir.joinpath('WrfHydroModel.pkl').open(mode='wb') as f:
                pickle.dump(self, f, 2)
            raise ValueError('Model did not successfully compile.' +
                             self.compile_log.stderr.decode('utf-8'))

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

        # Symlink/copy in exe
        from_file = self.wrf_hydro_exe
        to_file = dest_dir.joinpath(from_file.name)
        if symlink:
            to_file.symlink_to(from_file)
        else:
            shutil.copy(str(from_file), str(to_file))

    def _get_githash(self) -> str:
        """Private method to get the git hash if source_dir is a git repository
        Returns:
            git hash string
        """
        return get_git_revision_hash(self.source_dir)
