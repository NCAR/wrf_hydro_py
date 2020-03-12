import pathlib
import shutil

from .ioutils import \
    WrfHydroStatic, \
    WrfHydroTs
from .namelist import JSONNamelist


class Domain(object):
    """Class for a WRF-Hydro domain, which constitutes all domain-specific files needed for a
    setup.
    """

    def __init__(self,
                 domain_top_dir: str,
                 domain_config: str,
                 compatible_version: str = None,
                 hydro_namelist_patch_file: str = 'hydro_namelist_patches.json',
                 hrldas_namelist_patch_file: str = 'hrldas_namelist_patches.json'
                 ):
        """Instantiate a Domain object
        Args:
            domain_top_dir: Parent directory containing all domain directories and files.
            domain_config: The domain configuration to use, options are 'NWM',
                'Gridded', or 'Reach'
            compatible_version: String indicating the compatible model version, required if no
            .version file included in domain_top_dir.
            hydro_namelist_patch_file: Filename of json file containing namelist patches for
            hydro namelist
            hrldas_namelist_patch_file: Filename of json file containing namelist patches for
            hrldas namelist
        """

        # Instantiate arguments to object
        # Make file paths
        self.domain_top_dir = pathlib.Path(domain_top_dir).absolute()
        """pathlib.Path: pathlib.Paths to *.TBL files generated at compile-time."""

        self.domain_config = domain_config.lower()
        """str: Specified configuration for which the domain is to be used, e.g. 'NWM_ana'"""

        self.compatible_version = compatible_version
        """str: Source-code version for which the domain is to be used."""

        # Check .version file if compatible_version not specified
        if self.compatible_version is None:
            try:
                with self.domain_top_dir.joinpath('.version').open() as f:
                    self.compatible_version = f.read()
            except FileNotFoundError:
                raise FileNotFoundError('file .version not found in directory ' +
                                        str(self.domain_top_dir) + ' and compatible_version not '
                                        'specified')

        # Load namelist patches
        hydro_namelist_patch_file = self.domain_top_dir.joinpath(hydro_namelist_patch_file)
        hrldas_namelist_patch_file = self.domain_top_dir.joinpath(hrldas_namelist_patch_file)

        self.hydro_namelist_patches = JSONNamelist(str(hydro_namelist_patch_file))
        """Namelist: Domain-specific hydro namelist settings."""
        self.hydro_namelist_patches = self.hydro_namelist_patches.get_config(self.domain_config)

        self.hrldas_namelist_patches = JSONNamelist(str(hrldas_namelist_patch_file))
        """Namelist: Domain-specific hrldas namelist settings."""
        self.hrldas_namelist_patches = self.hrldas_namelist_patches.get_config(self.domain_config)

        self.hydro_files = list()
        """list: Files specified in hydro_nlist section of the domain namelist patches"""
        self.nudging_files = list()
        """list: Files specified in nudging_nlist section of the domain namelist patches"""
        self.lsm_files = list()
        """list: Files specified in noahlsm_offline section of the domain namelist patches"""

        self.nudging_dir = None
        """pathlib.Path: path to the nudging obs directory"""

        self.forcing_dir = None
        """pathlib.Path: path to the forcing directory"""

        ###

        # Create file paths from hydro namelist
        domain_hydro_nlist = self.hydro_namelist_patches['hydro_nlist']

        for key, value in domain_hydro_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.hydro_files.append(WrfHydroStatic(file_path))
                else:
                    self.hydro_files.append(file_path)

        # Create file paths from nudging namelist
        domain_nudging_nlist = self.hydro_namelist_patches['nudging_nlist']

        for key, value in domain_nudging_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.nudging_files.append(WrfHydroStatic(file_path))
                else:
                    self.nudging_files.append(file_path)
            if key == 'timeslicepath' and value != '':
                self.nudging_dir = file_path
                self.nudging_files.append(WrfHydroTs(file_path.glob('*')))

        # Create file paths from lsm namelist
        domain_lsm_nlist = \
            self.hrldas_namelist_patches["noahlsm_offline"]

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

    def copy_files(self, dest_dir: str, symlink: bool = True):
        """Copy domain files to a new directory
        Args:
            dir: The destination directory for domain files
            symlink: Symlink domain files instead of copy
        """

        # Convert dir to pathlib.Path
        dest_dir = pathlib.Path(dest_dir)

        # Make directory if it does not exist.
        if not dest_dir.is_dir():
            dest_dir.mkdir(parents=True)

        # Create symlinks/copies
        # Symlink/copy in forcing
        from_dir = self.forcing_dir
        to_dir = dest_dir.joinpath(from_dir.name)
        if symlink:
            to_dir.symlink_to(from_dir, target_is_directory=True)
        else:
            shutil.copytree(str(from_dir), str(to_dir))

        # create DOMAIN directory and symlink in files
        # Symlink in hydro_files
        for from_path in self.hydro_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            try:
                relative_path = from_path.relative_to(self.domain_top_dir)
            except ValueError:
                pass
            else:
                to_path = dest_dir.joinpath(relative_path)
                if to_path.parent.is_dir() is False:
                    to_path.parent.mkdir(parents=True)
                if symlink:
                    to_path.symlink_to(from_path)
                else:
                    shutil.copy(str(from_path), str(to_path))

        # Symlink in nudging files

        # handling nudging obs files
        # Users may signal "None" by the null string (''), treat them the same.
        if not (self.nudging_dir is None or self.nudging_dir is ''):
            from_dir = self.nudging_dir
            try:
                to_dir = dest_dir.joinpath(from_dir.relative_to(self.domain_top_dir))
            except ValueError:
                pass
            else:
                if symlink:
                    to_dir.symlink_to(from_dir, target_is_directory=True)
                else:
                    shutil.copy(str(from_dir), str(to_dir))

        for from_path in self.nudging_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            if type(from_path) is not WrfHydroTs:
                try:
                    relative_path = from_path.relative_to(self.domain_top_dir)
                except ValueError:
                    pass
                else:
                    to_path = dest_dir.joinpath(relative_path)
                    if to_path.parent.is_dir() is False:
                        to_path.parent.mkdir(parents=True)
                    if symlink:
                        to_path.symlink_to(from_path)
                    else:
                        shutil.copy(str(from_path), str(to_path))

        # Symlink in lsm files
        for from_path in self.lsm_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            try:
                relative_path = from_path.relative_to(self.domain_top_dir)
            except ValueError:
                pass
            else:
                to_path = dest_dir.joinpath(relative_path)
                if to_path.parent.is_dir() is False:
                    to_path.parent.mkdir(parents=True)
                if symlink:
                    to_path.symlink_to(from_path)
                else:
                    shutil.copy(str(from_path), str(to_path))

        model_files = [*self.hydro_files,
                       *self.nudging_files,
                       *self.lsm_files]
        for ff in model_files:
            if type(ff) is not WrfHydroTs:
                if 'RESTART' in str(ff.name):
                    to_path = dest_dir.joinpath(ff.name).absolute()
                    if symlink:
                        to_path.symlink_to(ff)
                    else:
                        shutil.copy(str(ff), str(to_path))
                if 'HYDRO_RST' in str(ff.name):
                    to_path = dest_dir.joinpath(ff.name).absolute()
                    if symlink:
                        to_path.symlink_to(ff)
                    else:
                        shutil.copy(str(ff), str(to_path))
                if 'nudgingLastObs' in str(ff.name):
                    to_path = dest_dir.joinpath(ff.name).absolute()
                    if symlink:
                        to_path.symlink_to(ff)
                    else:
                        shutil.copy(str(ff), str(to_path))
