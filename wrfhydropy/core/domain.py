import pathlib
import json
import re
import shutil

from .fileutilities import \
    WrfHydroStatic, \
    WrfHydroTs

class Domain(object):
    """Class for a WRF-Hydro domain, which consitutes all domain-specific files needed for a
    setup.
    """

    def __init__(self,
                 domain_top_dir: str,
                 domain_config: str,
                 model_version: str,
                 namelist_patch_file: str = 'namelist_patches.json'
                 ):
        """Instantiate a Domain object
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
        self.namelist_patches = json.load(self.namelist_patch_file.open(mode='r'))
        """dict: Domain-specific namelist settings."""

        self.model_version = model_version
        """str: Specified source-code version for which the domain is to be used."""

        self.domain_config = domain_config
        """str: Specified configuration for which the domain is to be used, e.g. 'NWM'"""
        self.hydro_files = list()
        """list: Files specified in hydro_nlist section of the domain namelist patches"""
        self.nudging_files = list()
        """list: Files specified in nudging_nlist section of the domain namelist patches"""
        self.lsm_files = list()
        """list: Files specified in noahlsm_offline section of the domain namelist patches"""
        ###

        self.namelist_patches = self.namelist_patches[self.model_version][self.domain_config]

        # Create file paths from hydro namelist
        domain_hydro_nlist = self.namelist_patches['hydro_namelist']['hydro_nlist']

        for key, value in domain_hydro_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.hydro_files.append(WrfHydroStatic(file_path))
                else:
                    self.hydro_files.append(file_path)

        # Create file paths from nudging namelist
        domain_nudging_nlist = self.namelist_patches['hydro_namelist']['nudging_nlist']

        for key, value in domain_nudging_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.nudging_files.append(WrfHydroStatic(file_path))
                else:
                    self.nudging_files.append(file_path)

        # Create symlinks from lsm namelist
        domain_lsm_nlist = \
            self.namelist_patches['namelist_hrldas']["noahlsm_offline"]

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

    def copy_files(self,dest_dir: str,symlink: bool=True):
        """Copy domain files to new directory
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
            shutil.copy(str(from_dir),str(to_dir))

        # create DOMAIN directory and symlink in files
        # Symlink in hydro_files
        for from_path in self.hydro_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = from_path.relative_to(self.domain_top_dir)
            to_path = dest_dir.joinpath(relative_path)
            if to_path.parent.is_dir() is False:
                to_path.parent.mkdir(parents=True)
            if symlink:
                to_path.symlink_to(from_path)
            else:
                shutil.copy(str(from_path),str(to_path))

        # Symlink in nudging files
        for from_path in self.nudging_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = from_path.relative_to(self.domain_top_dir)
            to_path = dest_dir.joinpath(relative_path)
            if to_path.parent.is_dir() is False:
                to_path.parent.mkdir(parents=True)
            if symlink:
                to_path.symlink_to(from_path)
            else:
                shutil.copy(str(from_path),str(to_path))

        # Symlink in lsm files
        for from_path in self.lsm_files:
            # Get new file path for run directory, relative to the top-level domain directory
            # This is needed to ensure the path matches the domain namelist
            relative_path = from_path.relative_to(self.domain_top_dir)
            to_path = dest_dir.joinpath(relative_path)
            if to_path.parent.is_dir() is False:
                to_path.parent.mkdir(parents=True)
            if symlink:
                to_path.symlink_to(from_path)
            else:
                shutil.copy(str(from_path),str(to_path))

        # Restart files are symlinked in to the run dir at run init.
        model_files = [*self.hydro_files,
                       *self.nudging_files,
                       *self.lsm_files]
        for ff in model_files:
            if re.match('.*/RESTART/.*', str(ff)):
                to_path = dest_dir.joinpath(ff.name).absolute()
                if symlink:
                    to_path.symlink_to(ff)
                else:
                    shutil.copy(str(ff),str(to_path))
            if re.match('.*/nudgingLastObs/.*', str(ff)):
                to_path = dest_dir.joinpath(ff.name).absolute()
                if symlink:
                    to_path.symlink_to(ff)
                else:
                    shutil.copy(str(ff),str(to_path))