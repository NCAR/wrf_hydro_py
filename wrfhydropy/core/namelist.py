import copy
import deepdiff
import f90nml
import json
from typing import Union
import warnings


def load_namelist(nml_path: str) -> dict:
    """Load a F90 namelist into a wrfhydropy.Namelist object
        Args:
            nml_path: String containing path to F90 namelist
        Returns:
            dict interpretation of namelist
    """
    nml_dict = Namelist(json.loads(json.dumps(f90nml.read(nml_path), sort_keys=True)))
    return nml_dict


class JSONNamelist(object):
    """Class for a WRF-Hydro JSON namelist containing one more configurations"""
    def __init__(
            self,
            file_path: str):
        """Instantiate a Namelist object.
        Args:
            file_path: Path to the namelist file to open, can be a json or fortran90 namelist.
        """
        self._json_namelist = json.load(open(file_path, mode='r'))
        self.configs = self._json_namelist.keys()

    def get_config(self, config: str):
        """Get a namelist for a given configuration. This works internally by grabbing the base
        namelist and updating with the config-specific changes.
        Args:
            config: The configuration to retrieve
        """

        # This ifelse statement is to make the compile options files.
        # backwards-compatible. Should be left in through v2.1 (that makes sure v2.0 is covered).
        if 'base' in self._json_namelist.keys():
            base_namelist = copy.deepcopy(self._json_namelist['base'])
            config_patches = copy.deepcopy(self._json_namelist[config])
            # Update the base namelist with the config patches
            config_namelist = dict_merge(base_namelist, config_patches)

        else:
            # One can pass any "nwm_*" config to get the compile options.
            # if that specific config is not there, "nwm" config is used
            # for the compile options with a warning.
            if config not in self._json_namelist.keys():
                if 'nwm' in config and 'nwm' in self._json_namelist.keys():
                    config = 'nwm'
                    warnings.warn(
                        "The compile configuration 'nwm' is inferred from the"
                        " configuration passed: " + config)
            config_namelist = copy.deepcopy(self._json_namelist[config])

        return Namelist(config_namelist)


class Namelist(dict):
    """Class for a WRF-Hydro namelist"""

    def write(self, path: str, mode='x'):
        """Write a namelist to file as a fortran-compatible namelist
        Args:
            path: The file path
        """
        with open(str(path), mode=mode) as nml_file:
            f90nml.write(self, nml_file)

    def patch(self, patch: dict):
        """Recursively patch a namelist with key values from another namelist
        Args:
            patch: A Namelist or dict object containing the patches
        """
        patched_namelist = dict_merge(copy.deepcopy(self),
                                      copy.deepcopy(patch))
        return patched_namelist


def dict_merge(dct: dict, merge_dct: dict) -> dict:
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    Args:
     dct: dict onto which the merge is executed
     merge_dct: dct merged into dct
    Returns:
        The merged dict
    """

    for key, value in merge_dct.items():
        if key in dct.keys() and type(value) is dict:
            dict_merge(dct[key], merge_dct[key])
        else:
            dct[key] = merge_dct[key]

    return(dct)


def diff_namelist(
        old_namelist: Union[Namelist, str],
        new_namelist: Union[Namelist, str], **kwargs) -> dict:
    """Diff two Namelist objects or fortran 90 namelist files and return a dictionary of
    differences.

    Args:
        old_namelist: String containing path to the first namelist file, referred to as 'old' in
        outputs.
        new_namelist: String containing path to the second namelist file, referred to as 'new' in
        outputs.
        **kwargs: Additional arguments passed onto deepdiff.DeepDiff method
    Returns:
        The differences between the two namelists
    """

    # If supplied as strings try and read in from file path
    if type(old_namelist) == str:
        old_namelist = load_namelist(old_namelist)
    if type(new_namelist) == str:
        new_namelist = load_namelist(new_namelist)

    # Diff the namelists
    differences = deepdiff.DeepDiff(old_namelist, new_namelist, ignore_order=True, **kwargs)
    differences_dict = dict(differences)
    return (differences_dict)
