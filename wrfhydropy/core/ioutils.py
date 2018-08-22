import io
import os
import pathlib
import shlex
import subprocess
import warnings
from typing import Union

import numpy as np
import pandas as pd
import xarray as xr
from boltons import iterutils


def open_nwmdataset(paths: list,
                    chunks: dict=None,
                    forecast: bool = True) -> xr.Dataset:
    """Open a multi-file wrf-hydro output dataset
    Args:
        paths: List ,iterable, or generator of file paths to wrf-hydro netcdf output files
        chunks: chunks argument passed on to xarray DataFrame.chunk() method
        forecast: If forecast the reference time dimension is retained, if not then
        reference_time dimension is set to a dummy value (1970-01-01) to ease concatenation
        and analysis
    Returns:
        An xarray dataset of dask arrays chunked by chunk_size along the feature_id
        dimension concatenated along the time and
        reference_time dimensions
    """

    # Create dictionary of forecasts, i.e. reference times
    ds_dict = dict()
    for a_file in paths:
        ds = xr.open_dataset(a_file,chunks=chunks)
        # Check if forecast and set reference_time to zero if not
        if not forecast:
            ds.coords['reference_time'].values = np.array(
                [np.datetime64('1970-01-01T00:00:00', 'ns')])

        ref_time = ds['reference_time'].values[0]
        if ref_time in ds_dict:
            # append the new number to the existing array at this slot
            ds_dict[ref_time].append(ds)
        else:
            # create a new array in this slot
            ds_dict[ref_time] = [ds]

    # Concatenate along time axis for each forecast
    forecast_list = list()
    for key in ds_dict.keys():
        forecast_list.append(xr.concat(ds_dict[key],
                                       dim='time',
                                       coords='minimal'))

    # Concatenate along reference_time axis for all forecasts
    nwm_dataset = xr.concat(forecast_list,
                            dim='reference_time',
                            coords='minimal')

    # Break into chunked dask array
    if chunks is not None:
       nwm_dataset = nwm_dataset.chunk(chunks=chunks)

    return nwm_dataset

class WrfHydroTs(list):
    """WRF-Hydro netcdf timeseries data class"""
    def open(self, chunks: dict = None):
        """Open a WrfHydroTs object
        Args:
            self
            chunks: chunks argument passed on to xarray.DataFrame.chunk() method
        Returns:
            An xarray mfdataset object concatenated on dimension 'Time'.
        """
        return open_nwmdataset(self, chunks=chunks)

    def check_nas(self):
        """Return dictionary of counts of NA values for each data variable summed across files"""
        nc_dataset = self.open()
        return check_file_nas(nc_dataset)

class WrfHydroStatic(pathlib.PosixPath):
    """WRF-Hydro static data class"""
    def open(self):
        """Open a WrfHydroStatic object
        Args:
            self
        Returns:
            An xarray dataset object.
        """
        return xr.open_dataset(self)

    def check_nas(self):
        """Return dictionary of counts of NA values for each data variable"""
        return check_file_nas(self)

def _check_file_exist_colon(dirpath: str, file_str: str):
    """Private method to check if a filename containing a colon exists, accounting for renaming
    to an underscore that is done by some systems.
    Args:
        dirpath: Path to directory containing files
        file_str: Name of file containing colons to search
    """
    if type(file_str) is not str:
        file_str = str(file_str)
    file_colon = pathlib.Path(file_str)
    file_no_colon = pathlib.Path(file_str.replace(':','_'))
    run_dir = pathlib.Path(dirpath)

    if (run_dir / file_colon).exists():
        return './' + str(file_colon)
    if (run_dir / file_no_colon).exists():
        return './' + str(file_no_colon)
    return None

def _touch(filename, mode=0o666, dir_fd=None, **kwargs):
    flags = os.O_CREAT | os.O_APPEND
    filename.open(mode='a+')
    with os.fdopen(os.open(str(filename), flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(f.fileno() if os.utime in os.supports_fd else filename,
                 dir_fd=None if os.supports_fd else dir_fd, **kwargs)

# TODO Refactor this to be a generic and not need both hydro and hrldas namelist to do a check
def check_input_files(hydro_namelist: dict,
                      hrldas_namelist: dict,
                      sim_dir: str,
                      ignore_restarts: bool = False):
    """Given hydro and hrldas namelists and a directory, check that all files listed in the
    namelist exist in the specified directory.
    Args:
        hydro_namelist: A wrfhydropy hydro_namelist dictionary
        hrldas_namelist: A wrfhydropy hydro_namelist dictionary
        file_str: A wrfhydropy hrldas_namelist dictionary
        sim_dir: The path to the directory containing input files.
        ignore_restarts: Ignore restart files.
    """

    def visit_is_file(path, key, value):
        if value is None:
            return False
        return type(value) is str or type(value) is dict

    def visit_not_none(path, key, value):
        return bool(value)

    def visit_str_posix_exists(path, key, value):
        if type(value) is dict:
            return True
        return key, (sim_dir / pathlib.PosixPath(value)).exists()

    def remap_nlst(nlst):
        # The outer remap removes empty dicts
        files = iterutils.remap(nlst,  visit=visit_is_file)
        files = iterutils.remap(files, visit=visit_not_none)
        exists = iterutils.remap(files, visit=visit_str_posix_exists)
        return exists

    hrldas_file_dict = remap_nlst(hrldas_namelist)
    hydro_file_dict = remap_nlst(hydro_namelist)

    # INDIR is a special case: do some regex magic and counting.

    # What are the colon cases? Hydro/nudging restart files
    hydro_file_dict['hydro_nlist']['restart_file'] = \
        bool(_check_file_exist_colon(sim_dir,
                                    hydro_namelist['hydro_nlist']['restart_file']))
    if 'nudging_nlist' in hydro_file_dict.keys():
        hydro_file_dict['nudging_nlist']['nudginglastobsfile'] = \
            bool(_check_file_exist_colon(sim_dir,
                                        hydro_namelist['nudging_nlist']['nudginglastobsfile']))

    hrldas_exempt_list = []
    hydro_exempt_list = ['nudginglastobsfile', 'timeslicepath']

    # Build conditional exemptions.
    if hydro_namelist['hydro_nlist']['udmp_opt'] == 0:
        hydro_exempt_list = hydro_exempt_list + ['udmap_file']

    if hrldas_namelist['wrf_hydro_offline']['forc_typ'] in [9,10]:
        hrldas_exempt_list = hrldas_exempt_list + ['restart_filename_requested']

    if ignore_restarts:
        hydro_exempt_list = hydro_exempt_list + ['restart_file']
        hydro_exempt_list = hydro_exempt_list + ['nudginglastobsfile']
        hrldas_exempt_list = hrldas_exempt_list + ['restart_filename_requested']

    def check_nlst(nlst, file_dict):

        # Scan the dicts for FALSE exempting certain ones for certain configs.
        def visit_missing_file(path, key, value):
            if type(value) is dict:
                return True
            if not value:
                message = 'The namelist file ' + key + ' = ' + \
                          str(iterutils.get_path(nlst, (path))[key]) + ' does not exist'
                if key not in [*hrldas_exempt_list, *hydro_exempt_list]:
                    raise ValueError(message)
            return False

        iterutils.remap(file_dict, visit=visit_missing_file)
        return None

    check_nlst(hrldas_namelist, hrldas_file_dict)
    check_nlst(hydro_namelist, hydro_file_dict)

    return None

def check_file_nas(dataset_path: Union[str,pathlib.Path]) -> str:
    """Opens the specified netcdf file and checks all data variables for NA values. NA assigned
    according to xarray __FillVal parsing. See xarray.Dataset documentation
    Args:
        dataset_path: The path to the netcdf dataset file
    Returns: string summary of nas if present
    """

    # Set filepath to strings
    dataset_path = str(dataset_path)

    # Make string to pass to subprocess, this compares the file against itself
    # nans will not equal each other so will report nans as fails
    command_str = 'nccmp --data --metadata --force ' + dataset_path + ' ' + dataset_path

    #Run the subprocess to call nccmp
    proc = subprocess.run(shlex.split(command_str),
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)

    #Check return code
    if proc.returncode != 0:
        # Get stoud into stringio object
        output = io.StringIO()
        output.write(proc.stderr.decode('utf-8'))
        output.seek(0)

        # Open stringio object as pandas dataframe
        try:
            nccmp_out = pd.read_table(output,delimiter=':',header=None)
            return nccmp_out
        except:
            warnings.warn('Problem reading nccmp output to pandas dataframe,'
                          'returning as subprocess object')
            return proc.stderr

def sort_files_by_time(file_list: list):
    """Given a list of file paths, sort list by file modified time
    Args:
        file_list: The list of file paths to sort
    Returns: A list of file paths sorted by file modified time
    """
    file_list_sorted = sorted(
        file_list,
        key=lambda file: file.stat().st_mtime_ns
    )

    return file_list_sorted
