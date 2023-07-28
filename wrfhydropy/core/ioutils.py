from boltons import iterutils
from typing import Union

import collections
import dask
import dask.bag
import datetime
import hashlib
import io
import itertools
import numpy as np
import os
import pandas as pd
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import time
import warnings
import xarray as xr

from wrfhydropy.util.xrnan import xrnan


def is_not_none(x):
    return x is not None


def timesince(when=None):
    if when is None:
        return time.time()
    else:
        print(time.time() - when)
        sys.stdout.flush()
        sys.stderr.flush()
        return time.time()


def group_lead_time(ds: xr.Dataset) -> int:
    return ds.lead_time.item(0)


def group_member_lead_time(ds: xr.Dataset) -> int:
    return str(ds.member.item(0)) + '-' + str(ds.lead_time.item(0))


def group_member(ds: xr.Dataset) -> int:
    return ds.member.item(0)


def merge_reference_time(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='reference_time', coords='minimal')


def merge_member(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='member', coords='minimal')


def merge_lead_time(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='lead_time', coords='minimal')


def merge_time(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='time', coords='minimal')


def preprocess_nwm_data(
    path,
    spatial_indices: list = None,
    drop_variables: list = None
) -> xr.Dataset:

    try:
        ds = xr.open_dataset(path, mask_and_scale=False)
    except OSError:
        print("Skipping file, unable to open: ", path)
        return None

    if drop_variables is not None:
        to_drop = set(ds.variables).intersection(set(drop_variables))
        if to_drop != set():
            ds = ds.drop_vars(to_drop)

    # TODO JLM? Check range (e.g. "medium_range")
    # TODO JLM? Check file type (e.g "channel_rt")

    # Member preprocess
    filename_info = pathlib.Path(path).name.split('.')
    try:
        member = int(filename_info[3].split('_')[-1])
    except ValueError:
        member = None

    if member is not None:
        ds.coords['member'] = member

    # Lead time preprocess
    ds.coords['lead_time'] = np.array(
        ds.time.values - ds.reference_time.values,
        dtype='timedelta64[ns]'
    )
    ds = ds.drop_vars('time')

    # Spatial subsetting
    if spatial_indices is not None:
        ds = ds.isel(feature_id=spatial_indices)

    return ds


def open_nwm_dataset(
    paths: list,
    chunks: dict = None,
    attrs_keep: list = ['featureType', 'proj4',
                        'station_dimension', 'esri_pe_string',
                        'Conventions', 'model_version'],
    spatial_indices: list = None,
    drop_variables: list = None,
    npartitions: int = None,
    profile: int = False
) -> xr.Dataset:

    if profile:
        then = timesince()

    # This is totally arbitrary be seems to work ok.
    if npartitions is None:
        npartitions = dask.config.get('pool')._processes * 4
    # This choice does not seem to work well or at all, error?
    # npartitions = len(sorted(paths))
    paths_bag = dask.bag.from_sequence(paths, npartitions=npartitions)

    if profile:
        then = timesince(then)
        print('after paths_bag')

    ds_list = paths_bag.map(
        preprocess_nwm_data,
        chunks=chunks,
        spatial_indices=spatial_indices,
        drop_variables=drop_variables
    ).filter(is_not_none).compute()

    if profile:
        then = timesince(then)
        print("after ds_list preprocess/filter")

    # Group by and merge by choices
    have_members = 'member' in ds_list[0].coords
    if have_members:
        group_list = [group_member_lead_time, group_lead_time]
        merge_list = [merge_reference_time, merge_member]
    else:
        group_list = [group_lead_time]
        merge_list = [merge_reference_time]

    for group, merge in zip(group_list, merge_list):

        if profile:
            then = timesince(then)
            print('before sort')

        the_sort = sorted(ds_list, key=group)

        if profile:
            then = timesince(then)
            print('after sort, before group')

        ds_groups = [list(it) for k, it in itertools.groupby(the_sort, group)]

        if profile:
            then = timesince(then)
            print('after group, before merge')

        # npartitons = len(ds_groups)
        group_bag = dask.bag.from_sequence(ds_groups, npartitions=npartitions)
        ds_list = group_bag.map(merge).compute()

        if profile:
            then = timesince(then)
            print('after merge')

        del group_bag, ds_groups, the_sort

    nwm_dataset = merge_lead_time(ds_list)
    del ds_list

    # Create a valid_time variable.
    def calc_valid_time(ref, lead):
        return np.datetime64(int(ref) + int(lead), 'ns')
    nwm_dataset['valid_time'] = xr.apply_ufunc(
        calc_valid_time,
        nwm_dataset['reference_time'],
        nwm_dataset['lead_time'],
        vectorize=True
    )

    # Xarray sets nan as the fill value when there is none. Dont allow that...
    for key, val in nwm_dataset.variables.items():
        if '_FillValue' not in nwm_dataset[key].encoding:
            nwm_dataset[key].encoding.update({'_FillValue': None})

    # Clean up attributes
    new_attrs = collections.OrderedDict()
    if attrs_keep is not None:
        for key, value in nwm_dataset.attrs.items():
            if key in attrs_keep:
                new_attrs[key] = nwm_dataset.attrs[key]

    nwm_dataset.attrs = new_attrs

    # Break into chunked dask array
    if chunks is not None:
        nwm_dataset = nwm_dataset.chunk(chunks=chunks)

    return nwm_dataset


def preprocess_dart_data(
    path,
    chunks: dict = None,
    spatial_indices: list = None,
    drop_variables: list = None
) -> xr.Dataset:

    # This non-optional is different from preprocess_nwm_data
    # I kinda dont think this should be optional for dart experiment/run collection.
    # try:
    ds = xr.open_dataset(path)
    # except OSError:
    #    print("Skipping file, unable to open: ", path)
    #    return None

    # May need to add time... do this before changing any dimensions.
    for key in ds.variables.keys():
        if 'time' not in ds[key].dims:
            ds[key] = ds[key].expand_dims('time')

    if drop_variables is not None:
        to_drop = set(ds.variables).intersection(set(drop_variables))
        if to_drop != set():
            ds = ds.drop_vars(to_drop)

    # This member definition is different from preprocess_nwm_data
    member = int(ds.attrs['DART_file_information'].split()[-1])
    ds.coords['member'] = member

    # Spatial subsetting
    if spatial_indices is not None:
        ds = ds.isel(feature_id=spatial_indices)

    # Chunk here?

    return ds


def open_dart_dataset(
    paths: list,
    chunks: dict = None,
    spatial_indices: list = None,
    drop_variables: list = None,
    npartitions: int = None,
    attrs_keep: list = None
) -> xr.Dataset:
    """Open a multi-file ensemble wrf-hydro output dataset
    Args:
paths: List ,iterable, or generator of file paths to wrf-hydro netcdf output files
        chunks: chunks argument passed on to xarray DataFrame.chunk() method
        preprocess_member: A function that identifies the member from the file or filename.
        attrs_keep: A list of the global attributes to be retained.
    Returns:
        An xarray dataset of dask arrays chunked by chunk_size along the feature_id
        dimension concatenated along the time and member dimensions.
    """

    # TODO JLM: Can this be combined with open_wh_dataset?
    # Explanation:
    # Xarray currently first requires concatenation along existing dimensions (e.g. time)
    # over the individual member groups, then it allows concatenation along the member
    # dimensions.

    # Set partitions
    # This is arbitrary
    if npartitions is None:
        npartitions = dask.config.get('pool')._processes * 4

    paths_bag = dask.bag.from_sequence(paths)

    ds_list = paths_bag.map(
        preprocess_dart_data,
        chunks=chunks,
        spatial_indices=spatial_indices,
        drop_variables=drop_variables
    ).filter(is_not_none).compute()

    the_sort = sorted(ds_list, key=group_member)
    ds_groups = [list(it) for k, it in itertools.groupby(the_sort, group_member)]
    group_bag = dask.bag.from_sequence(ds_groups)  # , npartitions=npartitions)
    ds_list = group_bag.map(merge_time).compute()
    del group_bag, ds_groups, the_sort
    dart_dataset = merge_member(ds_list)
    del ds_list

    # Xarray sets nan as the fill value when there is none. Dont allow that...
    for key, val in dart_dataset.variables.items():
        if '_FillValue' not in dart_dataset[key].encoding:
            dart_dataset[key].encoding.update({'_FillValue': None})

    # Clean up attributes
    new_attrs = collections.OrderedDict()
    if attrs_keep is not None:
        for key, value in dart_dataset.attrs.items():
            if key in attrs_keep:
                new_attrs[key] = dart_dataset.attrs[key]

    dart_dataset.attrs = new_attrs

    # The existing DART convention.
    dart_dataset = dart_dataset.transpose('time', 'member', 'links')

    # Break into chunked dask array
    if chunks is not None:
        dart_dataset = dart_dataset.chunk(chunks=chunks)

    return dart_dataset


def open_wh_dataset(paths: list,
                    chunks: dict = None,
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
        ds = xr.open_dataset(a_file, chunks=chunks, mask_and_scale=False)
        # Check if forecast and set reference_time to zero if not
        if not forecast:
            ds.coords['reference_time'] = np.array(
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
    wh_dataset = xr.concat(
        forecast_list,
        dim='reference_time',
        coords='minimal'
    )

    # Break into chunked dask array
    if chunks is not None:
        wh_dataset = wh_dataset.chunk(chunks=chunks)

    return wh_dataset


def preprocess_dart_member(ds):
    member = int(ds.attrs['DART_file_information'].split()[-1])
    ds.coords['member'] = member
    return ds


def open_ensemble_dataset(
    paths: list,
    chunks: dict = None,
    preprocess_member: callable = preprocess_dart_member,
    attrs_keep: list = None
) -> xr.Dataset:
    """Open a multi-file ensemble wrf-hydro output dataset
    Args:
        paths: List ,iterable, or generator of file paths to wrf-hydro netcdf output files
        chunks: chunks argument passed on to xarray DataFrame.chunk() method
        preprocess_member: A function that identifies the member from the file or filename.
        attrs_keep: A list of the global attributes to be retained.
    Returns:
        An xarray dataset of dask arrays chunked by chunk_size along the feature_id
        dimension concatenated along the time and member dimensions.
    """

    # TODO JLM: Can this be combined with open_nwmdataset?
    # How can we differentiate between member and forecast, etc? provide as kw args?

    # Explanation:
    # Xarray currently first requires concatenation along existing dimensions (e.g. time)
    # over the individual member groups, then it allows concatenation along the member
    # dimensions. A dictionary is built wherein the member groups are identified/kept as
    # lists of data sets (per member). Once this dictionary is complete, each list in
    # the dict is concatenated along time. Once all members are concatenated along time,
    # the all the members can be concatenated along "member".

    paths_bag = dask.bag.from_sequence(paths)
    ds_all = paths_bag.map(xr.open_dataset, chunks=chunks, mask_and_scale=False).compute()
    all_bag = dask.bag.from_sequence(ds_all)

    def member_grouper(ds):
        return preprocess_member(ds).member.item(0)

    def concat_time(total, x):
        return xr.concat([total, x], dim='time', coords='minimal')

    # Foldby returns a tuple of (member_number, xarray.Dataset), strip off the member number.
    ds_members = [tup[1] for tup in all_bag.foldby(member_grouper, concat_time).compute()]
    del all_bag

    ens_dataset = xr.concat(ds_members, dim='member', coords='minimal')
    del ds_members

    # Xarray sets nan as the fill value.
    for key, val in ens_dataset.variables.items():
        ens_dataset[key].encoding.update({'_FillValue': None})

    new_attrs = collections.OrderedDict()
    if attrs_keep is not None:
        for key, value in ens_dataset.attrs.items():
            if key in attrs_keep:
                new_attrs[key] = ens_dataset.attrs[key]

    ens_dataset.attrs = new_attrs
    # ens_dataset = ens_dataset.transpose('time', 'member', 'links')

    # Break into chunked dask array
    if chunks is not None:
        ens_dataset = ens_dataset.chunk(chunks=chunks)

    return ens_dataset

# TODO JLM: deprecate?
class WrfHydroTs(list):
    """WRF-Hydro netcdf timeseries data class"""
    def open(self, chunks: dict = None, forecast: bool = True):
        """Open a WrfHydroTs object
        Args:
            self
            chunks: chunks argument passed on to xarray.DataFrame.chunk() method
            forecast: If forecast the reference time dimension is retained, if not then
            reference_time dimension is set to a dummy value (1970-01-01) to ease concatenation
            and analysis
        Returns:
            An xarray mfdataset object concatenated on dimension 'Time'.
        """
        return open_wh_dataset(self, chunks=chunks, forecast=forecast)

    def check_nans(self):
        """Return dictionary of counts of NA values for each data variable summed across files"""
        nc_dataset = self.open()
        return check_file_nans(nc_dataset)


# TODO JLM: deprecate?
class WrfHydroStatic(pathlib.PosixPath):
    """WRF-Hydro static data class"""
    def open(self):
        """Open a WrfHydroStatic object
        Args:
            self
        Returns:
            An xarray dataset object.
        """
        return xr.open_dataset(self, mask_and_scale=False)

    def check_nans(self):
        """Return dictionary of counts of NA values for each data variable"""
        return check_file_nans(self)


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
    file_no_colon = pathlib.Path(file_str.replace(':', '_'))
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
def check_input_files(
    hydro_namelist: dict,
    hrldas_namelist: dict,
    sim_dir: str,
    ignore_restarts: bool = False,
    check_nlst_warn: bool = False
):
    """Given hydro and hrldas namelists and a directory, check that all files listed in the
    namelist exist in the specified directory.
    Args:
        hydro_namelist: A wrfhydropy hydro_namelist dictionary
        hrldas_namelist: A wrfhydropy hydro_namelist dictionary
        file_str: A wrfhydropy hrldas_namelist dictionary
        sim_dir: The path to the directory containing input files.
        ignore_restarts: Ignore restart files.
        check_nlst_warn: Allow the namelist checking/validation to only result in warnings.
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
        files = iterutils.remap(nlst, visit=visit_is_file)
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

    if hrldas_namelist['wrf_hydro_offline']['forc_typ'] in [9, 10]:
        hrldas_exempt_list = hrldas_exempt_list + ['restart_filename_requested']

    if ignore_restarts:
        hydro_exempt_list = hydro_exempt_list + ['restart_file']
        hydro_exempt_list = hydro_exempt_list + ['nudginglastobsfile']
        hrldas_exempt_list = hrldas_exempt_list + ['restart_filename_requested']

    def check_nlst(
        nlst,
        file_dict,
        warn: bool = False
    ):
        """
        Check the paths in the namelist.
        Args:
            nlst: The namelist to check.
            file_dict: A dictionary of the files which are specified in nlst flaged True or
            False if they exist on disk.
            warn: Allow the namelist checking/validation to only result in warnings.
        """

        # Scan the dicts for FALSE exempting certain ones for certain configs.
        def visit_missing_file(path, key, value):
            if type(value) is dict:
                return True
            if not value:
                message = 'The namelist file ' + key + ' = ' + \
                          str(iterutils.get_path(nlst, (path))[key]) + ' does not exist'
                if key not in [*hrldas_exempt_list, *hydro_exempt_list]:
                    if warn:
                        warnings.warn(message)
                    else:
                        raise ValueError(message)
            return False

        iterutils.remap(file_dict, visit=visit_missing_file)
        return None

    check_nlst(hrldas_namelist, hrldas_file_dict, warn=check_nlst_warn)
    check_nlst(hydro_namelist, hydro_file_dict, warn=check_nlst_warn)

    return None


def check_file_nans(
    dataset_or_path: Union[str, pathlib.Path, xr.Dataset],
    n_cores: int = 1
) -> Union[pd.DataFrame, None]:
    """Opens the specified netcdf file and checks all data variables for NA values. NA assigned
    according to xarray __FillVal parsing. See xarray.Dataset documentation
    Args:
        dataset_or_path: The path to the netcdf dataset file, or a dataset itself
    Returns: string summary of nans if present
    """
    return xrnan(dataset_or_path, n_cores=n_cores)


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


def nwm_forcing_to_ldasin(
    nwm_forcing_dir: Union[pathlib.Path, str],
    ldasin_dir: Union[pathlib.Path, str],
    range: str,
    copy: bool = False,
    forc_type=1
):
    """Convert nwm dir and naming format to wrf-hydro read format.
    Args:
        nwm_forcing_dir: the pathlib.Path or str for the source dir or a list of source
            directories. If a pathlib.Path object or str is provided, it is assume that this
            single directory contains nwm.YYYYMMDDHH downloaded from NOMADS and that their
            subdirectory structure is unchanged. If a list of pathlib.Path (or str) is provided,
            these should be the desired nwm.YYYYMMDD to translate with no changed to their
            subdirectory structure.
        ldasin_dir: the pathlib.Path or str for a new NONEXISTANT output dir.
        range: str range as on nomads in: analysis_assim, analysis_assim_extend,
            analysis_assim_hawaii, medium_range, short_range,
            short_range_hawaii
        copy: True or false. Default is false creates symlinks.
        forc_type: 1 (hour) or 2 (minute) formats are supported.
    Returns:
        None on success.
"""

    # The proper range specification is as in args above, but if "forcing_" is
    # prepended, try our best.
    if 'forcing_' in range:
        range = range.split('forcing_')[1]

    # Ldasin dir
    # Might move this to the individual forecast folder creation below.
    if isinstance(ldasin_dir, str):
        ldasin_dir = pathlib.Path(ldasin_dir)
    if not ldasin_dir.exists():
        os.mkdir(str(ldasin_dir))

    if isinstance(nwm_forcing_dir, list):

        daily_dirs = [pathlib.Path(dd) for dd in nwm_forcing_dir]
        daily_exist = [dd.exists() for dd in daily_dirs]
        if not all(daily_exist):
            raise FileNotFoundError('Some requested daily nwm forcing source dirs do not exist')

    else:

        if isinstance(nwm_forcing_dir, str):
            nwm_forcing_dir = pathlib.Path(nwm_forcing_dir)
        if not nwm_forcing_dir.exists():
            raise FileNotFoundError("The nwm_forcing_dir does not exist, exiting.")
        os.chdir(str(nwm_forcing_dir))
        daily_dirs = sorted(nwm_forcing_dir.glob("nwm.*[0-9]"))
        if len(daily_dirs) == 0:
            warnings.warn(
                "No daily nwm.YYYYMMDD directores found in the supplied path, "
                "If you passed a daily directory, it must be conatined in a list."
            )

    for daily_dir in daily_dirs:
        the_day = datetime.datetime.strptime(daily_dir.name, 'nwm.%Y%m%d')

        if forc_type == 9 or forc_type == 10:
            member_dirs = sorted(daily_dir.glob(range))
        else:
            member_dirs = sorted(daily_dir.glob('forcing_' + range))

        for member_dir in member_dirs:
            if not member_dir.is_dir():
                continue

            re_range = range
            if '_hawaii' in range:
                re_range = range.split('_hawaii')[0]
            elif '_puertorico' in range:
                re_range = range.split('_puertorico')[0]

            if forc_type == 9 or forc_type == 10:
                forcing_files = member_dir.glob('*' + re_range + '.channel_rt.*')
            else:
                forcing_files = member_dir.glob('*' + re_range + '.forcing.*')

            for forcing_file in forcing_files:
                name_split = forcing_file.name.split('.')
                init_hour = int(re.findall(r'\d+', name_split[1])[0])

                # Each init time will have it's own directory.
                init_time = the_day + datetime.timedelta(hours=init_hour)
                init_time_dir = ldasin_dir / init_time.strftime('%Y%m%d%H')
                init_time_dir.mkdir(mode=0o777, parents=False, exist_ok=True)

                # Them each file inside has it's own time on the file.
                cast_hour = int(re.findall(r'\d+', name_split[4])[0])
                if 'analysis_assim' in range:
                    model_time = init_hour - cast_hour
                else:
                    model_time = init_hour + cast_hour

                ldasin_time = the_day + datetime.timedelta(hours=model_time)

                # Solve the forcing type/format
                if forc_type == 1:
                    fmt = '%Y%m%d%H.LDASIN_DOMAIN1'
                elif forc_type == 2:
                    fmt = '%Y%m%d%H00.LDASIN_DOMAIN1'
                elif forc_type == 9 or forc_type == 10:
                    fmt = '%Y%m%d%H00.CHRTOUT_DOMAIN1'
                else:
                    raise ValueError("Only forc_types 1, 2, 9, & 10 are supported.")
                ldasin_file_name = init_time_dir / ldasin_time.strftime(fmt)

                if copy:
                    shutil.copy(forcing_file, ldasin_file_name)
                else:
                    ldasin_file_name.symlink_to(forcing_file)

    return


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
