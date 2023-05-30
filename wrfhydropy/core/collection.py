import collections
import dask
import dask.bag
from datetime import datetime
import itertools
from multiprocessing.pool import Pool
import numpy as np
import pathlib
from wrfhydropy.core.ioutils import timesince
import xarray as xr


def is_not_none(x):
    return x is not None


def group_lead_time(ds: xr.Dataset) -> int:
    return ds.lead_time.item(0)


def group_member_lead_time(ds: xr.Dataset) -> str:
    return str(ds.member.item(0)) + '-' + str(ds.lead_time.item(0))


def group_member(ds: xr.Dataset) -> int:
    return ds.member.item(0)


def group_identity(ds: xr.Dataset) -> int:
    return 1


def merge_reference_time(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='reference_time', coords='minimal')


def merge_member(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='member', coords='minimal')


def merge_lead_time(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='lead_time', coords='minimal')


def merge_time(ds_list: list) -> xr.Dataset:
    return xr.concat(ds_list, dim='time', coords='minimal')


def preprocess_whp_data(
    path,
    isel: dict = None,
    drop_variables: list = None
) -> xr.Dataset:
    try:
        ds = xr.open_dataset(path)
    except OSError:
        print("Skipping file, unable to open: ", path)
        return None

    if drop_variables is not None:
        to_drop = set(ds.variables).intersection(set(drop_variables))
        if to_drop != set():
            ds = ds.drop_vars(to_drop)

    # Exception for RESTART.YYMMDDHHMM_DOMAIN1 files
    if 'RESTART.' in str(path):
        time = datetime.strptime(ds.Times.values[0].decode('utf-8'), '%Y-%m-%d_%H:%M:%S')
        ds = ds.squeeze('Time')
        ds = ds.drop_vars(['Times'])
        ds = ds.assign_coords(time=time)

    # Exception for HYDRO_RST.YY-MM-DD_HH:MM:SS_DOMAIN1 files
    if 'HYDRO_RST.' in str(path):
        time = datetime.strptime(ds.attrs['Restart_Time'], '%Y-%m-%d_%H:%M:%S')
        ds = ds.assign_coords(time=time)

    filename_parent = pathlib.Path(path).parent
    filename_grandparent = pathlib.Path(path).parent.parent

    # Member preprocess
    # Assumption is that parent dir is member_mmm
    # member = None
    if 'member' in filename_parent.name:
        # This is a double check that this convention is because of wrf_hydro_py
        assert filename_parent.parent.joinpath('WrfHydroEns.pkl').exists()
        member = int(filename_parent.name.split('_')[-1])
        ds.coords['member'] = member

    # Lead time preprocess
    # Assumption is that parent dir is cast_yymmddHH
    if 'cast_' in filename_parent.name or 'cast_' in filename_grandparent.name:
        # Exception for cast HYDRO_RST.YY-MM-DD_HH:MM:SS_DOMAIN1 and
        # RESTART.YYMMDDHHMM_DOMAIN1 files
        if 'HYDRO_RST.' in str(path) or 'RESTART' in str(path):
            cast_fmt = 'cast_%Y%m%d%H'
            if 'cast_' in filename_parent.name:
                # This is a double check that this convention is because of wrf_hydro_py
                assert filename_parent.parent.joinpath('WrfHydroCycle.pkl').exists()
                ds.coords['reference_time'] = datetime.strptime(filename_parent.name, cast_fmt)
            elif 'cast_' in filename_grandparent.name:
                # This is a double check that this convention is because of wrf_hydro_py
                assert filename_grandparent.parent.joinpath('WrfHydroCycle.pkl').exists()
                ds.coords['reference_time'] = \
                    datetime.strptime(filename_grandparent.name, cast_fmt)
        ds.coords['lead_time'] = np.array(
            ds.time.values - ds.reference_time.values,
            dtype='timedelta64[ns]'
        )
        ds = ds.drop_vars('time')

        # Could create a valid time variable here, but I'm guessing it's more efficient
        # after all the data are collected.
        # ds['valid_time'] = np.datetime64(int(ds.lead_time) + int(ds.reference_time), 'ns')

    else:
        if 'reference_time' in ds.variables:
            ds = ds.drop_vars('reference_time')

    # Spatial subsetting
    if isel is not None:
        ds = ds.isel(isel)

    return ds


def open_whp_dataset_inner(
    paths: list,
    chunks: dict = None,
    attrs_keep: list = ['featureType', 'proj4',
                        'station_dimension', 'esri_pe_string',
                        'Conventions', 'model_version'],
    isel: dict = None,
    drop_variables: list = None,
    npartitions: int = None,
    profile: int = False
) -> xr.Dataset:

    if profile:
        then = timesince()

    # This is totally arbitrary be seems to work ok.
    # if npartitions is None:
    #     npartitions = dask.config.get('pool')._processes * 4
    # This choice does not seem to work well or at all, error?
    # npartitions = len(sorted(paths))
    paths_bag = dask.bag.from_sequence(paths, npartitions=npartitions)

    if profile:
        then = timesince(then)
        print('after paths_bag')

    ds_list = paths_bag.map(
        preprocess_whp_data,
        isel=isel,
        drop_variables=drop_variables
    ).filter(is_not_none).compute()

    if len(ds_list) == 0:
        return None

    if profile:
        then = timesince(then)
        print("after ds_list preprocess/filter")

    # Group by and merge by choices
    have_members = 'member' in ds_list[0].coords
    have_lead_time = 'lead_time' in ds_list[0].coords
    if have_lead_time:
        if have_members:
            group_list = [group_member_lead_time, group_lead_time]
            merge_list = [merge_reference_time, merge_member]
        else:
            group_list = [group_lead_time]
            merge_list = [merge_reference_time]
    else:
        if have_members:
            group_list = [group_member]
            merge_list = [merge_time]
        else:
            group_list = [group_identity]
            merge_list = [merge_time]

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

    if have_lead_time:
        nwm_dataset = merge_lead_time(ds_list)
    elif have_members:
        nwm_dataset = merge_member(ds_list)
    else:
        nwm_dataset = ds_list[0]

    del ds_list

    # Impose some order.
    if have_members:
        nwm_dataset = nwm_dataset.sortby(['member'])
    if have_lead_time:
        nwm_dataset = nwm_dataset.sortby(['reference_time', 'lead_time'])

    # Create a valid_time variable. I'm estimating that doing it here is more efficient
    # than adding more data to the collection processes.
    def calc_valid_time(ref, lead):
        return np.datetime64(int(ref) + int(lead), 'ns')
    if have_lead_time:
        nwm_dataset['valid_time'] = xr.apply_ufunc(
            calc_valid_time,
            nwm_dataset['reference_time'],
            nwm_dataset['lead_time'],
            vectorize=True
        ).transpose()  # Not sure this is consistently anti-transposed.

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

    # I submitted a PR fix to xarray.
    # I will leave this here until the PR is merged.
    # Workaround/prevent https://github.com/pydata/xarray/issues/1849
    # for v in nwm_dataset.variables.values():
    #     try:
    #         del v.encoding["contiguous"]
    #     except KeyError: # no problem
    #         pass

    return nwm_dataset


def open_whp_dataset_orig(
    paths: list,
    chunks: dict = None,
    attrs_keep: list = ['featureType', 'proj4',
                        'station_dimension', 'esri_pe_string',
                        'Conventions', 'model_version'],
    isel: dict = None,
    drop_variables: list = None,
    npartitions: int = None,
    profile: int = False,
    n_cores: int = 1
) -> xr.Dataset:

    import sys
    import os

    # print('n_cores', str(n_cores))
    the_pool = Pool(n_cores)
    with dask.config.set(scheduler='processes', pool=the_pool):
        whp_ds = open_whp_dataset_inner(
            paths,
            chunks,
            attrs_keep,
            isel,
            drop_variables,
            npartitions,
            profile
        )
    the_pool.close()
    return whp_ds


def open_whp_dataset(
    paths: list,
    file_chunk_size: int = None,
    chunks: dict = None,
    attrs_keep: list = ['featureType', 'proj4',
                        'station_dimension', 'esri_pe_string',
                        'Conventions', 'model_version'],
    isel: dict = None,
    drop_variables: list = None,
    npartitions: int = None,
    profile: int = False,
    n_cores: int = 1,
    write_cumulative_file: pathlib.Path = None
) -> xr.Dataset:

    import sys
    import os
    import math
    import multiprocessing
    import pickle

    n_files = len(paths)
    print('n_files', str(n_files))

    # remove path to file if file doesn't exist
    for p in paths:
        if (os.path.exists(p) == False):
            print("removing file since it doesn't exist:", str(p))
            paths.remove(p)

    if file_chunk_size is None:
        file_chunk_size = n_files

    if file_chunk_size >= n_files:
        the_pool = Pool(n_cores)
        with dask.config.set(scheduler='processes', pool=the_pool):
            whp_ds = open_whp_dataset_inner(
                paths=paths,
                chunks=chunks,
                attrs_keep=attrs_keep,
                isel=isel,
                drop_variables=drop_variables,
                npartitions=npartitions,
                profile=profile
            )
        the_pool.close()

    else:

        n_file_chunks = math.ceil(n_files / file_chunk_size)
        start_list = [file_chunk_size * ii for ii in range(n_file_chunks)]
        end_list = [file_chunk_size * (ii + 1) - 1 for ii in range(n_file_chunks)]
#        adsf

        whp_ds = None
        for start_ind, end_ind in zip(start_list, end_list):
            the_pool = Pool(n_cores)
            with dask.config.set(scheduler='processes', pool=the_pool):
                ds_chunk = open_whp_dataset_inner(
                    paths=paths[start_ind:(end_ind+1)],
                    chunks=chunks,
                    attrs_keep=attrs_keep,
                    isel=isel,
                    drop_variables=drop_variables,
                    npartitions=npartitions,
                    profile=profile
                )
            the_pool.close()

            if ds_chunk is not None:
                if whp_ds is None:
                    whp_ds = ds_chunk
                else:
                    whp_ds = xr.merge([whp_ds, ds_chunk])
                if write_cumulative_file is not None:
                    if not write_cumulative_file.parent.exists():
                        write_cumulative_file.parent.mkdir()
                    whp_ds.to_netcdf(write_cumulative_file)
                    cumulative_files_file = write_cumulative_file.parent / (
                        write_cumulative_file.stem + '.files.pkl')
                    pickle.dump(
                        paths[0:end_ind],
                        open(str(cumulative_files_file), 'wb'))

    return whp_ds
