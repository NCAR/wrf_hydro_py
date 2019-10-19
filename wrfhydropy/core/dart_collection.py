import collections
import dask
import gc
import itertools
import math
import multiprocessing
import os
import pathlib
import pickle
import sys
import time
import xarray as xr

from .ioutils import is_not_none, group_member, merge_time, merge_member


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
            ds = ds.drop(to_drop)

    # This member definition is different from preprocess_nwm_data
    member = int(ds.attrs['DART_file_information'].split()[-1])
    ds.coords['member'] = member

    # Spatial subsetting
    if spatial_indices is not None:
        ds = ds.isel(feature_id=spatial_indices)

    # Chunk here?

    return ds


def open_dart_dataset_inner(
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


def open_dart_dataset(
    paths: list,
    file_chunk_size: int = 1200,
    chunks: dict = None,
    isel: dict = None,
    drop_variables: list = None,
    npartitions: int = None,
    attrs_keep: list = None,
    n_cores: int = 1,
    write_cumulative_file: pathlib.Path = None
) -> xr.Dataset:

    n_files = len(paths)
    print('n_files', str(n_files))
    print('')
    
    if file_chunk_size is None:
        file_chunk_size = n_files

    if file_chunk_size >= n_files:
        the_pool = multiprocessing.Pool(n_cores)
        with dask.config.set(scheduler='processes', pool=the_pool):
            whp_ds = open_dart_dataset_inner(
                paths=paths,
                chunks=chunks,
                attrs_keep=attrs_keep,
                spatial_indices=isel,
                drop_variables=drop_variables,
                npartitions=npartitions
            )
        the_pool.close()
        return whp_ds

    else:

        n_file_chunks = math.ceil(n_files / file_chunk_size)
        start_list = [file_chunk_size * ii for ii in range(n_file_chunks)]
        end_list = [min(file_chunk_size * (ii + 1), n_files)
                    for ii in range(n_file_chunks)]
        cumulative_files_file = write_cumulative_file.parent / (
            write_cumulative_file.stem + '.files.pkl')
        
        whp_ds = None
        for start_ind, end_ind in zip(start_list, end_list):

            print('start_ind: ', start_ind)
            print('end_ind: ', end_ind)

            loop_start_time = time.time()

            path_chunk = paths[start_ind:end_ind]
            if write_cumulative_file.exists():
                cumulative_files = pickle.load(
                    open(cumulative_files_file, 'rb'))
                path_chunk = set(set(path_chunk) - set(cumulative_files))
                if len(path_chunk) is 0:
                    print('files in loop already processed... ')
                    print('loop took: ', time.time() - loop_start_time)
                    print('')
                    continue
            
            if len(path_chunk) != end_ind - start_ind: 
                print('Some but not all files previously processed in this chunk.')
            
            the_pool = multiprocessing.Pool(n_cores)
            with dask.config.set(scheduler='distributed', pool=the_pool):
                ds_chunk = open_dart_dataset_inner(
                    paths=path_chunk,
                    chunks=chunks,
                    attrs_keep=attrs_keep,
                    spatial_indices=isel,
                    drop_variables=drop_variables,
                    npartitions=npartitions
                )
            the_pool.close()

            if ds_chunk is not None:
                if not write_cumulative_file.exists():
                    if not write_cumulative_file.parent.exists():
                        write_cumulative_file.parent.mkdir()
                    ds_chunk.to_netcdf(write_cumulative_file)
                    whp_ds = ds_chunk
                else:
                    backup_file = write_cumulative_file.with_suffix('.prev')
                    write_cumulative_file.replace(backup_file)
                    cumulative_ds = xr.open_dataset(backup_file)
                    whp_ds = xr.merge([cumulative_ds, ds_chunk])
                    whp_ds.to_netcdf(write_cumulative_file)
                    backup_file.unlink()
                    cumulative_ds.close()
                    del cumulative_ds

                whp_ds.close()
                del whp_ds
                ds_chunk.close()
                del ds_chunk
                gc.collect()
                
                pickle.dump(
                    paths[0:end_ind],
                    open(str(cumulative_files_file), 'wb'))

            print('wrote: ')
            print('       ', write_cumulative_file)
            print('       ', cumulative_files_file)
            print('loop took: ', time.time() - loop_start_time)
            print('')

        return True
