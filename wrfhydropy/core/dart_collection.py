import collections
import dask
import itertools
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
