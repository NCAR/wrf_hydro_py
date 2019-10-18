#!/usr/bin/env python

import argparse
import dask
from multiprocessing.pool import Pool
import os
import pathlib
import sys
import time
from typing import Union
import wrfhydropy
import xarray as xr


def collect_dart_output(
    run_dir: Union[pathlib.Path, str],
    out_dir: Union[pathlib.Path, str] = None,
    n_cores: int = 1,
    file_chunk_size: int = 1200,
    spatial_indices: list = None,
    drop_variables: list = None
):

    run_dir = pathlib.Path(run_dir)
    if out_dir is None:
        out_dir = run_dir
    out_dir = pathlib.Path(out_dir)
    if not out_dir.exists():
        os.mkdir(out_dir)
    
    t_start = time.time()

    # # -------------------------------------------------------
    # # 1. These files only concatenate along the time dimension.
    # #    We can use xr.open_mfdataset out of the box!
    # stage_list = ['input', 'preassim', 'analysis', 'output']

    # type_base_list = ['mean', 'sd', 'priorinf_mean', 'priorinf_sd', 'postinf_mean', 'postinf_sd']
    # domain_list = ['d01', 'd02']
    # type_list = type_base_list
    # for domain in domain_list:
    #     type_list = type_list + [(typ + '_' + domain) for typ in type_base_list]

    # for stage in stage_list:
    #     for typ in type_list:
    #         # Restrictive enough for the DART_cleanup *out* files.
    #         in_files = sorted((run_dir / 'output').glob('*/' + stage + '_' + typ + '.*[0-9].nc'))
    #         if len(in_files) == 0:
    #             continue

    #         out_file = out_dir / ('all_' + stage + '_' + typ + '.nc')
    #         # Do have to add the time dim to each variable to get the correct result.

    #         def preproc_time(ds):
    #             for key in ds.variables.keys():
    #                 if 'time' not in ds[key].dims:
    #                     ds[key] = ds[key].expand_dims('time')
    #             return ds

    #         the_pool = Pool(n_cores)
    #         with dask.config.set(scheduler='processes', pool=the_pool):
    #             ds = xr.open_mfdataset(in_files, parallel=True, preprocess=preproc_time)
    #         the_pool.close()
    #         # Feel like this should go in the above with/context. But it errors.
    #         # Xarray sets nan as the fill value when there is none. Dont allow that...
    #         for key, val in ds.variables.items():
    #             if '_FillValue' not in ds[key].encoding:
    #                 ds[key].encoding.update({'_FillValue': None})
    #         ds.to_netcdf(out_file)
    #         del ds

    # # # A check... to hold on to for a while.
    # # import xarray as xr
    # # import pathlib
    # # run_dir = pathlib.Path('/glade/scratch/jamesmcc/wrfhydro_dart/flo_cut/runs/bucket1')
    # # new_files = sorted(run_dir.glob("all*.nc"))
    # # for file in new_files:
    # #     old_file = run_dir / 'cleanup_dart' / file.name
    # #     print(old_file)
    # #     o = xr.open_dataset(old_file)
    # #     n = xr.open_dataset(file)
    # #     assert o.equals(n)
    # # Success.

    # -------------------------------------------------------
    # 2. Collect members. This replaces DART_cleanup_pack_members.csh and DART_cleanup.csh
    #    The explicit handling of individual members happens in wrfhydropy.open_dart_dataset
    stage_list = ['input', 'preassim', 'analysis', 'output']
    domain_list = ['', '_d0', '_d01', '_d02']

    for stage in stage_list:
        for domain in domain_list:

            # This is the correct glob
            # in_files = sorted((run_dir / 'output').glob('*/' + stage + '_' + typ + '.*.nc'))
            # This one is more restrictive b/c of the DART_cleanup *out* files.
            in_files = sorted(
                (run_dir / 'output').glob(
                    '*/' + stage + '_member_*' + domain + '.*[0-9].nc'
                )
            )
            if len(in_files) == 0:
                continue

            out_file = out_dir / ('all_' + stage + '_ensemble' + domain + '.nc')
            #the_pool = Pool(n_cores)
            #with dask.config.set(scheduler='processes', pool=the_pool):
            ds = wrfhydropy.open_dart_dataset(
                in_files,
                n_cores=n_cores,
                file_chunk_size=file_chunk_size,
                write_cumulative_file=out_file
            )
            #the_pool.close()

            ds.to_netcdf(out_file)
            ds.close()

    # -------------------------------------------------------
    # Wrap it up.
    t_end = time.time()
    print("Wrote collected output to : ", out_dir)
    print('DART data collection took: %2.4f sec' % (t_end-t_start))

    return(True)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Script interface to wrfhydropy.ioutils.open_nwm_dataset.'
    )
    requiredNamed = parser.add_argument_group('Required, named arguments')
    requiredNamed.add_argument(
        '--run_dir',
        required=True,
        action='store',
        help='The path to the dart run_dir.'
    )
    requiredNamed.add_argument(
        '--out_dir',
        required=True,
        action='store',
        help='The path to the dart run_dir.'
    )
    requiredNamed.add_argument(
        '--n_cores',
        required=True,
        action='store',
        help='The number of cores to use in the processing.'
    )
    parser.add_argument(
        '--file_chunk_size',
        action='store',
        default=None,
        help='Integer size of number of files to process simultaneously. Default = 1200.'
    )
    # Not sure these two will ever be used, but they are easy to leave until later.
    parser.add_argument(
        '--spatial_indices',
        action='store',
        default=None,
        help='A comma separated list of spatial/feature_id indices (quoted in the shell) or "None".'
    )
    parser.add_argument(
        '--drop_variables',
        action='store',
        default=None,
        help='A comma separated list of spatial/feature_id indices (quoted in the shell) or "None".'
    )
    args = parser.parse_args()

    run_dir = args.run_dir
    out_dir = args.out_dir
    n_cores = int(args.n_cores)
    file_chunk_size = args.file_chunk_size
    spatial_indices = args.spatial_indices
    drop_variables = args.drop_variables

    if file_chunk_size == 'None':
        file_chunk_size = 1200
    file_chunk_size = int(file_chunk_size)
        
    if spatial_indices == 'None':
        spatial_indices = None
    else:
        spatial_indices = [int(ind) for ind in spatial_indices.replace(" ", "").split(',')]

    if drop_variables == 'None':
        drop_variables = None
    else:
        drop_variables = [var for var in drop_variables.replace(" ", "").split(',')]

    print('file_chunk_size: ', file_chunk_size)
        
    success = collect_dart_output(
        run_dir=run_dir,
        out_dir=out_dir,
        n_cores=n_cores,
        file_chunk_size=file_chunk_size,
        spatial_indices=spatial_indices,
        drop_variables=drop_variables
    )

    # Shell translation.
    sys.exit(int(not success))
