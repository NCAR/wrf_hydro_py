#!/usr/bin/env python

import argparse
import dask
from multiprocessing.pool import Pool
import pathlib
import pickle
import sys
import time
from typing import Union
import wrfhydropy
import xarray as xr


def collect_dart_output_sub(pkl_file: pathlib.Path):
    piece_dict = pickle.load(open(pkl_file, 'rb'))
    # piece_dict = {
    #    piece_number:int,
    #    paths:paths,
    #    ncores_sub:int,
    #    isel:isel,
    #    drop_variables:drop_variables,
    #    chunks:chunks,
    #    npartitions:npartitions
    # }
    ds = wrfhydropy.open_dart_dataset(
        paths=piece_dict['paths'],
        file_chunk_size=len(piece_dict['paths']),
        isel=piece_dict['isel'],
        drop_variables=piece_dict['drop_variables'],
        chunks=piece_dict['chunks'],
        npartitions=piece_dict['npartitions'],
        n_cores=piece_dict['n_cores_inner'],
    )
    out_file = pkl_file.parent / (
        'piece_' + str(piece_dict['piece_number']) + '.nc')
    ds.to_netcdf(out_file)
    return(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script interface to open_dart_dataset_sub')
    requiredNamed = parser.add_argument_group('Required, named arguments')
    requiredNamed.add_argument(
        '--pkl_file',
        required=True,
        action='store',
        help='The path to the dart run_dir.'
    )
    args = parser.parse_args()
    pkl_file = pathlib.Path(args.pkl_file)
    return_value = collect_dart_output_sub(pkl_file)
    sys.exit(return_value)
