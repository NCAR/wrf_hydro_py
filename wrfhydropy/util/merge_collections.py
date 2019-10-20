#!/usr/bin/env python

import argparse
import pathlib
import sys
import xarray as xr


def merge_collections(file_a, file_b, out_file):
    ds = xr.merge([xr.open_dataset(file) for file in [file_a, file_b]])
    ds.to_netcdf(out_file)
    ds.close()
    del ds
    return(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script interface to open_dart_dataset_sub')
    requiredNamed = parser.add_argument_group('Required, named arguments')
    requiredNamed.add_argument(
        '--file_a',
        required=True,
        action='store',
        help='The path to the dart run_dir.'
    )
    requiredNamed.add_argument(
        '--file_b',
        required=True,
        action='store',
        help='The path to the dart run_dir.'
    )
    requiredNamed.add_argument(
        '--out_file',
        required=True,
        action='store',
        help='The path to the dart run_dir.'
    )    
    args = parser.parse_args()
    file_a = pathlib.Path(args.file_a)
    file_b = pathlib.Path(args.file_b)
    out_file = pathlib.Path(args.out_file)
    return_value = merge_collections(file_a, file_b, out_file)
    sys.exit(return_value)
