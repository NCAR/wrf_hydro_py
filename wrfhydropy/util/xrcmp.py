#!/usr/bin/env python

# ipython --pdb  xrcmp.py -- \
#     --candidate /glade/scratch/rcabell/testing/take_test_CONUS_subsurface_candidate/nwm_ana/run_candidate/201806012300.RTOUT_DOMAIN1 \
#     --reference /glade/scratch/rcabell/testing/take_test_CONUS_subsurface_candidate/nwm_ana/run_candidate/201806020000.RTOUT_DOMAIN1 \
#     --log_file /glade/u/home/jamesmcc/Downloads/log.txt

import pathlib
import xarray as xr
import sys

def main(
    can_file: str,
    ref_file: str,
    log_file: str,
    n_cores: int = 1
) -> int:
    
    can_ds = xr.open_dataset(can_file)
    ref_ds = xr.open_dataset(ref_file)
    result = can_ds.equals(ref_ds)
    print(result)
    return(int(not result))


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--candidate", metavar="FILE", type=str, required=True,
        help="Candidate file to compare."
    )
    parser.add_argument(
        "--reference", metavar="FILE", type=str, required=True,
        help="Reference file to compare."
    )
    parser.add_argument(
        "--log_file", metavar="FILE", type=str, required=True,
        help="File to log potential differences to. "
        "Existing file is clobbered."
    )
    args = parser.parse_args()
    can_file = args.candidate
    ref_file = args.reference
    log_file = args.log_file

    ret = main(can_file=can_file, ref_file=ref_file, log_file=log_file)

    sys.exit()
