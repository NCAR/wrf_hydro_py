#!/usr/bin/env python

# ipython --pdb  xrcmp.py -- \
#     --candidate conus_test/201806012300.RTOUT_DOMAIN1 \
#     --reference conus_test/201806020000.RTOUT_DOMAIN1 \
#     --log_file log.txt

from math import log, ceil
import numpy as np
import pathlib
import scipy.stats as stats
import sys
import time
import xarray as xr

# A decorator/closure to check timings.
def stopwatch(the_func):
    def the_closure(*args, **kw):
        ts = time.time()
        result = the_func(*args, **kw)
        te = time.time()
        print('Timing: ' + the_func.__name__ + ' took ', round(te - ts, 2),' seconds.')
        return result
    return the_closure


# This renames and derives some variables.
def dr_to_dict(dr, var):
    return {
        'Variable': var,
        'Count': dr.nobs,
        'Sum': dr.mean * dr.nobs,
        'Min': dr.minmax[0],
        'Max': dr.minmax[-1],
        'Range': dr.minmax[-1] - dr.minmax[0],
        'Mean': dr.mean,
        'StdDev': dr.variance**(1.0/2.0)
    }


#@stopwatch
def main(
    can_file: str,
    ref_file: str,
    log_file: str,
    n_cores: int = 1
) -> int:

    # The goal is to print something like this which is what nccmp outputs.
    # channel_rt
    #      Variable Group  Count       Sum  ...       Max     Range      Mean    StdDev
    # 0  streamflow     /    162  0.003022  ...  0.003832  0.004315  0.000019  0.000361
    # 1       nudge     /      4 -0.001094  ...  0.000093  0.001272 -0.000274  0.000605
    # 2   q_lateral     /    170  0.000345  ...  0.000700  0.001145  0.000002  0.000086
    # 3    velocity     /    165  0.010788  ...  0.005488  0.006231  0.000065  0.000503
    # 4        Head     /    177  0.002717  ...  0.002662  0.003292  0.000015  0.000258

    # delete log file first
    # should write a log file that says nothing determined
    log_file = pathlib.Path(log_file)
    if log_file.exists():
        log_file.unlink()
    
    can_ds = xr.open_dataset(can_file)
    ref_ds = xr.open_dataset(ref_file)

    # May need to check that they have the same vars.
    can_vars = set([kk for kk in can_ds.variables.keys()])
    ref_vars = set([kk for kk in ref_ds.variables.keys()])
    have_same_variables = can_vars.difference(ref_vars) == set([])

    # Check that the meta data matches
    
    # This is quick if not true
    # ds_equal = can_ds.equals(re_ds)
    #if not ds_equal:
    
    all_stats = {}
    for key, val in can_ds.items():
        print(key)
        if not can_ds[key].equals(ref_ds[key]):
            diff = can_ds[key].values - ref_ds[key].values
            non_zeros = np.nonzero(diff)
            stats_desc = stats.describe(non_zeros, axis=None, nan_policy='omit'),
            all_stats[key] = dr_to_dict(stats_desc, key)
            del diff

    # Formatting: find the length of the fields to make fixed widths
    diff_var_names = sorted(all_stats.keys())
    if diff_var_names == []:
        return 0
    
    stat_names = sorted(all_stats[diff_var_names[0]].keys())
    stat_lens = {}
    n_dec = 3

    for stat_name in stat_names:
        # The strings are different than the numerics... have to name the string variables.
        if stat_name in ['Variable']:
            stat_lens[stat_name] = \
                max([len(val[stat_name]) for key, val in all_stats.items()])
        else:
            the_max = max([val[stat_name] for key, val in all_stats.items()])
            if the_max == 0:
                stat_lens[stat_name] = 1 + n_dec + 1
            else:
                stat_lens[stat_name] = ceil(log(abs(the_max), 10)) + n_dec + 1

    header_string = ('{Variable:>' + str(stat_lens['Variable']) + '}  '
                  '{Count:>' + str(stat_lens['Count']) + '}  '
                  '{Sum:>' + str(stat_lens['Sum']) + '}  '
                  '{Min:>' + str(stat_lens['Min']) + '}  '
                  '{Max:>' + str(stat_lens['Max']) + '}  '
                  '{Range:>' + str(stat_lens['Range']) + '}  '
                  '{Mean:>' + str(stat_lens['Mean']) + '}  '
                  '{StdDev:>' + str(stat_lens['StdDev']) + '}  \n'
    )

    var_string = ('{Variable:>' + str(stat_lens['Variable']) + '}  '
                  '{Count:>' + str(stat_lens['Count']) + '}  '
                  '{Sum:>' + str(stat_lens['Sum']) + '.' + str(n_dec) + 'f}  '
                  '{Min:>' + str(stat_lens['Min']) + '.' + str(n_dec) + 'f}  '
                  '{Max:>' + str(stat_lens['Max']) + '.' + str(n_dec) + 'f}  '
                  '{Range:>' + str(stat_lens['Range']) + '.' + str(n_dec) + 'f}  '
                  '{Mean:>' + str(stat_lens['Mean']) + '.' + str(n_dec) + 'f}  '
                  '{StdDev:>' + str(stat_lens['StdDev']) + '.' + str(n_dec) + 'f}  \n'
    )

    header_dict = {name:name for name in stat_names}
    the_header = header_string.format(**header_dict)

    with open(log_file, 'w') as opened_file:
        opened_file.write(the_header)
        for key in  all_stats.keys():
            opened_file.write(var_string.format(**all_stats[key]))

    return 1


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

    sys.exit(ret)
