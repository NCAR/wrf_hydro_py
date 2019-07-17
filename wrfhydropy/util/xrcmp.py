#!/usr/bin/env python3

# Example Usage
# ipython --pdb  xrcmp.py -- \
#     --candidate conus_test/201806012300.RTOUT_DOMAIN1 \
#     --reference conus_test/201806020000.RTOUT_DOMAIN1 \
#     --log_file log.txt
import math
from math import log, ceil, sqrt
import pathlib
import sys
# import time
import xarray as xr


# # A decorator/closure to check timings.
# def stopwatch(the_func):
#     def the_closure(*args, **kw):
#         ts = time.time()
#         result = the_func(*args, **kw)
#         te = time.time()
#         print('Timing: ' + the_func.__name__ + ' took ', round(te - ts, 2),' seconds.')
#         return result
#     return the_closure


# @stopwatch
def xrcmp(
    can_file: str,
    ref_file: str,
    log_file: str,
    n_cores: int = 1,
    exclude_vars: list = []
) -> int:

    # Delete log file first
    # Should write a log file that says nothing yet determined?
    log_file = pathlib.Path(log_file)
    if log_file.exists():
        log_file.unlink()

    can_ds = xr.open_dataset(can_file, mask_and_scale=False)
    ref_ds = xr.open_dataset(ref_file, mask_and_scale=False)

    # May need to check that they have the same vars.
    can_vars = set([kk for kk in can_ds.variables.keys()])
    ref_vars = set([kk for kk in ref_ds.variables.keys()])
    have_same_variables = can_vars.difference(ref_vars) == set([])

    # TODO: Check that the meta data matches

    # This is quick if not true
    # ds_equal = can_ds.equals(re_ds)
    #if not ds_equal:

    all_stats = {}
    for key, val in can_ds.items():

        # ignore excluded vars
        if exclude_vars is None:
            exclude_vars = []
        if key in exclude_vars:
            continue

        # Check for variables in reference and not in candidate?
        # Check for variables in candidate and not in reference?
        print(key)

        if not can_ds[key].equals(ref_ds[key]):

            cc = can_ds[key]
            rr = ref_ds[key]
            # rr['time'] = cc.time ## THIS NEEDS REMOVED AFTER TESTING IS COMPLETE
            diff_xr = cc - rr

            # This threshold should be type dependent
            nz_xr = diff_xr.where(abs(diff_xr) > 0.0000000000000, drop=True)

            the_count = nz_xr.count().item()
            the_sum = nz_xr.sum().item()
            the_min = nz_xr.min().item()
            the_max = nz_xr.max().item()
            the_range = the_max - the_min
            the_mean = the_sum / the_count
            the_z = (nz_xr - the_mean)
            the_std = sqrt((the_z * the_z).sum() / the_count)

            all_stats[key] = {
                'Variable': key,
                'Count': the_count,
                'Sum': the_sum,
                'Min': the_min,
                'Max': the_max,
                'Range': the_range,
                'Mean':  the_mean,
                'StdDev': the_std
            }

    diff_var_names = sorted(all_stats.keys())
    if not diff_var_names:
        with open(log_file, 'w') as opened_file:
            opened_file.write("Files are identical\n")
        return 0

    # Formatting:

    # The goal is to print something like this which is what nccmp outputs.
    # channel_rt
    #      Variable Group  Count       Sum  ...       Max     Range      Mean    StdDev
    # 0  streamflow     /    162  0.003022  ...  0.003832  0.004315  0.000019  0.000361
    # 1       nudge     /      4 -0.001094  ...  0.000093  0.001272 -0.000274  0.000605
    # 2   q_lateral     /    170  0.000345  ...  0.000700  0.001145  0.000002  0.000086
    # 3    velocity     /    165  0.010788  ...  0.005488  0.006231  0.000065  0.000503
    # 4        Head     /    177  0.002717  ...  0.002662  0.003292  0.000015  0.000258
    
    stat_names = sorted(all_stats[diff_var_names[0]].keys())
    stat_lens = {}  # the length/width of each column/stat
    n_dec = 3  # number of decimals for floats
    n_dec_p = n_dec + 1  # plus the decimal point

    # The format for each type, where full_len sepcifices the width of the field.
    type_fmt = {
        str: '{{:{full_len}}}',
        int: '{{:{full_len}}}',
        float: '{{:{full_len}.' + str(n_dec) + 'f}}'
    }

    # Now solve the full_len field widths for all stats. Do this by
    # just formatting each as it's type and finding the max (best way
    # to handle negatives). For floats, take the integer part to find
    # its length to the left of the decimal.
    for stat_name in stat_names:
        all_lens = []
        for key, val in all_stats.items():
            the_val = val[stat_name]
            the_type = type(the_val)
            the_fmt0 = type_fmt[the_type]
            if the_type is str:
                full_len = len(the_val)
            elif not math.isfinite(the_val):
                full_len = len(str(the_val))
            else:
                full_len = len(str(int(the_val)))
                if the_type is float:
                    full_len = full_len + n_dec_p
            the_fmt = the_fmt0.format(**{'full_len': full_len})
            the_string = the_fmt.format(*[the_val])
            all_lens.append(len(the_string))

        stat_lens[stat_name] = max(all_lens)

    header_string = (
        '{Variable:>' + str(stat_lens['Variable']) + '}  '
        '{Count:>' + str(stat_lens['Count']) + '}  '
        '{Sum:>' + str(stat_lens['Sum']) + '}  '
        '{Min:>' + str(stat_lens['Min']) + '}  '
        '{Max:>' + str(stat_lens['Max']) + '}  '
        '{Range:>' + str(stat_lens['Range']) + '}  '
        '{Mean:>' + str(stat_lens['Mean']) + '}  '
        '{StdDev:>' + str(stat_lens['StdDev']) + '}  \n'
    )

    var_string = (
        '{Variable:>' + str(stat_lens['Variable']) + '}  '
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
        for key in all_stats.keys():
            opened_file.write(var_string.format(**all_stats[key]))

    return 1

def parse_arguments():

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

    return can_file, ref_file, log_file


if __name__ == "__main__":

    can_file, ref_file, log_file = parse_arguments()
    ret = xrcmp(can_file=can_file, ref_file=ref_file, log_file=log_file)
    sys.exit(ret)
