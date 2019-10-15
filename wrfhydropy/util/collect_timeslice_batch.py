# This scrip collects individual casts from the ensemble forecasts and
# writes them to intermediate files to be collected by a second script.
import pathlib
import sys
import wrfhydropy


def collect_timeslice_batch(path, glob, full_gage_list_pkl, n_cores, out_file):
    import pickle 
    full_gage_list = pickle.load(open(full_gage_list_pkl, 'rb'))
    the_files = sorted(pathlib.Path(path).glob(glob))
    ds = wrfhydropy.open_timeslice_dataset(
        paths=the_files,
        full_gage_list=full_gage_list,
        n_cores=n_cores
    )              
    ds.to_netcdf(out_file)
    return 0


def collect_timeslice_batch_final(path, glob, out_file):
    import xarray as xr
    the_files = sorted(pathlib.Path(path).glob(glob))
    ds = xr.open_mfdataset(paths=the_files)
    ds.to_netcdf(out_file)
    for file in the_files:
        file.unlink()
    return 0


def parse_arguments():
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path", metavar="path", type=str, required=True,
        help="The pathlib.Path()"
    )
    parser.add_argument(
        "--glob", metavar="glob", type=str, required=True,
        help="The expression for path.glob() "
    )
    parser.add_argument(
        "--full_gage_list_pkl", metavar="file", type=str, required=True,
        help="The pkl file containing the full_gage_list"
    )
    parser.add_argument(
        "--n_cores", metavar="n_cores", type=int, required=True,
        help="The number of cores to use."
    )
    parser.add_argument(
        "--out_file", metavar="outputfile", type=str, required=True,
        help="The /path/file to write to"
    )
    parser.add_argument(
        "--final", metavar="outputfile", type=bool, required=False, default=False,
        help="Is this the final collect?"
    )

    args = parser.parse_args()
    return args.path, args.glob, \
        args.full_gage_list_pkl, args.n_cores, args.out_file, args.final


if __name__ == "__main__":

    path, glob, full_gage_list_pkl, n_cores, out_file, final = parse_arguments()
    if not final:
        ret = collect_timeslice_batch(path, glob, full_gage_list_pkl, n_cores, out_file)
    else:
        ret = collect_timeslice_batch_final(path, glob, out_file)
    sys.exit(ret)
