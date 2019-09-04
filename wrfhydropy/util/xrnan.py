import pathlib
import sys
from typing import Union
import xarray as xr


def xrnan(
    dataset_or_path: Union[str, pathlib.Path, xr.Dataset],
    log_file: str = None,
    exclude_vars: list = []
) -> int:
    # Set filepath to strings
    if not isinstance(dataset_or_path, xr.Dataset):
        ds = xr.open_dataset(str(dataset_or_path), mask_and_scale=False)
    else:
        ds = dataset_or_path

    if ds.isnull().any().to_array().data.any():
        print(str(dataset_or_path), "has NaNs")
        return ds.where(ds.isnull()).to_dataframe()
    else:
        return None


def parse_arguments():

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path", metavar="FILE", type=str, required=True,
        help="File to check for NaNs."
    )
    parser.add_argument(
        "--log_file", metavar="FILE", type=str, required=True,
        help="File to log potential differences to. "
        "Existing file is clobbered."
    )
    args = parser.parse_args()
    path = args.path
    log_file = args.log_file
    return path, log_file


if __name__ == "__main__":

    path, log_file = parse_arguments()
    ret = xrnan(path, log_file=log_file)
    if ret is None:
        exit_code = 0
    else:
        exit_code = 1
    sys.exit(exit_code)
