import deepdiff
import f90nml
import io
import os
import pandas as pd
import subprocess
import warnings
import xarray as xr
import pathlib
import numpy as np

def compare_nc_nccmp(candidate_nc: str,
                     reference_nc: str,
                     nccmp_options: list = ['--data','--metadata','--force','--quiet'],
                     exclude_vars: list = None):

    """Compare two netcdf files using nccmp
    Args:
        candidate_restart: The path for the candidate restart file
        ref_restarts: The path for the reference restart file
        nccmp_options: List of long-form command line options passed to nccmp,
        see http://nccmp.sourceforge.net/ for options
        exclude_vars: A list of strings containing variables names to
        exclude from the comparison
    Returns:
        Either a pandas dataframe if possible or subprocess object
    """
    #Try and set files to strings
    candidate_nc = str(candidate_nc)
    reference_nc = str(reference_nc)

    # Make list to pass to subprocess
    command_list=['nccmp']

    for item in nccmp_options:
        command_list.append(item)

    command_list.append('-S')

    if exclude_vars is not None:
        # Convert exclude_vars list into a comman separated string
        exclude_vars = ','.join(exclude_vars)
        #append
        command_list.append('-x ' + exclude_vars)

    command_list.append(candidate_nc)
    command_list.append(reference_nc)

    #Run the subprocess to call nccmp
    proc = subprocess.run(command_list,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)

    #Check return code
    if proc.returncode != 0:
        # Get stoud into stringio object
        output = io.StringIO()
        output.write(proc.stdout.decode('utf-8'))
        output.seek(0)

        # Open stringio object as pandas dataframe
        try:
            nccmp_out = pd.read_table(output,delim_whitespace=True,header=0)
            return nccmp_out
        except:
            warnings.warn('Probleming reading nccmp output to pandas dataframe,'
                 'returning as subprocess object')
            return proc


def compare_ncfiles(candidate_files: list,
                    reference_files: list,
                    nccmp_options: list = ['--data', '--metadata', '--force', '--quiet'],
                    exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                           'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt']):
    """Compare lists of netcdf restart files element-wise. Files must have common names
    Args:
        candidate_files: List of candidate restart file paths
        reference_files: List of reference restart file paths
        nccmp_options: List of long-form command line options passed to nccmp,
        see http://nccmp.sourceforge.net/ for options
        exclude_vars: A list of strings containing variables names to
        exclude from the comparison
    Returns:
        A named list of either pandas dataframes if possible or subprocess objects
    """

    ref_dir = reference_files[0].parent
    output_list = []
    for file_candidate in candidate_files:
        file_reference = ref_dir.joinpath(file_candidate.name)
        if file_reference.is_file():
            nccmp_out = compare_nc_nccmp(candidate_nc=file_candidate,
                                         reference_nc=file_reference,
                                         nccmp_options=nccmp_options,
                                         exclude_vars=exclude_vars)
            output_list.append(nccmp_out)
        else:
            warnings.warn(str(file_candidate) + 'not found in ' + str(ref_dir))
    return output_list

###TODO JTM: Retaining for backwards compatibility until deprecated
compare_restarts = compare_ncfiles

def diff_namelist(namelist1: str, namelist2: str, **kwargs) -> dict:
    """Diff two fortran namelist files and return a dictionary of differences.

    Args:
        namelist1: String containing path to the first namelist file.
        namelist2: String containing path to the second namelist file.
        **kwargs: Additional arguments passed onto deepdiff.DeepDiff method
    Returns:
        The differences between the two namelists
    """

    # Read namelists into dicts
    namelist1 = f90nml.read(namelist1)
    namelist2 = f90nml.read(namelist2)
    # Diff the namelists
    differences = deepdiff.DeepDiff(namelist1, namelist2, ignore_order=True, **kwargs)
    differences_dict = dict(differences)
    return (differences_dict)


def open_nwmdataset(paths: list,
                    chunks: dict=None,
                    forecast: bool = True) -> xr.Dataset:
    """Open a multi-file wrf-hydro output dataset

    Args:
        paths: List ,iterable, or generator of file paths to wrf-hydro netcdf output files
        chunks: chunks argument passed on to xarray DataFrame.chunk() method
        forecast: If forecast the nreference time dimensions is retained, if not then
        reference_time dimension is set to a dummy value (1970-01-01) to ease concatenation
        and analysis
    Returns:
        An xarray dataset of dask arrays chunked by chunk_size along the feature_id
        dimension concatenated along the time and
        reference_time dimensions
    """

    # Create dictionary of forecasts, i.e. reference times
    ds_dict = dict()
    for a_file in paths:
        ds = xr.open_dataset(a_file,chunks=chunks)
        # Check if forecast and set reference_time to zero if not
        if not forecast:
            ds.coords['reference_time'].values = np.array(
                [np.datetime64('1970-01-01T00:00:00', 'ns')])

        ref_time = ds['reference_time'].values[0]
        if ref_time in ds_dict:
            # append the new number to the existing array at this slot
            ds_dict[ref_time].append(ds)
        else:
            # create a new array in this slot
            ds_dict[ref_time] = [ds]

    # Concatenate along time axis for each forecast
    forecast_list = list()
    for key in ds_dict.keys():
        forecast_list.append(xr.concat(ds_dict[key],
                                       dim='time',
                                       coords='minimal'))

    # Concatenate along reference_time axis for all forecasts
    nwm_dataset = xr.concat(forecast_list,
                            dim='reference_time',
                            coords='minimal')

    # Break into chunked dask array
    if chunks is not None:
       nwm_dataset = nwm_dataset.chunk(chunks=chunks)

    return nwm_dataset


class WrfHydroTs(list):
    def open(self, chunks: dict = None):
        """Open a WrfHydroTs object
        Args:
            self
            chunks: chunks argument passed on to xarray.DataFrame.chunk() method
        Returns:
            An xarray mfdataset object concatenated on dimension 'Time'.
        """
        return open_nwmdataset(self, chunks=chunks)


class WrfHydroStatic(pathlib.PosixPath):
    def open(self):
        """Open a WrfHydroStatic object
        Args:
            self
        Returns:
            An xarray dataset object.
        """
        return xr.open_dataset(self)


class RestartDiffs(object):
    def __init__(self,
                 candidate_run: WrfHydroRun,
                 reference_run: WrfHydroRun,
                 nccmp_options: list = ['--data', '--metadata', '--force', '--quiet'],
                 exclude_vars: list = ['ACMELT', 'ACSNOW', 'SFCRUNOFF', 'UDRUNOFF', 'ACCPRCP',
                                       'ACCECAN', 'ACCEDIR', 'ACCETRAN', 'qstrmvolrt']):
        """Calculate Diffs between restart objects for two WrfHydroRun objects
        Args:
            candidate_run: The candidate WrfHydroRun object
            reference_run: The reference WrfHydroRun object
            nccmp_options: List of long-form command line options passed to nccmp,
            see http://nccmp.sourceforge.net/ for options
            exclude_vars: A list of strings containing variables names to
            exclude from the comparison
        Returns:
            A DomainDirectory directory object
        """
        # Instantiate all attributes
        self.diff_counts = dict()
        """dict: Counts of diffs by restart type"""
        self.hydro = list()
        """list: List of pandas dataframes if possible or subprocess objects containing hydro 
        restart file diffs"""
        self.lsm = list()
        """list: List of pandas dataframes if possible or subprocess objects containing lsm restart 
        file diffs"""
        self.nudging = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging 
        restart file diffs"""

        if len(candidate_run.restart_hydro) != 0 and len(reference_run.restart_hydro) != 0:
            self.hydro = compare_ncfiles(candidate_files=candidate_run.restart_hydro,
                                         reference_files=reference_run.restart_hydro,
                                         nccmp_options=nccmp_options,
                                         exclude_vars=exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.hydro))
            self.diff_counts.update({'hydro': diff_counts})
        else:
            warnings.warn('length of candidate_sim.restart_hydro or reference_sim.restart_hydro '
                          'is 0')

        if len(candidate_run.restart_lsm) != 0 and len(reference_run.restart_lsm) != 0:
            self.lsm = compare_ncfiles(candidate_files=candidate_run.restart_lsm,
                                       reference_files=reference_run.restart_lsm,
                                       nccmp_options=nccmp_options,
                                       exclude_vars=exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.lsm))
            self.diff_counts.update({'lsm': diff_counts})
        else:
            warnings.warn('length of candidate_sim.restart_lsm or reference_sim.restart_lsm is 0')

        if candidate_run.restart_nudging is not None or \
                        reference_run.restart_nudging is not None:
            if len(candidate_run.restart_nudging) != 0 and len(reference_run.restart_nudging) != 0:
                self.nudging = compare_ncfiles(
                    candidate_files=candidate_run.restart_nudging,
                    reference_files=reference_run.restart_nudging,
                    nccmp_options=nccmp_options,
                    exclude_vars=exclude_vars)
                diff_counts = sum(1 for _ in filter(None.__ne__, self.nudging))
                self.diff_counts.update({'nudging': diff_counts})
            else:
                warnings.warn('length of candidate_sim.restart_nudging or '
                              'reference_sim.restart_nudging is 0')


def check_file_exist_colon(run_dir, file_str):
    """Takes a file WITH A COLON (not without)."""
    if type(file_str) is not str:
        file_str = str(file_str)
    file_colon = pathlib.Path(file_str)
    file_no_colon = pathlib.Path(file_str.replace(':','_'))
    run_dir = pathlib.Path(run_dir)

    if (run_dir / file_colon).exists():
        return './' + str(file_colon)
    if (run_dir / file_no_colon).exists():
        return './' + str(file_no_colon)
    return None

def touch(filename, mode=0o666, dir_fd=None, **kwargs):
    flags = os.O_CREAT | os.O_APPEND
    filename.open(mode='a+')
    with os.fdopen(os.open(str(filename), flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(f.fileno() if os.utime in os.supports_fd else filename,
                 dir_fd=None if os.supports_fd else dir_fd, **kwargs)