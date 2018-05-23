import deepdiff
import f90nml
import io
import os
import pandas as pd
import pathlib
import subprocess
import warnings
import xarray as xr
from .job_tools import touch
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

###Retaining for backwards compatibility until deprecated
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


def __make_relative__(run_object, basepath=None):
    """Make all file paths relative to a given directory, useful for opening file
    attributes in a run object after it has been moved or copied to a new directory or
    system.
    Args:
        basepath: The base path to use for relative paths. Defaults to run directory.
        This rarely needs to be defined.
    Returns:
        self with relative files paths for file-like attributes
    """
    import wrfhydropy
    if basepath is None:
        basepath = run_object.simulation_dir
    for attr in dir(run_object):
        if attr.startswith('__') is False:
            attr_object = getattr(run_object, attr)
            if type(attr_object) == list:
                relative_list = list()
                for item in attr_object:
                    if type(item) is pathlib.PosixPath or type(
                            item) is wrfhydropy.WrfHydroStatic:
                        relative_list.append(item.relative_to(basepath))
                        setattr(run_object, attr, relative_list)
            if type(attr_object) is wrfhydropy.WrfHydroTs:
                relative_list = list()
                for item in attr_object:
                    if type(item) is pathlib.PosixPath or type(
                            item) is wrfhydropy.WrfHydroStatic:
                        relative_list.append(item.relative_to(basepath))
                        relative_list = wrfhydropy.WrfHydroTs(relative_list)
                        setattr(run_object, attr, relative_list)

            elif type(attr_object) is pathlib.PosixPath:
                setattr(run_object, attr, attr_object.relative_to(basepath))

        if attr == 'simulation':
            __make_relative__(run_object.simulation.domain,
                          basepath=run_object.simulation.domain.domain_top_dir)


def get_pickle_lock_file(run_obj):
    return run_obj.run_dir / 'pickle_locked'


def lock_pickle(run_obj):
    if is_pickle_locked(run_obj):
        raise ValueError('The pickle file, ' + run_obj.run_dir + ', is already locked')
    pickle_lock_file = get_pickle_lock_file(run_obj)
    touch(pickle_lock_file)
    run_obj._pickle_lock_file = pickle_lock_file


def unlock_pickle(run_obj):
    if not is_pickle_locked(run_obj):
        raise ValueError('The pickle file, ' + run_obj.run_dir + ', is already unlocked')
    pickle_lock_file = get_pickle_lock_file(run_obj)
    os.remove(pickle_lock_file)
    run_obj._pickle_lock_file = None


def is_pickle_locked(run_obj):
    internal_lock = run_obj._pickle_lock_file is not None
    pickle_lock_file = get_pickle_lock_file(run_obj)
    external_lock = pickle_lock_file.exists()
    total_lock = internal_lock + external_lock
    if total_lock == 1:
        raise ValueError('The internal_lock must match external_lock.')
    return bool(total_lock)


def get_git_revision_hash(the_dir):

    # First test if this is even a git repo. (Have to allow for this unless the wrfhydropy
    # testing brings in the wrf_hydro_code as a repo with a .git file.)
    dir_is_repo = subprocess.call(
        ["git", "branch"],
        stderr=subprocess.STDOUT,
        stdout=open(os.devnull, 'w'),
        cwd=the_dir
    )
    if dir_is_repo != 0:
        warnings.warn('The source directory is NOT a git repo: ' + str(the_dir))
        return 'not-a-repo'

    dirty = subprocess.run(
        ['git', 'diff-index', 'HEAD'],  # --quiet seems to give the wrong result.
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=the_dir
    ).returncode
    the_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=the_dir)
    the_hash = the_hash.decode('utf-8').split()[0]
    if dirty:
        the_hash += '--DIRTY--'
    return the_hash
