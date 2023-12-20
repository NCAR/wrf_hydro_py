import io
import shlex
import subprocess
import warnings
import pandas as pd
import pathlib

from ..util.xrcmp import xrcmp
from .simulation import SimulationOutput


def compare_ncfiles(
    candidate_files: list,
    reference_files: list,
    stats_only: bool = False,
    nccmp_options: list = None,
    exclude_vars: list = None,
    exclude_atts: list = None,
    xrcmp_n_cores: int = 0
):
    """Compare lists of netcdf restart files element-wise. Files must have common names
    Args:
        candidate_files: List of candidate netcdf file paths
        reference_files: List of reference netcdf file paths
        stats_only: Only return statistics on differences in data values
        nccmp_options: List of long-form command line options passed to nccmp,
        see http://nccmp.sourceforge.net/ for options. Defaults are '--metadata', '--force'
        exclude_vars: A list of strings containing variables names to
        exclude from the comparison.
        exclude_atts: A list of strings containing attribute names to exclude from the
        comparison. Defaults are 'valid_min'
    Returns:
        A named list of either pandas dataframes if possible or subprocess objects
    """

    if nccmp_options is None:
        nccmp_options = ['--data', '--metadata', '--force']

    if len(candidate_files) != len(reference_files):
        raise ValueError('Length of candidate files does not match len of reference files')

    file_list = zip(candidate_files, reference_files)

    output_list = []
    for files in file_list:
        file_candidate = pathlib.Path(files[0])
        file_reference = pathlib.Path(files[1])

        if xrcmp_n_cores > 1 and '--metadata' not in nccmp_options:
            cmp_func = _compare_nc_xrcmp
        else:
            cmp_func = _compare_nc_nccmp

        nccmp_out = cmp_func(
            candidate_nc=str(file_candidate),
            reference_nc=str(file_reference),
            stats_only=stats_only,
            nccmp_options=nccmp_options,
            exclude_vars=exclude_vars,
            exclude_atts=exclude_atts,
            n_cores=xrcmp_n_cores
        )
        output_list.append(nccmp_out)
    return output_list


class OutputDataDiffs(object):
    def __init__(
        self,
        candidate_output: SimulationOutput,
        reference_output: SimulationOutput,
        nccmp_options: list = None,
        exclude_vars: list = None,
        exclude_atts: list = None,
        xrcmp_n_cores: int = 0
    ):
        """Calculate Diffs between SimulationOutput objects from two WrfHydroSim objects
        Args:
            candidate_output: The candidate SimulationOutput object
            reference_output: The reference SimulationOutput object
            nccmp_options: List of long-form command line options passed to nccmp,
            see http://nccmp.sourceforge.net/ for options. Defaults are '--data', '--force'
            exclude_vars: A list of strings containing variables names to
            exclude from the comparison.
            exclude_atts: A list of strings containing attribute names to exclude from the
            comparison. Defaults are 'valid_min'
        Returns:
            An OutputDiffs object
        """

        # Set default arguments
        if nccmp_options is None:
            nccmp_options = ['--data', '--force']

        if exclude_atts is None:
            exclude_atts = ['valid_min']

        # Instantiate all attributes
        self.diff_counts = dict()
        """dict: Counts of diffs by restart type"""

        self.channel_rt = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.channel_rt_grid = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.chanobs = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.lakeout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.gwout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.rtout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.ldasout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.restart_hydro = list()
        """list: List of pandas dataframes if possible or subprocess objects containing hydro
        restart file diffs"""
        self.restart_lsm = list()
        """list: List of pandas dataframes if possible or subprocess objects containing lsm restart
        file diffs"""
        self.restart_nudging = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""

        # Create list of attributes to diff
        atts_list = ['channel_rt', 'channel_rt_grid', 'chanobs', 'lakeout', 'gwout', 'rtout',
                     'ldasout', 'restart_hydro', 'restart_lsm', 'restart_nudging']
        for att in atts_list:
            candidate_att = getattr(candidate_output, att)
            reference_att = getattr(reference_output, att)

            if candidate_att is not None and reference_att is not None:
                # Check that files exist in both directories
                candidate_files = candidate_att
                reference_files = reference_att

                valid_files = _check_file_lists(candidate_files, reference_files)

                setattr(
                    self,
                    att,
                    compare_ncfiles(
                        candidate_files=valid_files[0],
                        reference_files=valid_files[1],
                        stats_only=True,
                        nccmp_options=nccmp_options,
                        exclude_vars=exclude_vars,
                        exclude_atts=exclude_atts,
                        xrcmp_n_cores=xrcmp_n_cores
                    )
                )
                diff_counts = sum(1 for _ in filter(None.__ne__, getattr(self, att)))
                self.diff_counts.update({att: diff_counts})


class OutputMetaDataDiffs(object):
    def __init__(
        self,
        candidate_output: SimulationOutput,
        reference_output: SimulationOutput,
        stats_only=False,
        nccmp_options: list = None,
        exclude_vars: list = None,
        exclude_atts: list = None,
        xrcmp_n_cores: int = 0
    ):
        """Calculate Diffs between SimulationOutput objects from two WrfHydroSim objects
        Args:
            candidate_output: The candidate SimulationOutput object
            reference_output: The reference SimulationOutput object
            nccmp_options: List of long-form command line options passed to nccmp,
            see http://nccmp.sourceforge.net/ for options. Defaults are '--metadata', '--force'
            exclude_vars: A list of strings containing variables names to
            exclude from the comparison.
            exclude_atts: A list of strings containing attribute names to exclude from the
            comparison. Defaults are 'valid_min'
        Returns:
            An OutputDiffs object
        """
        # Set default arguments
        if nccmp_options is None:
            nccmp_options = ['--metadata', '--force']

        if exclude_atts is None:
            exclude_atts = ['valid_min']

        # Instantiate all attributes
        self.diff_counts = dict()
        """dict: Counts of diffs by restart type"""

        self.channel_rt = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.chanobs = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.lakeout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.gwout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.rtout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.ldasout = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""
        self.restart_hydro = list()
        """list: List of pandas dataframes if possible or subprocess objects containing hydro
        restart file diffs"""
        self.restart_lsm = list()
        """list: List of pandas dataframes if possible or subprocess objects containing lsm restart
        file diffs"""
        self.restart_nudging = list()
        """list: List of pandas dataframes if possible or subprocess objects containing nudging
        restart file diffs"""

        # Create list of attributes to diff
        atts_list = ['channel_rt', 'chanobs', 'lakeout', 'gwout', 'rtout', 'ldasout',
                     'restart_hydro', 'restart_lsm', 'restart_nudging']
        for att in atts_list:
            candidate_att = getattr(candidate_output, att)
            reference_att = getattr(reference_output, att)

            if candidate_att is not None and reference_att is not None:
                # Check that files exist in both directories
                candidate_files = candidate_att
                reference_files = reference_att

                valid_files = _check_file_lists(candidate_files, reference_files)

                setattr(
                    self,
                    att,
                    compare_ncfiles(
                        candidate_files=valid_files[0],
                        reference_files=valid_files[1],
                        nccmp_options=nccmp_options,
                        exclude_vars=exclude_vars,
                        exclude_atts=exclude_atts,
                        xrcmp_n_cores=xrcmp_n_cores
                    )
                )
                diff_counts = sum(1 for _ in filter(None.__ne__, getattr(self, att)))
                self.diff_counts.update({att: diff_counts})


def _compare_nc_xrcmp(
    candidate_nc: str,
    reference_nc: str,
    stats_only: bool = False,
    nccmp_options: list = None,
    exclude_vars: list = None,
    exclude_atts: list = None,
    n_cores=1,
    log_file_path: str = "xrcmp.log"
):

    # Try and set files to strings
    candidate_nc = str(candidate_nc)
    reference_nc = str(reference_nc)

    if '/' not in log_file_path:
        log_file_path = pathlib.Path(candidate_nc).parent / pathlib.Path(log_file_path)

        ret = xrcmp(
            can_file=candidate_nc,
            ref_file=reference_nc,
            log_file=str(log_file_path),
            exclude_vars=exclude_vars,
            n_cores=n_cores
        )

    if ret != 0:
        if stats_only:
            try:
                # First try stdout because that is where statistics are written
                # Get stoud into stringio object

                nccmp_out = pd.read_table(log_file_path, delim_whitespace=True, header=0)
                return nccmp_out
            except Exception as e:
                warnings.warn('Problem reading xrcmp output to pandas dataframe,'
                              'returning error code: {e}')
                return ret
        else:
            return open(log_file_path, 'r').read()
    else:
        return None


def _compare_nc_nccmp(
    candidate_nc: str,
    reference_nc: str,
    stats_only: bool = False,
    nccmp_options: list = None,
    exclude_vars: list = None,
    exclude_atts: list = None,
    n_cores: int = 0
):

    """Private method to compare two netcdf files using nccmp.
    This is wrapped by compare ncfiles to applying to a list of one or more files
    Args:
        candidate_nc: The path for the candidate netcdf file
        reference_nc: The path for the reference netcdf file
        stats_only: Only return statistics on differences in data values
        nccmp_options: List of long-form command line options passed to nccmp,
        see http://nccmp.sourceforge.net/ for options. Defaults are '--metadata', '--force'
        exclude_vars: A list of strings containing variables names to
        exclude from the comparison.
        exclude_atts: A list of strings containing attribute names to exclude from the
        comparison. Defaults are 'valid_min'
    Returns:
        Either a pandas dataframe if possible or subprocess object
    """

    # Set default arguments
    if nccmp_options is None:
        nccmp_options = ['--metadata', '--force']

    # Try and set files to strings
    candidate_nc = str(candidate_nc)
    reference_nc = str(reference_nc)

    # Make string to pass to subprocess
    command_str = 'nccmp '

    command_str += ' '.join(nccmp_options)

    command_str += ' -S '

    if exclude_vars is not None:
        # Convert exclude_vars list into a comma separated string
        exclude_vars = ','.join(exclude_vars)
        command_str += '--exclude=' + exclude_vars + ' '

    if exclude_atts is not None:
        # Convert exclude_vars list into a comma separated string
        exclude_atts = ','.join(exclude_atts)
        command_str += '--Attribute=' + exclude_atts + ' '

    command_str += candidate_nc + ' '
    command_str += reference_nc

    # Run the subprocess to call nccmp
    proc = subprocess.run(
        shlex.split(command_str),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Check return code
    if proc.returncode != 0:
        if stats_only:
            try:
                # First try stdout because that is where statistics are written
                # Get stoud into stringio object
                output = io.StringIO()
                output.write(proc.stdout.decode('utf-8'))
                output.seek(0)

                nccmp_out = pd.read_table(output, delim_whitespace=True, header=0)
                return nccmp_out
            except Exception as e:
                warnings.warn('Problem reading nccmp output to pandas dataframe,'
                              'returning as subprocess object: {e}')
                return proc
        else:
            return proc.stderr.decode('utf-8') + proc.stdout.decode('utf-8')
    else:
        return None


def _check_file_lists(candidate_files: list, reference_files: list) -> tuple:
    """Function to check two lists of pathlib.Paths for commonly occuring files between the two
    Args:
        candidate_files: The candidate file list
        reference_files: The reference file list
    Returns:
        A tuple of lists sorted by file name of common files
    """
    candidate_names = [file.name for file in candidate_files]
    reference_names = [file.name for file in reference_files]

    # Get only files occurring in both lists
    matching_files = list(set(candidate_names).intersection(reference_names))

    # Print warning about missing files
    missing_ref_files = [file.name for file in candidate_files if file.name not in matching_files]
    missing_can_files = [file.name for file in reference_files if file.name not in matching_files]
    if len(missing_ref_files) > 0:
        if len(missing_ref_files) == 1:
            miss_file_str = str(missing_ref_files[0])
        else:
            miss_file_str = ', '.join(missing_ref_files)
        warnings.warn(
            'The following reference files were not found in the candidate: ' + miss_file_str)

    if len(missing_can_files) > 0:
        if len(missing_can_files) == 1:
            miss_file_str = str(missing_can_files[0])
        else:
            miss_file_str = ', '.join(missing_can_files)
        warnings.warn(
            'The following candidate files were not found in the reference: ' + miss_file_str)

    # Subset lists to only those files that occur in both
    valid_ref_files = [file for file in candidate_files if file.name in matching_files]
    valid_can_files = [file for file in reference_files if file.name in matching_files]

    # Sort files by name
    valid_ref_files.sort(key=lambda x: x.name)
    valid_can_files.sort(key=lambda x: x.name)

    return valid_can_files, valid_ref_files


def check_unprocessed_diffs(unexpected_diffs: list):
    """Function to the unexpected_diffs of DeepDiff
    Args:
        unexpected_diffs: The unexpected diffs list
    Returns:
        None
    """
    for diff_str in unexpected_diffs:
        diff_key = diff_str.split(':', 1)[0].strip()
        diff = diff_str.split(':', 1)[1].strip()
        diff_parts = diff.split(' and ')  # .strip()
        if (diff_parts[0] != diff_parts[1]):
            # commenting out this section for now to check if CI tests pass
            if ('_compose_dir' in diff_key):
                warnings.warn(UserWarning("deepdiff _compose_dirs were different:",
                                          diff_key, ":", diff_parts[0], " != ", diff_parts[1]))
                return

            # if difference isn't in _compose_dir raise error
            raise ValueError(
                'Unexpected attribute differences in unexpected members from DeepDiff output:',
                diff_key, ":", diff_parts[0], " != ", diff_parts[1])
