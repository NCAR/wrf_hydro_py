import io
import shlex
import subprocess
import warnings

import pandas as pd

from .simulation import SimulationOutput


def compare_ncfiles(candidate_files: list,
                    reference_files: list,
                    nccmp_options: list = ['--data', '--metadata', '--force'],
                    exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                          'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt',
                                          'reference_time'],
                    exclude_atts: list = ['valid_min']):
    """Compare lists of netcdf restart files element-wise. Files must have common names
    Args:
        candidate_files: List of candidate netcdf file paths
        reference_files: List of reference netcdf file paths
        nccmp_options: List of long-form command line options passed to nccmp,
        see http://nccmp.sourceforge.net/ for options
        exclude_vars: A list of strings containing variables names to
        exclude from the comparison
        exclude_atts: A list of strings containing attribute names to
        exclude from the comparison
    Returns:
        A named list of either pandas dataframes if possible or subprocess objects
    """

    ref_dir = reference_files[0].parent
    output_list = []
    for file_candidate in candidate_files:
        file_reference = ref_dir.joinpath(file_candidate.name)
        if file_reference.is_file():
            nccmp_out = _compare_nc_nccmp(candidate_nc=file_candidate,
                                          reference_nc=file_reference,
                                          nccmp_options=nccmp_options,
                                          exclude_vars=exclude_vars,
                                          exclude_atts=exclude_atts)
            output_list.append(nccmp_out)
        else:
            warnings.warn(str(file_candidate) + 'not found in ' + str(ref_dir))
    return output_list


class OutputDiffs(object):
    def __init__(self,
                 candidate_output: SimulationOutput,
                 reference_output: SimulationOutput,
                 nccmp_options: list = ['--data', '--metadata', '--force'],
                 exclude_vars: list = ['ACMELT', 'ACSNOW', 'SFCRUNOFF', 'UDRUNOFF', 'ACCPRCP',
                                       'ACCECAN', 'ACCEDIR', 'ACCETRAN', 'qstrmvolrt',
                                       'reference_time'],
                 exclude_atts: list = ['valid_min']):
        """Calculate Diffs between SimulationOutput objects from two WrfHydroSim objects
        Args:
            candidate_output: The candidate SimulationOutput object
            reference_output: The reference SimulationOutput object
            nccmp_options: List of long-form command line options passed to nccmp,
            see http://nccmp.sourceforge.net/ for options
            exclude_vars: A list of strings containing variables names to
            exclude from the comparison
            exclude_atts: A list of strings containing attribute names to
            exclude from the comparison
        Returns:
            An OutputDiffs object
        """
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
        atts_list = ['channel_rt','chanobs','lakeout','gwout','restart_hydro','restart_lsm',
                     'restart_nudging']
        for att in atts_list:
            candidate_att = getattr(candidate_output,att)
            reference_att = getattr(reference_output,att)

            if candidate_att is not None and reference_att is not None:
                setattr(self,att,compare_ncfiles(candidate_files=candidate_att,
                                                 reference_files=reference_att,
                                                 nccmp_options=nccmp_options,
                                                 exclude_vars=exclude_vars,
                                                 exclude_atts=exclude_atts)
                        )
                diff_counts = sum(1 for _ in filter(None.__ne__, getattr(self,att)))
                self.diff_counts.update({att: diff_counts})


def _compare_nc_nccmp(candidate_nc: str,
                      reference_nc: str,
                      nccmp_options: list = ['--data','--metadata','--force'],
                      exclude_vars: list = None,
                      exclude_atts: list = None):

    """Private method to compare two netcdf files using nccmp.
    This is wrapped by compare ncfiles to applying to a list of one or more files
    Args:
        candidate_nc: The path for the candidate restart file
        ref_nc: The path for the reference restart file
        nccmp_options: List of long-form command line options passed to nccmp,
        see http://nccmp.sourceforge.net/ for options
        exclude_vars: A list of strings containing variables names to
        exclude from the comparison
        exclude_atts: A list of strings containing attribute names to
        exclude from the comparison
    Returns:
        Either a pandas dataframe if possible or subprocess object
    """
    #Try and set files to strings
    candidate_nc = str(candidate_nc)
    reference_nc = str(reference_nc)

    # Make string to pass to subprocess
    command_str = 'nccmp '

    for item in nccmp_options:
        command_str += item + ' '

    command_str += '-S '

    if exclude_vars is not None:
        # Convert exclude_vars list into a comman separated string
        exclude_vars = ','.join(exclude_vars)
        #append
        command_str += '--exclude=' + exclude_vars + ' '

    if exclude_atts is not None:
        # Convert exclude_vars list into a comman separated string
        exclude_atts = ','.join(exclude_atts)
        #append
        command_str += '--Attributes=' + exclude_atts + ' '

    command_str += candidate_nc + ' '
    command_str += reference_nc

    #Run the subprocess to call nccmp
    proc = subprocess.run(shlex.split(command_str),
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
            warnings.warn('Problem reading nccmp output to pandas dataframe,'
                          'returning as subprocess object')
            return proc
