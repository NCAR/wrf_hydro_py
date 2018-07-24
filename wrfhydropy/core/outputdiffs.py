import pandas as pd
import warnings
import subprocess
import io
import f90nml
import deepdiff

from .simulation import SimulationOutput

def diff_namelist(namelist1: str, namelist2: str, **kwargs) -> dict:
    """Diff two fortran namelist files and return a dictionary of differences.

    Args:
        old_namelist: String containing path to the first namelist file, referred to as 'old' in
        outputs.
        new_namelist: String containing path to the second namelist file, referred to as 'new' in
        outputs.
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

def compare_ncfiles(candidate_files: list,
                    reference_files: list,
                    nccmp_options: list = ['--data', '--metadata', '--force', '--quiet'],
                    exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                           'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt']):
    """Compare lists of netcdf restart files element-wise. Files must have common names
    Args:
        candidate_files: List of candidate netcdf file paths
        reference_files: List of reference netcdf file paths
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
            nccmp_out = _compare_nc_nccmp(candidate_nc=file_candidate,
                                         reference_nc=file_reference,
                                         nccmp_options=nccmp_options,
                                         exclude_vars=exclude_vars)
            output_list.append(nccmp_out)
        else:
            warnings.warn(str(file_candidate) + 'not found in ' + str(ref_dir))
    return output_list

class OutputDiffs(object):
    def __init__(self,
                 candidate_output: SimulationOutput,
                 reference_output: SimulationOutput,
                 nccmp_options: list = ['--data', '--metadata', '--force', '--quiet'],
                 exclude_vars: list = ['ACMELT', 'ACSNOW', 'SFCRUNOFF', 'UDRUNOFF', 'ACCPRCP',
                                       'ACCECAN', 'ACCEDIR', 'ACCETRAN', 'qstrmvolrt']):
        """Calculate Diffs between SimulationOutput objects from two WrfHydroSim objects
        Args:
            candidate_output: The candidate SimulationOutput object
            reference_output: The reference SimulationOutput object
            nccmp_options: List of long-form command line options passed to nccmp,
            see http://nccmp.sourceforge.net/ for options
            exclude_vars: A list of strings containing variables names to
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
                                             exclude_vars=exclude_vars)
                        )
                diff_counts = sum(1 for _ in filter(None.__ne__, self.restart_hydro))
                self.diff_counts.update({att: diff_counts})

def _compare_nc_nccmp(candidate_nc: str,
                     reference_nc: str,
                     nccmp_options: list = ['--data','--metadata','--force','--quiet'],
                     exclude_vars: list = None):

    """Private method to compare two netcdf files using nccmp.
    This is wrapped by compare ncfiles to applying to a list of one or more files
    Args:
        candidate_nc: The path for the candidate restart file
        ref_nc: The path for the reference restart file
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
        command_list = command_list + ['-x'] + [exclude_vars]

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
            warnings.warn('Problem reading nccmp output to pandas dataframe,'
                 'returning as subprocess object')
            return proc
