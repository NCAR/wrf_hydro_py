import subprocess
from io import StringIO
import pandas as pd
from warnings import warn
import f90nml
from deepdiff import DeepDiff

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
        output = StringIO()
        output.write(proc.stdout.decode('utf-8'))
        output.seek(0)

        # Open stringio object as pandas dataframe
        try:
            nccmp_out = pd.read_table(output,delim_whitespace=True,header=0)
            return nccmp_out
        except:
            warn('Probleming reading nccmp output to pandas dataframe,'
                 'returning as subprocess object')
            return proc


def compare_restarts(candidate_files: list,
                     reference_files: list,
                     nccmp_options: list = ['--data', '--metadata', '--force', '--quiet'],
                     exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                           'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt']):
    """Compare lists of netcdf files in two directories. Files must have common names
    Args:
        candidate_restart: The path for the candidate restart file
        ref_restarts: The path for the reference restart file
    Returns:
        A named list of either a pandas dataframes if possible or subprocess objects
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
            warn(str(file_candidate) + 'not found in ' + str(ref_dir))
    return output_list

def diff_namelist(namelist1: str, namelist2: str, **kwargs) -> dict:
    """Diff two fortran namelist files and return a dictionary of differences.

    Args:
        namelist1: String containing path to the first namelist file.
        namelist2: String containing path to the second namelist file.
        **kwargs: Additionaly arguments passed onto deepdiff.DeepDiff method
    Returns:
        The differences between the two namelists
    """

    # Read namelists into dicts
    namelist1 = f90nml.read(namelist1)
    namelist2 = f90nml.read(namelist2)
    # Diff the namelists
    differences = DeepDiff(namelist1, namelist2, ignore_order=True, **kwargs)
    differences_dict = dict(differences)
    return (differences_dict)


####Classes
class RestartDiffs(object):
    def __init__(self, candidate_sim, reference_sim,
                 nccmp_options: list = ['--data','--metadata', '--force', '--quiet'],
                 exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                       'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt']):

        #Add a dictionary with counts of diffs
        self.diff_counts = {}

        if len(candidate_sim.restart_hydro) != 0:
            self.hydro = compare_restarts(candidate_files=candidate_sim.restart_hydro,
                                          reference_files=reference_sim.restart_hydro)
            diff_counts = len(self.hydro) - self.hydro.count(None)
            self.diff_counts.update({'hydro':diff_counts})

        if len(candidate_sim.restart_lsm) != 0:
            self.lsm = compare_restarts(candidate_files=candidate_sim.restart_lsm,
                                        reference_files=reference_sim.restart_lsm)
            diff_counts = len(self.lsm) - self.lsm.count(None)
            self.diff_counts.update({'lsm':diff_counts})

        if len(candidate_sim.restart_nudging) != 0:
            self.nudging = compare_restarts(
                candidate_files=candidate_sim.restart_nudging,
                reference_files=reference_sim.restart_nudging)
            diff_counts = len(self.nudging) - self.nudging.count(None)
            self.diff_counts.update({'nudging':diff_counts})

