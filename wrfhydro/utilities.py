import subprocess
from io import StringIO
import pandas as pd
from warnings import warn
import f90nml
from deepdiff import DeepDiff

def compare_nc_nccmp(candidate_restart: str,
                     ref_restart: str,
                     nccmp_options: list = ['--data','--metadata','--force','--quiet'],
                     exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                           'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt']):

    """Function to compare two restart files
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

    # Make list to pass to subprocess
    command_list=['nccmp']

    for item in nccmp_options:
        command_list.append(item)

    command_list.append('-S')

    if len(exclude_vars) != 0:
        # Convert exclude_vars list into a comman separated string
        exclude_vars = ','.join(exclude_vars)
        #append
        command_list.append('-x ' + exclude_vars)

    command_list.append(candidate_restart)
    command_list.append(ref_restart)

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
            return (nccmp_out)
        except:
            warn('Probleming reading nccmp output to pandas dataframe,'
                 'returning as subprocess object')
            return proc
    else:
        return 'No differences found'


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
