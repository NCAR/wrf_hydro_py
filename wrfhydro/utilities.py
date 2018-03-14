import subprocess
from io import StringIO
import pandas as pd
from warnings import warn

def compare_restarts(candidate_restart: str,
                     ref_restart: str,
                     exclude_vars: list = ['ACMELT,ACSNOW,SFCRUNOFF,UDRUNOFF,'
                                          'ACCPRCP,ACCECAN,ACCEDIR,'
                                          'ACCETRAN,qstrmvolrt']):

    """Function to compare two restart files
    Args:
        candidate_restart: The path for the candidate restart file
        ref_restarts: The path for the reference restart file
        exclude_vars: A list of strings containing variables names to exclude from the comparison
    Returns:
        A string containing results of the diff
    """

    #Convert exclude_vars list into a comman separated string
    exclude_vars = ','.join(exclude_vars)

    #Make list to pass to subprocess
    command_list = ['nccmp', '-dmfq', '-S','-x '+exclude_vars,
                    candidate_restart,
                    ref_restart]

    #Run the subprocess to call nccmp
    proc = subprocess.run(command_list,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    #Check return code
    if proc.returncode != 0:
        #Get stoud into stringio object
        output = StringIO()
        output.write(proc.stdout.decode('utf-8'))
        output.seek(0)

        #Open stringio object as pandas dataframe
        try:
            nccmpOut = pd.read_table(output,delim_whitespace=True,header=0)
            return (nccmpOut)
        except:
            warn('Probleming reading nccmp output to pandas dataframe, returning as subprocess object')
            return(proc)
    else:
        return('No differences found')

