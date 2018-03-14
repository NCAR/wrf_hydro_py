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


def main():
    #Testing stuff
    candidate = '/home/docker/tempTests/nwmOut/RESTART.2011082612_DOMAIN1'
    reference = '/home/docker/tempTests/nwmOut/RESTART.2011082700_DOMAIN1'

    compare_restarts(candidate,reference)


# def compare_restarts(candidate_restarts: list, ref_restarts: list, exclude_vars: list):
#     """Function to compare two restart files
#     Args:
#         candidate_restarts: A list of path objects for the candidate restart files
#         ref_restarts: A list of path objects for the reference restart files
#         exclude_vars: A list of strings containing variables names to exclude from the comparison
#     Returns:
#         A string containing results of the diff
#     """
#
#     for candidate_file in candidate_restarts:
#
#
#
#
#
#
#
#
#
#     restart_files = glob(candidate_restarts + '/*RESTART*')
#     hydro_files = glob(candidate_restarts + '/*HYDRO_RST*')
#     nudging_files = glob(candidate_restarts + '/*nudgingLastObs*')
#
#     # Make a flag for when comparisons actually happen
#     comparison_run_check = 0
#
#     # Make a flag for exit codes
#     exit_code = 0
#
#     # Compare RESTART files
#     restart_out = list()
#     print('Comparing RESTART files')
#     for test_run_file in restart_files:
#         test_run_filename = os.path.basename(test_run_file)
#         ref_run_file = glob(ref_restarts + '/' + test_run_filename)
#         if len(ref_run_file) == 0:
#             warnings.warn(test_run_filename + ' not found in reference run directory')
#         else:
#             print('Comparing candidate file ' + test_run_file + ' against reference file ' +
#                   ref_run_file[0])
#             restart_comp_out = subprocess.run(['nccmp', '-dmfq', '-S', \
#                                                '-x ACMELT,ACSNOW,SFCRUNOFF,UDRUNOFF,ACCPRCP,ACCECAN,ACCEDIR,ACCETRAN,qstrmvolrt', \
#                                                test_run_file, ref_run_file[0]], \
#                                               stderr=subprocess.STDOUT)
#             if restart_comp_out.returncode != 0:
#                 print(restart_comp_out.stdout)
#             restart_out.append(restart_comp_out)
#             comparison_run_check = 1
#
#     # Compare HYDRO_RST files
#     hydro_out = list()
#     print('Comparing HYDRO_RST files')
#     for test_run_file in hydro_files:
#         test_run_filename = os.path.basename(test_run_file)
#         ref_run_file = glob(ref_restarts + '/' + test_run_filename)
#         if len(ref_run_file) == 0:
#             warnings.warn(test_run_filename + ' not found in reference run directory')
#         else:
#             print('Comparing candidate file ' + test_run_file + ' against reference file ' +
#                   ref_run_file[0])
#             hydro_restart_comp_out = subprocess.run(['nccmp', '-dmfq', '-S', \
#                                                      '-x ACMELT,ACSNOW,SFCRUNOFF,UDRUNOFF,ACCPRCP,ACCECAN,ACCEDIR,ACCETRAN,qstrmvolrt', \
#                                                      test_run_file, ref_run_file[0]], \
#                                                     stderr=subprocess.STDOUT)
#             if hydro_restart_comp_out.returncode != 0:
#                 print(hydro_restart_comp_out.stdout)
#             hydro_out.append(hydro_restart_comp_out)
#             comparison_run_check = 1
#
#     # Compare nudgingLastObs files
#     nudging_out = list()
#     print('Comparing nudgingLastObs files')
#     for test_run_file in nudging_files:
#         test_run_filename = os.path.basename(test_run_file)
#         ref_run_file = glob(ref_restarts + '/' + test_run_filename)
#         if len(ref_run_file) == 0:
#             warnings.warn(test_run_filename + ' not found in reference run directory')
#         else:
#             print('Comparing candidate file ' + test_run_file + ' against reference file ' +
#                   ref_run_file[0])
#             nudging_restart_out = subprocess.run(['nccmp', '-dmfq', '-S', \
#                                                   '-x ACMELT,ACSNOW,SFCRUNOFF,UDRUNOFF,ACCPRCP,ACCECAN,ACCEDIR,ACCETRAN,qstrmvolrt', \
#                                                   test_run_file, ref_run_file[0]], \
#                                                  stderr=subprocess.STDOUT)
#             if nudging_restart_out.returncode != 0:
#                 print(nudging_restart_out.stdout)
#             nudging_out.append(nudging_restart_out)
#             comparison_run_check = 1
#
#     # Check that a comparison was actually done
#     if comparison_run_check != 1:
#         print('No matching files were found to compare')
#         exit(1)
#
#     # Check for exit codes and fail if non-zero
#     for output in restart_out:
#         if output.returncode == 1:
#             print('One or more RESTART comparisons failed, see stdout')
#             exit_code = 1
#
#     # Check for exit codes and fail if non-zero
#     for output in hydro_out:
#         if output.returncode == 1:
#             print('One or more HYDRO_RST comparisons failed, see stdout')
#             exit_code = 1
#
#     # Check for exit codes and fail if non-zero
#     for output in nudging_out:
#         if output.returncode == 1:
#             print('One or more nudgingLastObs comparisons failed, see stdout')
#             exit_code = 1
#
#     # If no errors exit with code 0
#     if exit_code == 0:
#         print('All restart file comparisons pass')
#         exit(0)
#     else:
#         exit(1)
#
#
# def main():
#     """Main function of script that taes 2 command line arguments. The first argument is the candidate_run_dir
#     and the second argument is the reference_run_dir
#
#     Args:
#         candidate_run_dir: The directory containing the restart files for the candidate run
#         ref_run_dir: The directory containing the restart files for the reference run
#
#     Returns:
#         A string indicating success of directory creation
#     """
#     candidate_run_dir = argv[1]
#     ref_run_dir = argv[2]
#     compare_restarts(candidate_run_dir, ref_run_dir)
#
#
# if __name__ == "__main__":
#     main()
