import subprocess
from io import StringIO
import pandas as pd
from warnings import warn
import f90nml
from deepdiff import DeepDiff
import json
from pathlib import Path
from wrfhydropy import *

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
            warn(str(file_candidate) + 'not found in ' + str(ref_dir))
    return output_list

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
    differences = DeepDiff(namelist1, namelist2, ignore_order=True, **kwargs)
    differences_dict = dict(differences)
    return (differences_dict)

####Classes
class RestartDiffs(object):
    def __init__(self,
                 candidate_run: WrfHydroRun,
                 reference_run: WrfHydroRun,
                 nccmp_options: list = ['--data','--metadata', '--force', '--quiet'],
                 exclude_vars: list = ['ACMELT','ACSNOW','SFCRUNOFF','UDRUNOFF','ACCPRCP',
                                       'ACCECAN','ACCEDIR','ACCETRAN','qstrmvolrt']):
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
        self.diff_counts = None
        """dict: Counts of diffs by restart type"""
        self.hydro = None
        """list: List of pandas dataframes if possible or subprocess objects containing hydro 
        restart file diffs"""
        self.lsm = None
        """list: List of pandas dataframes if possible or subprocess objects containing lsm restart 
        file diffs"""
        self.nudging = None
        """list: List of pandas dataframes if possible or subprocess objects containing nudging 
        restart file diffs"""

        #Add a dictionary with counts of diffs
        self.diff_counts = {}

        if len(candidate_run.restart_hydro) != 0 and len(reference_run.restart_hydro) != 0:
            self.hydro = compare_restarts(candidate_files=candidate_run.restart_hydro,
                                          reference_files=reference_run.restart_hydro,
                                          nccmp_options = nccmp_options,
                                          exclude_vars = exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.hydro))
            self.diff_counts.update({'hydro':diff_counts})
        else:
            warn('length of candidate_sim.restart_hydro or reference_sim.restart_hydro is 0')

        if len(candidate_run.restart_lsm) != 0 and len(reference_run.restart_lsm) != 0:
            self.lsm = compare_restarts(candidate_files=candidate_run.restart_lsm,
                                        reference_files=reference_run.restart_lsm,
                                        nccmp_options = nccmp_options,
                                        exclude_vars = exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.lsm))
            self.diff_counts.update({'lsm':diff_counts})
        else:
            warn('length of candidate_sim.restart_lsm or reference_sim.restart_lsm is 0')

        if len(candidate_run.restart_nudging) != 0 and len(reference_run.restart_nudging) != 0:
            self.nudging = compare_restarts(
                candidate_files=candidate_run.restart_nudging,
                reference_files=reference_run.restart_nudging,
                nccmp_options = nccmp_options,
                exclude_vars = exclude_vars)
            diff_counts = sum(1 for _ in filter(None.__ne__, self.nudging))
            self.diff_counts.update({'nudging':diff_counts})
        else:
            warn('length of candidate_sim.restart_nudging or reference_sim.restart_nudging is 0')


class DomainDirectory(object):
    """An object that represents a WRF-Hydro domain directory. Primarily used as a utility class
    for WrfHydroDomain"""
    def __init__(self,
                 domain_top_dir: str,
                 domain_config: str,
                 model_version: str,
                 namelist_patch_file: str = 'namelist_patches.json'):
        """Create a run directory of symlinks using the domain namelist patches
        Args:
            domain_top_dir: Parent directory containing all domain directories and files.
            domain_config: The domain configuration to use, options are 'NWM',
                'Gridded', or 'Reach'
            model_version: The WRF-Hydro model version
            namelist_patch_file: Filename of json file containing namelist patches
        Returns:
            A DomainDirectory directory object
        """

        ###Instantiate arguments to object
        # Make file paths
        self.domain_top_dir = Path(domain_top_dir)
        self.namelist_patch_file = self.domain_top_dir.joinpath(namelist_patch_file)

        # Load namelist patches
        self.namelist_patches = json.load(open(self.namelist_patch_file, 'r'))

        self.model_version = model_version
        self.domain_config = domain_config
        ###

        # Create file paths from hydro namelist
        domain_hydro_nlist = self.namelist_patches[self.model_version][self.domain_config][
            'hydro_namelist']['hydro_nlist']

        self.hydro_files = []
        for key, value in domain_hydro_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.suffix =='.nc':
                self.hydro_files.append(WrfHydroStatic(file_path))
            else:
                self.hydro_files.append(file_path)

        # Create file paths from nudging namelist
        domain_nudging_nlist = self.namelist_patches[self.model_version][self.domain_config
        ]['hydro_namelist']['nudging_nlist']

        self.nudging_files = []
        for key, value in domain_nudging_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))
            if file_path.suffix =='.nc':
                self.nudging_files.append(WrfHydroStatic(file_path))
            else:
                self.nudging_files.append(file_path)

        # Create symlinks from lsm namelist
        domain_lsm_nlist = \
            self.namelist_patches[self.model_version][self.domain_config]['namelist_hrldas'
            ]["noahlsm_offline"]

        self.lsm_files = []
        for key, value in domain_lsm_nlist.items():
            file_path = self.domain_top_dir.joinpath(str(value))

            if file_path.is_file() is True:
                if file_path.suffix == '.nc':
                    self.lsm_files.append(WrfHydroStatic(file_path))
                else:
                    self.lsm_files.append(file_path)

            if key == 'indir':
                self.forcing = file_path

