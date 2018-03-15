from pathlib import Path
from shutil import rmtree
from utilities import *

class FundamentalTest(object):
    def __init__(self,candidate_sim,reference_sim,test_output_dir,overwrite = False):
        self.candidate_sim = candidate_sim
        self.reference_sim = reference_sim
        self.test_output_dir = Path(test_output_dir)
        self.test_results = {}

        if self.test_output_dir.is_dir() is False:
            self.test_output_dir.mkdir(parents=True)
        else:
            if self.test_output_dir.is_dir() is True and overwrite is True:
                rmtree(str(self.test_output_dir))
                self.test_output_dir.mkdir()
            else:
                raise IOError(str(self.test_output_dir) + ' directory already exists')

    ###Compile questions
    def test_compile_candidate(self, compiler: str,
                               overwrite: bool = False,
                               compile_options: dict = None):

        compile_dir = self.test_output_dir.joinpath('compile_candidate')

        #Compile the model
        self.candidate_sim.model.compile(compiler,
                                         compile_dir,
                                         overwrite,
                                         compile_options)
        #Check compilation status
        if self.candidate_sim.model.compile_log.returncode != 0:
            self.test_results.update({'compile_candidate':'fail'})
        else:
            self.test_results.update({'compile_candidate':'pass'})

    def test_compile_reference(self, compiler: str = None,
                               overwrite: bool = False,
                               compile_options: dict = None):

        compile_dir = self.test_output_dir.joinpath('compile_reference')

        #If not specifying different compile options default to candidate options
        if compiler == None:
            compiler = self.candidate_sim.compiler
        if compile_options == None:
            compile_options = self.candidate_sim.model.compile_options

        #Compile the model
        self.reference_sim.model.compile(compiler,
                                   compile_dir,
                                   overwrite,
                                   compile_options)

        # Check compilation status
        if self.reference_sim.model.compile_log.returncode != 0:
            self.test_results.update({'compile_reference':'fail'})
        else:
            self.test_results.update({'compile_reference':'pass'})

    ###Run questions
    def test_run_candidate(self,num_cores: int = 2):

        #Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('run_candidate')

        #Run the simulation
        self.candidate_run = self.candidate_sim.run(simulation_dir,num_cores)

        #Check subprocess and model run status
        if self.candidate_run.run_log.returncode != 0 | self.candidate_run.run_status != 0:
            self.test_results.update({'run_candidate':'fail'})
        else:
            self.test_results.update({'run_candidate': 'pass'})

    def test_run_reference(self, num_cores: int = 2):
        #Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('run_reference')

        # If not specifying different run options default to candidate options
        if num_cores is None:
            num_cores = self.candidate_run.num_cores

        # Run the simulation
        self.reference_run = self.reference_sim.run(simulation_dir,
                                                    num_cores)

        # Check subprocess and model run status
        if self.reference_run.run_log.returncode != 0 | self.reference_run.run_status != 0:
            self.test_results.update({'run_reference': 'fail'})
        else:
            self.test_results.update({'run_reference': 'pass'})

    #Ncores question
    def test_ncores_candidate(self, num_cores: int = 1):

        # Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('ncores_candidate')

        # Run the simulation
        self.candidate_ncores_run = self.candidate_sim.run(simulation_dir, num_cores)

        # Check subprocess and model run status
        if self.candidate_ncores_run.run_log.returncode != 0 | self.candidate_ncores_run.run_status != 0:
            self.test_results.update({'run_ncores': 'fail'})
        else:
            self.test_results.update({'run_ncores': 'pass'})

        #Check against initial run
        self.ncores_restart_diffs = RestartDiffs(self.candidate_ncores_run,
                                                 self.candidate_run)

        #Check that all restart diffs are None
        if all(value == 0 for value in self.ncores_restart_diffs.diff_counts.values()):
            self.test_results.update({'diff_ncores': 'pass'})
        else:
            diff_status = ''
            for key in self.ncores_restart_diffs.diff_counts.keys():
                diff_status = diff_status + str(key) + ':' + \
                              str(self.ncores_restart_diffs.diff_counts[key]) + ' '
            self.test_results.update({'diff_ncores': 'fail -' + diff_status})

    #Perfect restarts question
    def test_prestart_candidate(self, num_cores: int = 2):

        # Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('restart_candidate')

        #Get the correct restarts
        #Make dict of filename and restart time
        hydro_restart_times = {}
        for hydro_file in self.candidate_sim.restart_hydro:
            hydro_dataset = hydro_file.open()
            hydro_restart_times.append({str(hydro_file): hydro_dataset.Restart_Time})

        # TODO - Get restart time out of lsm

        lsm_restart_times = {}
        for lsm_file in self.candidate_sim.restart_lsm:
            lsm_dataset = lsm_file.open()
            lsm_restart_times.append({str(lsm_file): lsm_dataset.START_DATE})

        nudging_restart_times = {}
        for nudging_file in self.candidate_sim.restart_nudging:
            nudging_dataset = nudging_file.open()
            nudging_restart_times.append({str(nudging_file): nudging_dataset.Restart_Time})

        self.candidate_sim.hydro_namelist['hydro_nlist'].update(
            {'restart_file': str(self.candidate_sim.restart_hydro[1])})

        self.candidate_sim.namelist_hrldas['noahlsm_offline'].update(
            {'restart_filename_requested': str(self.candidate_sim.restart_lsm[1])}
        )

        # Run the simulation
        self.candidate_prestart_run = self.candidate_sim.run(simulation_dir, num_cores)

        # Check subprocess and model run status
        if self.candidate_restart_run.run_log.returncode != 0 | \
                self.candidate_prestart_run.run_status != 0:
            self.test_results.update({'run_restart': 'fail'})
        else:
            self.test_results.update({'run_restart': 'pass'})

        #Check against initial run
        self.prestart_restart_diffs = RestartDiffs(self.candidate_prestart_run,
                                                 self.candidate_run)

    #regression question
    def test_regression(self, num_cores: int = 2):

        #Check regression
        self.regression_diffs = RestartDiffs(self.candidate_run,
                                                 self.reference_run)

        #Check that all restart diffs are None
        if all(value == 0 for value in self.regression_diffs.diff_counts.values()):
            self.test_results.update({'diff_regression': 'pass'})
        else:
            diff_status = ''
            for key in self.regression_diffs.diff_counts.keys():
                diff_status = diff_status + str(key) + ':' + \
                              str(self.regression_diffs.diff_counts[key]) + ' '
            self.test_results.update({'diff_regression': 'fail -' + diff_status})