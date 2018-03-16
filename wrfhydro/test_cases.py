from pathlib import Path
from shutil import rmtree
from utilities import *
from copy import deepcopy
from datetime import datetime

class FundamentalTest(object):
    def __init__(self,candidate_sim,reference_sim,test_output_dir,overwrite = False):
        self.candidate_sim = deepcopy(candidate_sim)
        self.reference_sim = deepcopy(reference_sim)
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

        ###########
        # Enforce some namelist options up front

        # Make sure the lsm and hydro restart timesteps are output the same
        hydro_rst_dt = self.candidate_sim.hydro_namelist['hydro_nlist']['rst_dt']
        self.candidate_sim.namelist_hrldas['noahlsm_offline']['restart_frequency_hours'] = int(hydro_rst_dt/60)

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
    def test_perfrestart_candidate(self, num_cores: int = 2):
        #Make deep copy since changing namelist optoins
        perfrestart_sim = deepcopy(self.candidate_sim)

        # Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('restart_candidate')

        #Make directory so that symlinks can be placed
        simulation_dir.mkdir(parents=True)

        # Symlink restarts files to new directory and modify namelistrestart files

        # Hydro
        hydro_rst = self.candidate_run.restart_hydro[0]
        new_hydro_rst_path = simulation_dir.joinpath(hydro_rst.name)
        new_hydro_rst_path.symlink_to(hydro_rst)

        perfrestart_sim.hydro_namelist['hydro_nlist'].update(
            {'restart_file': str(new_hydro_rst_path)})

        # LSM
        lsm_rst = self.candidate_run.restart_lsm[0]
        new_lsm_rst_path = simulation_dir.joinpath(lsm_rst.name)
        new_lsm_rst_path.symlink_to(lsm_rst)

        perfrestart_sim.namelist_hrldas['noahlsm_offline'].update(
            {'restart_filename_requested': str(simulation_dir.joinpath(lsm_rst.name))})

        # Nudging
        if len(self.candidate_run.restart_nudging) > 0:
            nudging_rst = self.candidate_run.restart_nudging[0]
            new_nudging_rst_path = simulation_dir.joinpath(nudging_rst.name)
            new_nudging_rst_path.symlink_to(nudging_rst)

            perfrestart_sim.hydro_namelist['nudging_nlist'].update(
                {'nudginglastobsfile': str(simulation_dir.joinpath(nudging_rst.name))})

        #Move simulation start time to restart time in hydro restart file
        start_dt = hydro_rst.open()
        start_dt = datetime.strptime(start_dt.Restart_Time,'%Y-%m-%d_%H:%M:%S')
        perfrestart_sim.namelist_hrldas['noahlsm_offline'].update(
            {'start_year': start_dt.year,
             'start_month': start_dt.month,
             'start_day': start_dt.day,
             'start_hour': start_dt.hour,
             'start_min': start_dt.minute})

        #Adjust duration to be shorter by restart time delta in days
        hydro_rst_dt = self.candidate_sim.hydro_namelist['hydro_nlist']['rst_dt']
        previous_duration =  self.candidate_run.namelist_hrldas['noahlsm_offline']['kday']
        new_duration = int(previous_duration - hydro_rst_dt/60/24)
        perfrestart_sim.namelist_hrldas['noahlsm_offline'].update({'kday':new_duration})

        # Run the simulation
        self.candidate_perfrestart_run = perfrestart_sim.run(simulation_dir, num_cores,mode='a')

        # Check subprocess and model run status
        if self.candidate_perfrestart_run.run_log.returncode != 0 | \
                self.candidate_perfrestart_run.run_status != 0:
            self.test_results.update({'run_restart': 'fail'})
        else:
            self.test_results.update({'run_restart': 'pass'})

        #Check against initial run
        self.perfstart_restart_diffs = RestartDiffs(self.candidate_perfrestart_run,
                                                 self.candidate_run)

        #Check that all restart diffs are None
        if all(value == 0 for value in self.perfstart_restart_diffs.diff_counts.values()):
            self.test_results.update({'diff_perfrestart': 'pass'})
        else:
            diff_status = ''
            for key in self.perfstart_restart_diffs.diff_counts.keys():
                diff_status = diff_status + str(key) + ':' + \
                              str(self.perfstart_restart_diffs.diff_counts[key]) + ' '
            self.test_results.update({'diff_perfrestart': 'fail -' + diff_status})

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