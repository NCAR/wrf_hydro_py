import sys
sys.path.insert(0, '/home/docker/wrf_hydro_py/wrfhydro')

import pathlib
from wrf_hydro_model import *
import pickle
import deepdiff
import copy
import pytest

##################################
####Setup the test with a domain, a candidate, and a reference
# Get domain, reference, candidate, and optional output directory from command line arguments
# Setup a domain

# Make sure the lsm and hydro restart output timesteps are the same
#hydro_rst_dt = CANDIDATE_SIM.hydro_namelist['hydro_nlist']['rst_dt']
#CANDIDATE_SIM.namelist_hrldas['noahlsm_offline']['restart_frequency_hours'] = int(hydro_rst_dt/60)

##################################
# Define tests

###Compile questions
def test_compile_candidate(candidate_sim,output_dir):
    compile_dir = output_dir / 'compile_candidate'

    # Compile the model
    candidate_sim.model.compile(compile_dir,'gfort')

    # Check compilation status
    assert candidate_sim.model.compile_log.returncode == 0


def test_compile_reference(reference_sim,output_dir):
    compile_dir = output_dir / 'compile_reference'

    # Compile the model
    reference_sim.model.compile(compile_dir,'gfort')

    # Check compilation status
    assert reference_sim.model.compile_log.returncode == 0



###Run questions
def test_run_candidate(candidate_sim,output_dir):
    # Set simulation directory
    simulation_dir = output_dir / 'run_candidate'

    # Run the simulation
    candidate_run = candidate_sim.run(simulation_dir,2)

    # Check subprocess and model run status
    assert candidate_run.run_log.returncode == 0
    assert candidate_run.run_status == 0

def test_run_reference(reference_sim,output_dir):
    #Set simulation directory
    simulation_dir = OUTPUT_DIR / 'run_reference'

    # Run the simulation
    reference_run = REFERENCE_SIM.run(simulation_dir,2)

    # Check subprocess and model run status
    assert reference_run.run_log.returncode == 0
    assert reference_run.run_status == 0


#Ncores question
def test_ncores_candidate(candidate_sim,output_dir):

    # Load initial run model object
    candidate_run_expected = pickle.load(open(output_dir / 'run_candidate/WrfHydroRun.pkl', "rb"))
    # Set simulation directory
    simulation_dir = output_dir.joinpath('ncores_candidate')

    # Run the simulation
    candidate_ncores_run = candidate_sim.run(simulation_dir, 1)

    #Check against initial run
    ncores_restart_diffs = RestartDiffs(candidate_ncores_run,candidate_run_expected)

    ## Check hydro restarts
    for diff in ncores_restart_diffs.hydro:
        assert diff == None

    ## Check lsm restarts
    for diff in ncores_restart_diffs.lsm:
        assert diff == None

    ## Check nudging restarts
    for diff in ncores_restart_diffs.nudging:
        assert diff == None


#Perfect restarts question
def test_perfrestart_candidate(output_dir,candidate_sim):
    # Load initial run model object
    candidate_run_expected = pickle.load(open(output_dir / 'run_candidate/WrfHydroRun.pkl', "rb"))

    #Make deep copy since changing namelist optoins
    perfrestart_sim = copy.deepcopy(candidate_sim)

    # Set simulation directory
    simulation_dir = output_dir.joinpath('restart_candidate')

    #Make directory so that symlinks can be placed
    simulation_dir.mkdir(parents=True)

    # Symlink restarts files to new directory and modify namelistrestart files

    # Hydro
    hydro_rst = candidate_sim.restart_hydro[0]
    new_hydro_rst_path = simulation_dir.joinpath(hydro_rst.name)
    new_hydro_rst_path.symlink_to(hydro_rst)

    perfrestart_sim.hydro_namelist['hydro_nlist'].update(
        {'restart_file': str(new_hydro_rst_path)})

    # LSM
    lsm_rst = candidate_sim.restart_lsm[0]
    new_lsm_rst_path = simulation_dir.joinpath(lsm_rst.name)
    new_lsm_rst_path.symlink_to(lsm_rst)

    perfrestart_sim.namelist_hrldas['noahlsm_offline'].update(
        {'restart_filename_requested': str(simulation_dir.joinpath(lsm_rst.name))})

    # Nudging
    if len(candidate_sim.restart_nudging) > 0:
        nudging_rst = candidate_run_expected.restart_nudging[0]
        new_nudging_rst_path = simulation_dir.joinpath(nudging_rst.name)
        new_nudging_rst_path.symlink_to(nudging_rst)

        perfrestart_sim.hydro_namelist['nudging_nlist'].update(
            {'nudginglastobsfile': str(simulation_dir.joinpath(nudging_rst.name))})

    #Move simulation start time to restart time in hydro restart file
    start_dt = hydro_rst.open()
    start_dt = dt.datetime.strptime(start_dt.Restart_Time,'%Y-%m-%d_%H:%M:%S')
    perfrestart_sim.namelist_hrldas['noahlsm_offline'].update(
        {'start_year': start_dt.year,
         'start_month': start_dt.month,
         'start_day': start_dt.day,
         'start_hour': start_dt.hour,
         'start_min': start_dt.minute})

    #Adjust duration to be shorter by restart time delta in days
    hydro_rst_dt = candidate_sim.hydro_namelist['hydro_nlist']['rst_dt']
    previous_duration =  candidate_run_expected.simulation.namelist_hrldas['noahlsm_offline'][
        'kday']
    new_duration = int(previous_duration - hydro_rst_dt/60/24)
    perfrestart_sim.namelist_hrldas['noahlsm_offline'].update({'kday':new_duration})

    # Run the simulation
    candidate_perfrestart_run = perfrestart_sim.run(simulation_dir, 1,mode='a')

    #Check against initial run
    perfstart_restart_diffs = RestartDiffs(candidate_perfrestart_run,candidate_run_expected)
    ## Check hydro restarts
    for diff in perfstart_restart_diffs.hydro:
        assert diff == None

    ## Check lsm restarts
    for diff in perfstart_restart_diffs.lsm:
        assert diff == None

    ## Check nudging restarts
    for diff in perfstart_restart_diffs.nudging:
        assert diff == None

#regression question
def test_regression(output_dir):
    candidate_run_expected = pickle.load(open(output_dir / 'run_candidate', "rb"))
    reference_run_expected = pickle.load(open(output_dir / 'run_reference', "rb"))
    #Check regression
    regression_diffs = RestartDiffs(candidate_run_expected,reference_run_expected)

    ## Check hydro restarts
    for diff in regression_diffs.hydro:
        assert diff == None

    ## Check lsm restarts
    for diff in regression_diffs.lsm:
        assert diff == None

    ## Check nudging restarts
    for diff in regression_diffs.nudging:
        assert diff == None
