import pathlib
import shutil
import copy
import datetime as dt
import pickle
from pprint import pprint
import warnings
import sys
from wrf_hydro_model import *
from wrfhydro_test_cases import *
from utilities import *
import pytest

##################################
####Setup the test with a domain, a candidate, and a reference
# Get domain, reference, candidate, and optional output directory from command line arguments
DOMAIN_DIR = str(sys.argv[1])
CANDIDATE_DIR = str(sys.argv[2])
REFERENCE_DIR = str(sys.argv[3])
if len(sys.argv == 4):
    OUTPUT_DIR = str(sys.argv[4])
else:
    OUTPUT_DIR = None

# Setup a candidate model
CANDIDATE_MODEL = WrfHydroModel(CANDIDATE_DIR)

# Setup a reference model
REFERENCE_MODEL = WrfHydroModel(REFERENCE_DIR)

# Setup a domain
DOMAIN = WrfHydroDomain(domain_top_dir=DOMAIN_DIR,
                           domain_config='NWM',
                           model_version=CANDIDATE_MODEL.version)

# Setup a candidate simulation
CANDIDATE_SIM = WrfHydroSim(CANDIDATE_MODEL,DOMAIN)

# Setup a reference simulation
REFERENCE_SIM = WrfHydroSim(REFERENCE_MODEL,DOMAIN)

# Make copies
CANDIDATE_SIM = copy.deepcopy(CANDIDATE_SIM)
REFERENCE_SIM = copy.deepcopy(REFERENCE_SIM)

# Setup output directory
if OUTPUT_DIR is not None:
    OUTPUT_DIR = pathlib.Path(OUTPUT_DIR)
    if OUTPUT_DIR.is_dir() is False:
        OUTPUT_DIR.mkdir(parents=True)
    else:
        raise IOError(str(OUTPUT_DIR) + ' directory already exists')


# Make sure the lsm and hydro restart output timesteps are the same
hydro_rst_dt = CANDIDATE_SIM.hydro_namelist['hydro_nlist']['rst_dt']
CANDIDATE_SIM.namelist_hrldas['noahlsm_offline']['restart_frequency_hours'] = int(hydro_rst_dt/60)

##################################
# Define tests

###Compile questions
def test_compile_candidate():
    compile_dir = OUTPUT_DIR / 'compile_candidate'

    #Compile the model
    CANDIDATE_SIM.model.compile(compile_dir,'gfort')

    #Check compilation status
    assert CANDIDATE_SIM.model.compile_log.returncode == 0


def test_compile_reference():
    compile_dir = OUTPUT_DIR / 'compile_reference'

    #Compile the model
    REFERENCE_SIM.model.compile(compile_dir,'gfort')

    # Check compilation status
    assert REFERENCE_SIM.model.compile_log.returncode == 0



###Run questions
def test_run_candidate():
    #Set simulation directory
    simulation_dir = OUTPUT_DIR / 'run_candidate'

    #Run the simulation
    candidate_run = CANDIDATE_SIM.run(simulation_dir,2)

    #Check subprocess and model run status
    assert candidate_run.run_log.returncode == 0
    assert candidate_run.run_status == 0

def test_run_reference():
    #Set simulation directory
    simulation_dir = OUTPUT_DIR / 'run_reference'

    # Run the simulation
    reference_run = REFERENCE_SIM.run(simulation_dir,2)

    # Check subprocess and model run status
    assert reference_run.run_log.returncode == 0
    assert reference_run.run_status == 0


#Ncores question
def test_ncores_candidate():

    # Load initial run model object
    candidate_run_expected = pickle.load(open(OUTPUT_DIR / 'run_candidate', "rb"))
    # Set simulation directory
    simulation_dir = OUTPUT_DIR.joinpath('ncores_candidate')

    # Run the simulation
    candidate_ncores_run = CANDIDATE_SIM.run(simulation_dir, 1)

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
def test_perfrestart_candidate():
    #Make deep copy since changing namelist optoins
    perfrestart_sim = copy.deepcopy(CANDIDATE_SIM)

    # Set simulation directory
    simulation_dir = OUTPUT_DIR.joinpath('restart_candidate')

    #Make directory so that symlinks can be placed
    simulation_dir.mkdir(parents=True)

    # Symlink restarts files to new directory and modify namelistrestart files

    # Hydro
    hydro_rst = candidate_run.restart_hydro[0]
    new_hydro_rst_path = simulation_dir.joinpath(hydro_rst.name)
    new_hydro_rst_path.symlink_to(hydro_rst)

    perfrestart_sim.hydro_namelist['hydro_nlist'].update(
        {'restart_file': str(new_hydro_rst_path)})

    # LSM
    lsm_rst = candidate_run.restart_lsm[0]
    new_lsm_rst_path = simulation_dir.joinpath(lsm_rst.name)
    new_lsm_rst_path.symlink_to(lsm_rst)

    perfrestart_sim.namelist_hrldas['noahlsm_offline'].update(
        {'restart_filename_requested': str(simulation_dir.joinpath(lsm_rst.name))})

    # Nudging
    if len(candidate_run.restart_nudging) > 0:
        nudging_rst = candidate_run.restart_nudging[0]
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
    hydro_rst_dt = CANDIDATE_SIM.hydro_namelist['hydro_nlist']['rst_dt']
    previous_duration =  candidate_run.simulation.namelist_hrldas['noahlsm_offline'][
        'kday']
    new_duration = int(previous_duration - hydro_rst_dt/60/24)
    perfrestart_sim.namelist_hrldas['noahlsm_offline'].update({'kday':new_duration})

    # Run the simulation
    candidate_perfrestart_run = perfrestart_sim.run(simulation_dir, 1,mode='a')

    #Check against initial run
    perfstart_restart_diffs = RestartDiffs(candidate_perfrestart_run,candidate_run)
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
def test_regression():
    #Check regression
    regression_diffs = RestartDiffs(candidate_run,reference_run)

    ## Check hydro restarts
    for diff in regression_diffs.hydro:
        assert diff == None

    ## Check lsm restarts
    for diff in regression_diffs.lsm:
        assert diff == None

    ## Check nudging restarts
    for diff in regression_diffs.nudging:
        assert diff == None
