import pathlib
from wrfhydropy.model_classes import *
import pickle
import deepdiff
import copy

##################################
# Directories for import and for test data, used for making expected datasets
# testDataDir = pathlib.Path('/home/docker/wrf_hydro_py/wrfhydro/tests/data')
##################################

##################################
# Domain object tests

#####
# NWM
#####

## Make expected data

# domain_top_dir= testDataDir / 'domain'
# domain_object = WrfHydroDomain(domain_top_dir=domain_top_dir,
#                                domain_config='NWM',
#                                model_version='v1.2.1')
# with open(testDataDir / 'expected/test_domain_nwm.pkl', 'wb') as f:
#     pickle.dump(domain_object, f, 2)

## Define test
def test_domain_nwm(datadir_copy):
    """The WrfHydroDomain class is constructed properly for NWM private"""
    # Load expected object
    expected_dir = datadir_copy["expected"]
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm.pkl',"rb"))

    # Generate new objects
    domain_top_dir = datadir_copy["domain"]
    domain_object = WrfHydroDomain(domain_top_dir=domain_top_dir,
                                   domain_config='NWM',
                                   model_version='v1.2.1')

    # Compare to expected to new
    diffs = deepdiff.DeepDiff(domain_object_expected,domain_object)
    assert diffs == {}

#####
# Public
#####

## Make expected data

# domain_top_dir= testDataDir / 'domain'
# domain_object = WrfHydroDomain(domain_top_dir=domain_top_dir,
#                                domain_config='NWM',
#                                model_version='v1.2.1')
# with open(testDataDir / 'expected/test_domain_nwm_public.pkl', 'wb') as f:
#     pickle.dump(domain_object, f, 2)

## Define test
def test_domain_nwm_public(datadir_copy):
    """The WrfHydroDomain class is constructed properly for NWM public"""
    # Load expected object
    expected_dir = datadir_copy["expected"]
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm_public.pkl',"rb"))

    # Generate new objects
    domain_top_dir = datadir_copy["domain"]
    domain_object = WrfHydroDomain(domain_top_dir=domain_top_dir,
                                   domain_config='NWM',
                                   model_version='v1.2.1')

    # Compare to expected to new
    diffs = deepdiff.DeepDiff(domain_object_expected,domain_object)
    assert diffs == {}

##################################

##################################
# Model object tests

#####
# NWM
#####

## Make expected data object

# wrf_hydro_nwm_dir = testDataDir / 'wrf_hydro_nwm'
# source_dir = wrf_hydro_nwm_dir / 'source'
# compile_dir = wrf_hydro_nwm_dir / 'compiled'
#
# ### Make precompile object
# model_object_precompile = WrfHydroModel(source_dir=source_dir)
#
# ### Make post compile object
# model_object_postcompile = copy.deepcopy(model_object_precompile)
# model_object_postcompile.compile('gfort',compile_dir = compile_dir,overwrite=True)
#
# ### Combine into one test object
# model_test_objects = {'model_object_precompile':model_object_precompile,
#                       'model_object_postcompile':model_object_postcompile}
#
# ### Pickle
# with open(testDataDir / 'expected/test_model_nwm.pkl', 'wb') as f:
#     pickle.dump(model_test_objects, f, 2)

## Define test
def test_model_nwm(datadir_copy):
    """WrfHydroModel object is able to compile NWM private"""
    # Load expected object
    expected_dir = datadir_copy["expected"]
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm.pkl',"rb"))

    # Generate new objects
    wrf_hydro_nwm_dir = datadir_copy['wrf_hydro_nwm']
    source_dir = wrf_hydro_nwm_dir / 'source'
    compile_dir = wrf_hydro_nwm_dir / 'compiled'

    # Make precompile object
    model_object_precompile = WrfHydroModel(source_dir=source_dir)

    # Make post compile object
    model_object_postcompile = copy.deepcopy(model_object_precompile)
    model_object_postcompile.compile('gfort',compile_dir=compile_dir,overwrite=True)

    # Compare to expected pre-compile object
    diffs_precompile = deepdiff.DeepDiff(model_objects_expected['model_object_precompile'],
                                model_object_precompile)
    assert diffs_precompile == {}

    # Compare to expected post-compile object, file paths will be different,so only check existence
    postcompile_expected = model_objects_expected['model_object_postcompile']

    # check that the model compiled successfully
    diffs_compile_options = deepdiff.DeepDiff(model_object_postcompile.compile_options,
                                     postcompile_expected.compile_options,
                                     ignore_order=True)
    assert diffs_compile_options == {}
    assert model_object_postcompile.compile_log.returncode == 0
    assert model_object_postcompile.wrf_hydro_exe.name == 'wrf_hydro.exe'

#####
# Public
#####

## Make expected data

# ### Setup directoires
# wrf_hydro_nwm_dir = testDataDir / 'wrf_hydro_nwm_public'
# source_dir = wrf_hydro_nwm_dir / 'source'
# compile_dir = wrf_hydro_nwm_dir / 'compiled'
#
# ### Make precompile model object
# model_object_precompile = WrfHydroModel(source_dir=source_dir)
#
# ### Make post compile object
# model_object_postcompile = copy.deepcopy(model_object_precompile)
# model_object_postcompile.compile('gfort',compile_dir = compile_dir,overwrite=True)
#
# ### Combine into one test object
# model_test_objects = {'model_object_precompile':model_object_precompile,
#                       'model_object_postcompile':model_object_postcompile}
#
# ### Pickle
# with open(testDataDir / 'expected/test_model_nwm_public.pkl', 'wb') as f:
#     pickle.dump(model_test_objects, f, 2)

## Define test
def test_model_nwm_public(datadir_copy):
    """WrfHydroModel object is able to compile NWM public"""

    # Load expected object
    expected_dir = datadir_copy["expected"]
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm_public.pkl',"rb"))

    # Generate new objects
    wrf_hydro_nwm_dir = datadir_copy['wrf_hydro_nwm_public']
    source_dir = wrf_hydro_nwm_dir / 'source'
    compile_dir = wrf_hydro_nwm_dir / 'compiled'

    # Make precompile object
    model_object_precompile = WrfHydroModel(source_dir=source_dir)

    # Make post compile object
    model_object_postcompile = copy.deepcopy(model_object_precompile)
    model_object_postcompile.compile('gfort',compile_dir=compile_dir,overwrite=True)

    # Compare to expected pre-compile object
    diffs_precompile = deepdiff.DeepDiff(model_objects_expected['model_object_precompile'],
                                model_object_precompile)
    assert diffs_precompile == {}

    # Compare to expected post-compile object, file paths will be different,so only check existence
    postcompile_expected = model_objects_expected['model_object_postcompile']

    # check that the model compiled successfully
    diffs_compile_options = deepdiff.DeepDiff(model_object_postcompile.compile_options,
                                     postcompile_expected.compile_options,
                                     ignore_order=True)
    assert diffs_compile_options == {}
    assert model_object_postcompile.compile_log.returncode == 0
    assert model_object_postcompile.wrf_hydro_exe.name == 'wrf_hydro.exe'

##################################
# Simuilation object tests

#####
# NWM
#####

## Make expected data

# ### Make prerun simulation object
# domain_object_expected = pickle.load(open(testDataDir / 'expected/test_domain_nwm.pkl', "rb"))
# model_objects_expected = pickle.load(open(testDataDir / 'expected/test_model_nwm.pkl', "rb"))
# model_object_postcompile_expected=model_objects_expected['model_object_postcompile']
#
# simulation_object = WrfHydroSim(model_object_postcompile_expected,domain_object_expected)
#
# ### Pickle
# with open(testDataDir / 'expected/test_simulation_nwm.pkl', 'wb') as f:
#     pickle.dump(simulation_object, f, 2)

# Define test
def test_simulation_nwm(datadir_copy):
    """WrfHydroSim object is constructed properly for NWM private"""

    # Load expected objects
    expected_dir = datadir_copy["expected"]
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm.pkl', "rb"))
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm.pkl', "rb"))
    simulation_object_expected = pickle.load(open(expected_dir / 'test_simulation_nwm.pkl',"rb"))

    # Setup a simulation
    model_object_postcompile_expected=model_objects_expected['model_object_postcompile']
    simulation_object = WrfHydroSim(model_object_postcompile_expected,domain_object_expected)

    # Compare to expected to new
    diffs = deepdiff.DeepDiff(simulation_object_expected,simulation_object)
    assert diffs == {}

#####
# Public
#####

## Make expected data

# ### Make prerun simulation object
# domain_object_expected = pickle.load(open(testDataDir / 'expected/test_domain_nwm_public.pkl',
#     "rb"))
# model_objects_expected = pickle.load(open(testDataDir / 'expected/test_model_nwm_public.pkl', "rb"))
# model_object_postcompile_expected=model_objects_expected['model_object_postcompile']
#
# simulation_object = WrfHydroSim(model_object_postcompile_expected,domain_object_expected)
#
# # Pickle
# with open(testDataDir / 'expected/test_simulation_nwm_public.pkl', 'wb') as f:
#     pickle.dump(simulation_object, f, 2)

# Define test
def test_simulation_nwm_public(datadir_copy):
    """WrfHydroSim object is constructed properly for NWM public"""

    # Load expected objects
    expected_dir = datadir_copy["expected"]
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm_public.pkl', "rb"))
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm_public.pkl', "rb"))
    simulation_object_expected = pickle.load(open(expected_dir / 'test_simulation_nwm_public.pkl',
                                                  "rb"))
    # Setup a simulation
    model_object_postcompile_expected=model_objects_expected['model_object_postcompile']
    simulation_object = WrfHydroSim(model_object_postcompile_expected,domain_object_expected)

    # Compare to expected to new
    diffs = deepdiff.DeepDiff(simulation_object_expected,simulation_object)
    assert diffs == {}

##################################
# Run object tests

#####
# NWM
#####

## Make expected data

# ### Get directories
# wrf_hydro_nwm_dir = testDataDir / 'wrf_hydro_nwm'
# run_dir = wrf_hydro_nwm_dir / 'run'
#
# ### Load the simulation object
# simulation_object = pickle.load(open(testDataDir / 'expected/test_simulation_nwm.pkl', "rb"))
#
# ### Run the model
# run_object = simulation_object.run(run_dir,mode='w')
#
# ### Pickle
# with open(testDataDir / 'expected/test_run_nwm.pkl', 'wb') as f:
#     pickle.dump(run_object, f, 2)

# Define test
def test_run_nwm(datadir_copy):
    """WrfHydroSim object is able to run NWM private"""

    # Load expected objects
    expected_dir = datadir_copy["expected"]
    run_object_expected = pickle.load(open(expected_dir / 'test_run_nwm.pkl', "rb"))
    simulation_object_expected = pickle.load(open(expected_dir / 'test_simulation_nwm.pkl',
                                             "rb"))

    # Generate new objects
    simulation_object = copy.deepcopy(simulation_object_expected)
    wrf_hydro_nwm_dir = datadir_copy['wrf_hydro_nwm']
    run_dir = wrf_hydro_nwm_dir / 'run'
    run_object = simulation_object.run(run_dir, mode='w')

    # Compare to the expected simulation to the post-run simulation to make sure nothing was
    # altered at run-time
    diffs = deepdiff.DeepDiff(simulation_object_expected,simulation_object)
    assert diffs == {}

    # Check that the model ran successfully
    assert run_object.run_log.returncode == 0

    ## Get names of restart files in expected run and check that names nad orders match
    hydro_restarts_expected=[]
    for restart_file in run_object_expected.restart_hydro:
        hydro_restarts_expected.append(restart_file.name)

    for i in range(len(hydro_restarts_expected)):
        assert run_object.restart_hydro[i].name == hydro_restarts_expected[i]

#####
# Public
#####

## Make expected data

# ### Get directories
# wrf_hydro_nwm_dir = testDataDir / 'wrf_hydro_nwm_public'
# run_dir = wrf_hydro_nwm_dir / 'run'
#
# ### Load the simulation object
# simulation_object = pickle.load(open(testDataDir / 'expected/test_simulation_nwm_public.pkl', "rb"))
#
# ### Run the model
# run_object = simulation_object.run(run_dir,mode='w')
#
# ### Pickle
# with open(testDataDir / 'expected/test_run_nwm_public.pkl', 'wb') as f:
#     pickle.dump(run_object, f, 2)

def test_run_nwm(datadir_copy):
    """WrfHydroSim object is able to run NWM public"""

    # Load expected objects
    expected_dir = datadir_copy["expected"]
    run_object_expected = pickle.load(open(expected_dir / 'test_run_nwm_public.pkl', "rb"))
    simulation_object_expected = pickle.load(open(expected_dir /
                                                  'test_simulation_nwm_public.pkl',
                                             "rb"))

    # Generate new objects
    simulation_object = copy.deepcopy(simulation_object_expected)
    wrf_hydro_nwm_dir = datadir_copy['wrf_hydro_nwm']
    run_dir = wrf_hydro_nwm_dir / 'run'
    run_object = simulation_object.run(run_dir, mode='w')

    # Compare to the expected simulation to the post-run simulation to make sure nothing was
    # altered at run-time
    diffs = deepdiff.DeepDiff(simulation_object_expected,simulation_object)
    assert diffs == {}

    # Check that the model ran successfully
    assert run_object.run_log.returncode == 0

    ## Get names of restart files in expected run and check that names nad orders match
    hydro_restarts_expected=[]
    for restart_file in run_object_expected.restart_hydro:
        hydro_restarts_expected.append(restart_file.name)

    for i in range(len(hydro_restarts_expected)):
        assert run_object.restart_hydro[i].name == hydro_restarts_expected[i]