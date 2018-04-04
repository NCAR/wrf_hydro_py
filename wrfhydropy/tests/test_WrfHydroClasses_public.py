import pathlib
import pickle
import deepdiff
import copy
import wrfhydropy
import shutil
import pytest
##################################
# Directories for import and for test data, used for making expected datasets
# testDataDir = pathlib.Path('/home/docker/wrf_hydro_py/wrfhydropy/tests/data/public')
##################################

#Make a temporary directory
@pytest.fixture(scope='session')
def tmp_data_dir_public(tmpdir_factory):
    tmp_data_dir = tmpdir_factory.mktemp('tmp_data_public')
    shutil.copytree('data/public',tmp_data_dir / 'data',symlinks=True)
    return tmp_data_dir

##################################
# Domain object tests

#####
# PUBLIC
#####

## Make expected data

# domain_top_dir= testDataDir / 'domain'
# domain_object = wrfhydropy.WrfHydroDomain(domain_top_dir=domain_top_dir,
#                                domain_config='NWM',
#                                model_version='v1.2.1')
# with open(testDataDir / 'expected/test_domain_nwm_public.pkl', 'wb') as f:
#     pickle.dump(domain_object, f, 2)

# Define test
def test_domain_nwm_public(tmp_data_dir_public,capsys):
    with capsys.disabled():
        print("Question: The WrfHydroDomain class is constructed properly for NWM public?")

    # Set directory paths
    domain_top_dir = tmp_data_dir_public / 'data' / 'domain'
    expected_dir = tmp_data_dir_public / 'data' / 'expected'

    # Load expected object
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm_public.pkl',"rb"))

    # Generate new objects
    domain_object = wrfhydropy.WrfHydroDomain(domain_top_dir=domain_top_dir,
                                   domain_config='NWM',
                                   model_version='v1.2.1')

    # Compare expected to new
    diffs = deepdiff.DeepDiff(domain_object_expected,domain_object)
    assert diffs == {}

##################################
# Model object tests

## Make expected data object

# wrf_hydro_nwm_dir = testDataDir / 'wrf_hydro_nwm_public'
# source_dir = wrf_hydro_nwm_dir / 'source'
# compile_dir = wrf_hydro_nwm_dir / 'compiled'
#
# ### Make precompile object
# model_object_precompile = wrfhydropy.WrfHydroModel(source_dir=source_dir)
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
def test_model_nwm_public(tmp_data_dir_public,capsys):
    with capsys.disabled():
        print("Question: WrfHydroModel object is able to compile NWM public?")

    # Setup directory paths
    expected_dir = tmp_data_dir_public / 'data' / 'expected'
    source_dir = tmp_data_dir_public / 'data' / 'wrf_hydro_nwm_public' / 'source'
    compile_dir = tmp_data_dir_public / 'data' / 'wrf_hydro_nwm_public' / 'compiled'

    # Load expected data objects
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm_public.pkl',"rb"))

    # Make precompile object
    model_object_precompile = wrfhydropy.WrfHydroModel(source_dir=str(source_dir))

    # Make post compile object
    model_object_postcompile = copy.deepcopy(model_object_precompile)
    model_object_postcompile.compile('gfort',compile_dir=str(compile_dir),overwrite=True)

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

## Make expected data

# ### Make prerun simulation object
# domain_object_expected = pickle.load(open(testDataDir / 'expected/test_domain_nwm_public.pkl', "rb"))
# model_objects_expected = pickle.load(open(testDataDir / 'expected/test_model_nwm_public.pkl', "rb"))
# model_object_postcompile_expected=model_objects_expected['model_object_postcompile']
#
# simulation_object = wrfhydropy.WrfHydroSim(model_object_postcompile_expected,domain_object_expected)
#
# ### Pickle
# with open(testDataDir / 'expected/test_simulation_nwm_public.pkl', 'wb') as f:
#     pickle.dump(simulation_object, f, 2)

# Define test
def test_simulation_nwm_public(tmp_data_dir_public,capsys):
    with capsys.disabled():
        print("Question: WrfHydroSim object is constructed properly for NWM public?")

    # Set directory paths
    expected_dir = tmp_data_dir_public / 'data' / 'expected'
    compile_dir = tmp_data_dir_public / 'data' / 'wrf_hydro_nwm_public' / 'compiled'
    domain_top_dir = tmp_data_dir_public / 'data' / 'domain'

    # Load expected objects
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm_public.pkl', "rb"))
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm_public.pkl', "rb"))
    simulation_object_expected = pickle.load(open(expected_dir / 'test_simulation_nwm_public.pkl',"rb"))

    # Load previous test artifacts
    model_object_postcompile = pickle.load(open(compile_dir / 'WrfHydroModel.pkl','rb'))

    # Setup a simulation
    domain_object = wrfhydropy.WrfHydroDomain(domain_top_dir=domain_top_dir,
                                   domain_config='NWM',
                                   model_version='v1.2.1')
    model_object_postcompile_expected=model_objects_expected['model_object_postcompile']
    simulation_object = wrfhydropy.WrfHydroSim(model_object_postcompile,domain_object)

    # Compare expected to new
    hydro_diffs = deepdiff.DeepDiff(simulation_object_expected.hydro_namelist,
                              simulation_object.hydro_namelist)
    assert hydro_diffs == {}
    hrldas_diffs = deepdiff.DeepDiff(simulation_object_expected.namelist_hrldas,
                              simulation_object.namelist_hrldas)
    assert hrldas_diffs == {}

##################################
# Run object tests

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

# Define test
def test_run_nwm_public(tmp_data_dir_public,capsys,):
    with capsys.disabled():
        print("Question: WrfHydroSim object is able to run NWM public?")

    # Set directory paths
    expected_dir = tmp_data_dir_public / 'data' / 'expected'
    compile_dir = tmp_data_dir_public / 'data' / 'wrf_hydro_nwm_public' / 'compiled'
    domain_top_dir = tmp_data_dir_public / 'data' / 'domain'
    run_dir = tmp_data_dir_public / 'data' / 'wrf_hydro_nwm_public' / 'run'

    # Load expected objects
    simulation_object_expected = pickle.load(open(expected_dir / 'test_simulation_nwm_public.pkl',"rb"))
    run_object_expected = pickle.load(open(expected_dir / 'test_run_nwm_public.pkl',"rb"))

    # Load previous test artifacts
    model_object_postcompile = pickle.load(open(compile_dir / 'WrfHydroModel.pkl','rb'))

    # Setup a simulation
    domain_object = wrfhydropy.WrfHydroDomain(domain_top_dir=domain_top_dir,
                                   domain_config='NWM',
                                   model_version='v1.2.1')

    simulation_object = wrfhydropy.WrfHydroSim(model_object_postcompile,domain_object)

    # Run the sim
    run_object = simulation_object.run(run_dir, mode='w')

    # Compare to the expected simulation to the post-run simulation to make sure nothing in the
    # namelists was altered at run-time
    hydro_diffs = deepdiff.DeepDiff(simulation_object_expected.hydro_namelist,
                              simulation_object.hydro_namelist)
    assert hydro_diffs == {}
    hrldas_diffs = deepdiff.DeepDiff(simulation_object_expected.namelist_hrldas,
                              simulation_object.namelist_hrldas)
    assert hrldas_diffs == {}

    # Check that the model ran successfully
    assert run_object.run_log.returncode == 0

    ## Get names of restart files in expected run and check that names nad orders match
    hydro_restarts_expected=[]
    for restart_file in run_object_expected.restart_hydro:
        hydro_restarts_expected.append(restart_file.name)

    for i in range(len(hydro_restarts_expected)):
        assert run_object.restart_hydro[i].name == hydro_restarts_expected[i]
