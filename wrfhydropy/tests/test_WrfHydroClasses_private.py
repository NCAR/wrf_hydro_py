import pathlib
import pickle
import datetime
import deepdiff
import copy
import wrfhydropy
import shutil
import pytest


# # Make expected data
# # The reference/expected data for the tests is written by commented sections in this
# # code that begin with "# Make expected data"
# # Directories for import and for test data, used for making expected datasets.
# testDataDir = pathlib.Path('/home/docker/wrf_hydro_py/wrfhydropy/tests/data/private')

#Make a temporary directory
@pytest.fixture(scope='session')
def tmp_data_dir(tmpdir_factory):
    tmp_data_dir = tmpdir_factory.mktemp('tmp_data_private')
    shutil.copytree('data/private', tmp_data_dir / 'data', symlinks=True)
    return tmp_data_dir

##################################
# Domain object tests

#####
# PRIVATE
#####


# # Make expected data

# domain_top_dir= testDataDir / 'domain'
# domain_object = wrfhydropy.WrfHydroDomain(domain_top_dir=domain_top_dir,
#                                domain_config='NWM',
#                                model_version='v1.2.1')
# with open(testDataDir / 'expected/test_domain_nwm.pkl', 'wb') as f:
#     pickle.dump(domain_object, f, 2)


# Define test
def test_domain_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print("Question: The WrfHydroDomain class is constructed properly for NWM private?")

    # Set directory paths
    domain_top_dir = tmp_data_dir / 'data' / 'domain'
    expected_dir = tmp_data_dir / 'data' / 'expected'

    # Load expected object
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm.pkl',"rb"))

    # Generate new objects
    domain_object = wrfhydropy.WrfHydroDomain(domain_top_dir=domain_top_dir,
                                   domain_config='NWM',
                                   model_version='v1.2.1')

    # Compare expected to new
    diffs = deepdiff.DeepDiff(domain_object_expected,domain_object)
    assert diffs == {}


##################################
# Model object tests


# # Make expected data objects: pre and post compile objects will be tested.

# wrf_hydro_nwm_dir = testDataDir / 'wrf_hydro_nwm'
# source_dir = wrf_hydro_nwm_dir / 'source'
# compile_dir = wrf_hydro_nwm_dir / 'compiled'

# model_object_precompile = wrfhydropy.WrfHydroModel(
#     source_dir=source_dir,
#     model_config='NWM'
# )

# model_object_postcompile = copy.deepcopy(model_object_precompile)
# model_object_postcompile.compile('gfort',compile_dir = compile_dir,overwrite=True)

# model_test_objects = {'model_object_precompile':model_object_precompile,
#                       'model_object_postcompile':model_object_postcompile}

# with open(testDataDir / 'expected/test_model_nwm.pkl', 'wb') as f:
#     pickle.dump(model_test_objects, f, 2)


# Define test
def test_model_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print("Question: WrfHydroModel object is able to compile NWM private?")

    # Setup directory paths
    expected_dir = tmp_data_dir / 'data' / 'expected'
    source_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm' / 'source'
    compile_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm' / 'compiled'

    # Load expected data objects
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm.pkl',"rb"))

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
# Setup object tests


# # Make expected data

# domain_object_expected = pickle.load(open(testDataDir / 'expected/test_domain_nwm.pkl', "rb"))
# model_objects_expected = pickle.load(open(testDataDir / 'expected/test_model_nwm.pkl', "rb"))
# model_object_postcompile_expected=model_objects_expected['model_object_postcompile']

# setup_object = wrfhydropy.WrfHydroSetup(model_object_postcompile_expected,domain_object_expected)
# # Changing the restart freq is needed for the multi job run
# setup_object.namelist_hrldas['noahlsm_offline']['restart_frequency_hours'] = 6
# setup_object.hydro_namelist['hydro_nlist']['rst_dt'] = 360

# with open(testDataDir / 'expected/test_setup_nwm.pkl', 'wb') as f:
#     pickle.dump(setup_object, f, 2)


# Define test
def test_setup_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print("Question: WrfHydroSim object is constructed properly for NWM private?")

    # Set directory paths
    expected_dir = tmp_data_dir / 'data' / 'expected'
    compile_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm' / 'compiled'
    domain_top_dir = tmp_data_dir / 'data' / 'domain'

    # Load expected objects
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm.pkl', "rb"))
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm.pkl', "rb"))
    simulation_object_expected = pickle.load(open(expected_dir / 'test_simulation_nwm.pkl',"rb"))

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
# Job Object tests
# Note that on docker scheduler object tests are not really currently possible.


# # Make expected data
# # Create two job list to pass to run for a two-job run.
# time_0 = datetime.datetime(2011, 8, 26, 0, 0)
# time_1 = time_0 + datetime.timedelta(hours=6)
# time_2 = time_1 + datetime.timedelta(hours=18)

# job_object_1 = wrfhydropy.Job(
#     nproc=2,
#     model_start_time=time_0,
#     model_end_time=time_1
# )

# job_object_2 = wrfhydropy.Job(
#     nproc=2,
#     model_start_time=time_1,
#     model_end_time=time_2
# )

# job_list = [job_object_1, job_object_2]

# with open(testDataDir / 'expected/test_job_list_nwm.pkl', 'wb') as f:
#      pickle.dump(job_list, f, 2)

#def test_job_list_nwm(tmp_data_dir, capsys):


##################################
# Run object tests


# # Make expected data

# wrf_hydro_nwm_dir = testDataDir / 'wrf_hydro_nwm'
# run_dir = wrf_hydro_nwm_dir / 'run'

# setup_object = pickle.load(open(testDataDir / 'expected/test_setup_nwm.pkl', "rb"))
# job_list = pickle.load(open(testDataDir / 'expected/test_job_list_nwm.pkl', "rb"))

# run_object = wrfhydropy.WrfHydroRun(setup_object, run_dir=run_dir, jobs=job_list)
# run_object_prerun = copy.deepcopy(run_object)
# run_object.run_jobs()
# run_object_postrun = run_object

# with open(testDataDir / 'expected/test_run_nwm.pkl', 'wb') as f:
#     pickle.dump([run_object_prerun, run_object_postrun], f, 2)


# Define test
def test_run_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print("Question: WrfHydroSim object is able to run NWM private?")

    # Set directory paths
    expected_dir = tmp_data_dir / 'data' / 'expected'
    compile_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm' / 'compiled'
    domain_top_dir = tmp_data_dir / 'data' / 'domain'
    run_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm' / 'run'

    # Load expected objects
    simulation_object_expected = pickle.load(open(expected_dir / 'test_simulation_nwm.pkl',"rb"))
    run_object_expected = pickle.load(open(expected_dir / 'test_run_nwm.pkl',"rb"))

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
