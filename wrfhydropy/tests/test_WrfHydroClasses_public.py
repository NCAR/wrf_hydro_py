import pathlib
import pickle
import datetime
import deepdiff
import copy
import wrfhydropy
import shutil
from pprint import pprint
import pytest

##################################
# Setup


# # Make expected data
# # The reference/expected data for the tests is written by commented sections in this
# # code that begin with "# Make expected data"
# # Directories for import and for test data, used for making expected datasets.
# test_data_dir = pathlib.Path('/home/docker/wrf_hydro_py/wrfhydropy/tests/data/public')


# Make a temporary directory
# TODO(JLM): this is super opaque, can we describe in words where the tmp dir is made?
@pytest.fixture(scope='session')
def tmp_data_dir(tmpdir_factory):
    tmp_data_dir = tmpdir_factory.mktemp('tmp_data_public')
    shutil.copytree('data/public', tmp_data_dir / 'data', symlinks=True)
    return tmp_data_dir


##################################
# Domain object tests


# # Make expected data
# domain_top_dir= test_data_dir / 'domain'
# domain_object = wrfhydropy.WrfHydroDomain(
#     domain_top_dir=domain_top_dir,
#     domain_config='NWM',
#     model_version='v1.2.1'
# )
# with open(test_data_dir / 'expected/test_domain_nwm.pkl', 'wb') as f:
#     pickle.dump(domain_object, f, 2)


# Define test
def test_domain_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print(
            "\nQuestion: The WrfHydroDomain class is constructed properly for NWM public? ",
            end=''
        )

    # Set directory paths
    domain_top_dir = tmp_data_dir / 'data' / 'domain'
    expected_dir = tmp_data_dir / 'data' / 'expected'

    # Load expected object
    domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm.pkl', "rb"))

    # Generate new objects
    domain_object = wrfhydropy.WrfHydroDomain(
        domain_top_dir=domain_top_dir,
        domain_config='NWM',
        model_version='v1.2.1'
    )

    # Compare expected to new
    diffs = deepdiff.DeepDiff(domain_object_expected, domain_object)
    assert diffs == {}


##################################
# Model object tests


# # Make expected data objects: pre and post compile objects will be tested.
# wrf_hydro_nwm_dir = test_data_dir / 'wrf_hydro_nwm_public'
# source_dir = wrf_hydro_nwm_dir / 'source'
# compile_dir = wrf_hydro_nwm_dir / 'compiled'
#
# model_object_precompile = wrfhydropy.WrfHydroModel(
#     source_dir=source_dir,
#     model_config='NWM'
# )
#
# model_object_postcompile = copy.deepcopy(model_object_precompile)
# model_object_postcompile.compile('gfort',compile_dir = compile_dir,overwrite=True)
# # Because the actual test is done is a copied dir while the expected
# # result is constricuted under this repo, need to manually set this
# model_object_postcompile.git_hash = 'not-a-repo'
#
# model_test_objects = {'model_object_precompile':model_object_precompile,
#                       'model_object_postcompile':model_object_postcompile}
#
# with open(test_data_dir / 'expected/test_model_nwm.pkl', 'wb') as f:
#     pickle.dump(model_test_objects, f, 2)
#

# Define test
def test_model_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print(
            "\nQuestion: WrfHydroModel object is able to compile NWM public? ",
            end=''
        )

    # Setup directory paths
    expected_dir = tmp_data_dir / 'data' / 'expected'
    source_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm_public' / 'source'
    compile_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm_public' / 'compiled'

    # Load expected data objects
    model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm.pkl', "rb"))

    # Make precompile object
    model_object_precompile = wrfhydropy.WrfHydroModel(
        source_dir=str(source_dir),
        model_config='NWM'
    )

    # Make post compile object
    model_object_postcompile = copy.deepcopy(model_object_precompile)
    model_object_postcompile.compile('gfort',compile_dir=str(compile_dir),overwrite=True)

    # Compare to expected pre-compile object
    diffs_precompile = deepdiff.DeepDiff(
        model_objects_expected['model_object_precompile'],
        model_object_precompile
    )
    assert diffs_precompile == {}

    # Compare to expected post-compile object, file paths will be different,so only check existence
    postcompile_expected = model_objects_expected['model_object_postcompile']

    # check that the model compiled successfully
    diffs_compile_options = deepdiff.DeepDiff(
        model_object_postcompile.compile_options,
        postcompile_expected.compile_options,
        ignore_order=True
    )
    assert diffs_compile_options == {}
    assert model_object_postcompile.compile_log.returncode == 0
    assert model_object_postcompile.wrf_hydro_exe.name == 'wrf_hydro.exe'


##################################
# Setup object tests


# # make expected data
# domain_object_expected = pickle.load(open(test_data_dir / 'expected/test_domain_nwm.pkl', "rb"))
# model_objects_expected = pickle.load(open(test_data_dir / 'expected/test_model_nwm.pkl', "rb"))
# model_object_postcompile_expected=model_objects_expected['model_object_postcompile']
#
# setup_object = wrfhydropy.WrfHydroSetup(model_object_postcompile_expected,domain_object_expected)
# # Changing the restart freq is needed for the multi job run
# setup_object.namelist_hrldas['noahlsm_offline']['restart_frequency_hours'] = 6
# setup_object.hydro_namelist['hydro_nlist']['rst_dt'] = 360
#
# with open(test_data_dir / 'expected/test_setup_nwm.pkl', 'wb') as f:
#     pickle.dump(setup_object, f, 2)


# Define test
def test_setup_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print(
            "\nQuestion: WrfHydroSetup object is constructed properly for NWM public? ",
            end=''
        )

    # Set directory paths
    expected_dir = tmp_data_dir / 'data' / 'expected'
    compile_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm_public' / 'compiled'
    domain_top_dir = tmp_data_dir / 'data' / 'domain'

    # Load expected objects
    # model_objects_expected = pickle.load(open(expected_dir / 'test_model_nwm.pkl', "rb"))
    # domain_object_expected = pickle.load(open(expected_dir / 'test_domain_nwm.pkl', "rb"))
    setup_object_expected = pickle.load(open(expected_dir / 'test_setup_nwm.pkl', "rb"))

    # Load previous test artifacts
    model_object_postcompile = pickle.load(open(compile_dir / 'WrfHydroModel.pkl', 'rb'))

    # Setup a setup object
    domain_object = wrfhydropy.WrfHydroDomain(
        domain_top_dir=domain_top_dir,
        domain_config='NWM',
        model_version='v1.2.1'
    )
    #model_object_postcompile_expected = model_objects_expected['model_object_postcompile']
    setup_object = wrfhydropy.WrfHydroSetup(model_object_postcompile, domain_object)
    setup_object.namelist_hrldas['noahlsm_offline']['restart_frequency_hours'] = 6
    setup_object.hydro_namelist['hydro_nlist']['rst_dt'] = 360

    # Compare expected to new
    hydro_diffs = deepdiff.DeepDiff(
        setup_object_expected.hydro_namelist,
        setup_object.hydro_namelist
    )
    assert hydro_diffs == {}

    hrldas_diffs = deepdiff.DeepDiff(
        setup_object_expected.namelist_hrldas,
        setup_object.namelist_hrldas
    )
    assert hrldas_diffs == {}


##################################
# Job Object tests
# Note that on docker scheduler object tests are not really currently possible.

# Helper function
# This is kinda a lot of code to have in multiple places, so make the function.
def mk_job_list():
    time_0 = datetime.datetime(2011, 8, 26, 0, 0)
    # These choices of restart times match the restart frequencies set above.
    time_1 = time_0 + datetime.timedelta(hours=6)
    time_2 = time_1 + datetime.timedelta(hours=18)
    job_object_1 = wrfhydropy.Job(
        nproc=2,
        model_start_time=time_0,
        model_end_time=time_1
    )
    job_object_2 = wrfhydropy.Job(
        nproc=2,
        model_start_time=time_1,
        model_end_time=time_2
    )
    return [job_object_1, job_object_2]


# # Make expected data
# # Create a two-job list to pass to run for a two-job run.
# job_list = mk_job_list()
#
# with open(test_data_dir / 'expected/test_job_list_nwm.pkl', 'wb') as f:
#      pickle.dump(job_list, f, 2)
#
#
# def test_job_list_nwm(tmp_data_dir, capsys):
#     with capsys.disabled():
#         print(
#             "\nQuestion: Job object still the same on NWM public? ",
#             end=''
#         )
#
#     expected_dir = tmp_data_dir / 'data' / 'expected'
#     job_list_expected = pickle.load(open(expected_dir / 'test_job_list_nwm.pkl', "rb"))
#
#     job_list = mk_job_list()
#
#     job_diffs = deepdiff.DeepDiff(
#         job_list_expected,
#         job_list
#     )
#     assert job_diffs == {}


# #################################
# Run object tests


# # Make expected data
# wrf_hydro_nwm_dir = test_data_dir / 'wrf_hydro_nwm_public'
# run_dir = wrf_hydro_nwm_dir / 'run'
#
# setup_object = pickle.load(open(test_data_dir / 'expected/test_setup_nwm.pkl', "rb"))
# job_list = pickle.load(open(test_data_dir / 'expected/test_job_list_nwm.pkl', "rb"))
#
# run_object = wrfhydropy.WrfHydroRun(setup_object,
#                                     run_dir=run_dir,
#                                     jobs=job_list,
#                                     rm_existing_run_dir = True,
#                                     mode ='w')
#
# run_object_prerun = copy.deepcopy(run_object)
# run_object.run_jobs()
# run_object_postrun = run_object
#
# with open(test_data_dir / 'expected/test_run_nwm.pkl', 'wb') as f:
#     pickle.dump({'run_object_prerun': run_object_prerun,
#                  'run_object_postrun': run_object_postrun}, f, 2)


# Define test
def test_run_nwm(tmp_data_dir, capsys):
    with capsys.disabled():
        print(
            "\nQuestion: WrfHydroSetup object is able to run NWM public? ",
            end=''
        )

    # Set directory paths
    expected_dir = tmp_data_dir / 'data' / 'expected'
    compile_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm_public' / 'compiled'
    domain_top_dir = tmp_data_dir / 'data' / 'domain'
    run_dir = tmp_data_dir / 'data' / 'wrf_hydro_nwm_public' / 'run'

    # Load expected objects
    setup_object_expected = pickle.load(open(expected_dir / 'test_setup_nwm.pkl', "rb"))
    run_objects_expected = pickle.load(open(expected_dir / 'test_run_nwm.pkl', "rb"))
    job_list_expected = pickle.load(open(expected_dir / 'test_job_list_nwm.pkl', "rb"))

    # Load previous test artifacts
    model_object_postcompile = pickle.load(open(compile_dir / 'WrfHydroModel.pkl', 'rb'))

    # Setup a setup object
    domain_object = wrfhydropy.WrfHydroDomain(
        domain_top_dir=domain_top_dir,
        domain_config='NWM',
        model_version='v1.2.1'
    )
    # compare the domain object?

    setup_object = wrfhydropy.WrfHydroSetup(model_object_postcompile, domain_object)
    setup_object.namelist_hrldas['noahlsm_offline']['restart_frequency_hours'] = 6
    setup_object.hydro_namelist['hydro_nlist']['rst_dt'] = 360
    #assert setup_object == setup_object_expected
    # TODO JLM: why does this not check?

    job_list = mk_job_list()
    # assert job_list == job_list_expected
    # TODO(JLM): why does this not check?

    # Run the setup
    run_object = wrfhydropy.WrfHydroRun(
        setup_object,
        run_dir=run_dir,
        rm_existing_run_dir=True,
        jobs=job_list
    )

    prerun_diffs = deepdiff.DeepDiff(
        run_objects_expected['run_object_prerun'],
        run_object
    )

    # with capsys.disabled():
    #     #How to check that the diffs are actually allowable
    #     #pprint.pprint(prerun_diffs['values_changed'])
    #     pprint.pprint(prerun_diffs)
    #     #How to get the allowable diffs
    #     #pprint.pprint(prerun_diffs['values_changed'].keys())
    #     pprint.pprint(prerun_diffs.keys())

    allowable_prerun_diffs = set(
        ['root.jobs_pending[0].job_date_id',
         'root.jobs_pending[1].job_submission_time',
         'root.setup.model.compile_log.args[1]',
         'root.setup.model.object_id',
         'root.jobs_pending[1].job_date_id',
         'root.jobs_pending[0].job_submission_time',
         'root.setup.model.compile_log.stdout']
    )

    assert set(prerun_diffs.keys()) == set(['values_changed'])
    assert set(prerun_diffs['values_changed'].keys()) == allowable_prerun_diffs

    run_object.run_jobs()

    postrun_diffs = deepdiff.DeepDiff(
        run_objects_expected['run_object_postrun'],
        run_object
    )

    # with capsys.disabled():
        # How to check that the diffs are actually allowable
        # pprint.pprint(postrun_diffs['values_changed'])
        # How to get the allowable diffs
        # pprint.pprint(postrun_diffs['values_changed'].keys())
        
    allowable_postrun_diffs = set(
        ['root.jobs_completed[0].exe_cmd',
         'root.jobs_completed[0].job_submission_time',
         'root.jobs_completed[0].run_log.args[2]',
         'root.jobs_completed[1].job_submission_time',
         'root.jobs_completed[0].job_end_time',
         'root.object_id',
         'root.jobs_completed[0].job_date_id',
         'root.jobs_completed[0].job_start_time',
         'root.jobs_completed[1].job_date_id',
         'root.jobs_completed[1].job_end_time',
         'root.jobs_completed[1].run_log.args[2]',
         'root.jobs_completed[1].exe_cmd',
         'root.setup.model.compile_log.args[1]',
         'root.setup.model.compile_log.stdout',
         'root.jobs_completed[1].job_start_time',
         'root.setup.model.object_id']
    )

    assert set(postrun_diffs.keys()) == set(['values_changed'])
    assert set(postrun_diffs['values_changed'].keys()) == allowable_postrun_diffs
