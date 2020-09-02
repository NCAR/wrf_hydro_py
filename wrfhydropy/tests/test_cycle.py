import copy
import datetime
import deepdiff
import os
import pathlib
import pandas
import pytest
import shutil
import string
import timeit

from wrfhydropy.core.simulation import Simulation
from wrfhydropy.core.ensemble import EnsembleSimulation
from wrfhydropy.core.cycle import CycleSimulation

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))
node_file = test_dir / 'data/nodefile_pbs_example_copy.txt'

n_members = 2


def check_first_line(file, answer):
    with open(file) as f:
        first_line = f.readline()
        assert first_line == answer


def sub_tmpdir(the_string, tmpdir, pattern='<<tmpdir>>'):
    return the_string.replace(pattern, str(tmpdir))


@pytest.fixture(scope='function')
def init_times():
    some_time = datetime.datetime(2012, 12, 12, 0, 0)
    init_times = [some_time + datetime.timedelta(dd) for dd in range(0, 9, 3)]
    return init_times


@pytest.fixture(scope='function')
def restart_dirs(init_times):
    restart_dirs = ['.'] * len(init_times)
    return restart_dirs


@pytest.fixture(scope='function')
def restart_dirs_ensemble(init_times):
    restart_dirs_ensemble = [
        ['.'] * n_members,
        ['-72'] * n_members,
        ['../dummy_extant_dir'] * n_members
    ]
    return restart_dirs_ensemble


@pytest.fixture(scope='function')
def simulation(model, domain):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    return sim


@pytest.fixture(scope='function')
def simulation_compiled(model, domain, job_restart, tmpdir):
    sim_dir = pathlib.Path(tmpdir).joinpath('sim_compiled_dir')
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job_restart)
    os.mkdir(sim_dir)
    os.chdir(sim_dir)
    sim.compose()
    return sim


@pytest.fixture(scope='function')
def ensemble(model, domain, simulation_compiled):
    ens = EnsembleSimulation()
    ens.add(simulation_compiled)
    ens.replicate_member(n_members)
    return ens


base_time = datetime.datetime(2012, 12, 12, 0, 0)


@pytest.mark.parametrize(
    ['init_times', 'expected'],
    [
        (
            [base_time + datetime.timedelta(dd) for dd in range(0, 9, 3)],
            [base_time + datetime.timedelta(dd) for dd in range(0, 9, 3)]
        ),
        (
            [base_time, base_time, 'nondatetime object'],
            ["List object not all datetime.datetime objects, as expected"]
        ),
        (
            [base_time, base_time],
            ['Length of forcing_dirs does not match that of init_times.']
        )
    ]
)
def test_add_init_times(
    init_times,
    expected,
    restart_dirs
):
    try:
        cycle = CycleSimulation(
            init_times=init_times,
            restart_dirs=restart_dirs,
            forcing_dirs=['.', '.', '.']
        )
        result = cycle._init_times
    except Exception as e:
        result = [str(e)]  # Cludgy but it works

    assert result == expected


@pytest.mark.parametrize(
    ['restart_dirs', 'expected'],
    [(['.', '.', '.'],  # normal
      [pathlib.PosixPath('.')] * 3),
     (['.', '/foo/bar', -1],  # mixed, should pass
      [pathlib.PosixPath('.'), pathlib.PosixPath('/foo/bar'), pathlib.PosixPath('-1')]),
     (['.', '.', ['.']],  # improper mix, should not pass
      ['Types in restart_dirs argument are not appropriate.']),
     (['.', '.'],  # wrong length
      ['Length of restart_dirs does not match that of init_times.']),
     ([['.', '.'], ['.', '/foo/bar'], ['.', '-1']],  # Ensemble, should pass
      [[pathlib.PosixPath('.')] * n_members,
       [pathlib.PosixPath('.'), pathlib.PosixPath('/foo/bar')],
       [pathlib.PosixPath('.'), pathlib.PosixPath('-1')]]),
     ([['.', ['.']], ['.', '/foo/bar'], ['.', '-1']],  # improper mix, fails
      ['Types in ensemble restart_dirs argument are not appropriate.']),
     ([['.', '.'], ['.', '/foo/bar'], ['.', '-1', '.']],  # ensemble wrong lengths
      ['Inconsistent ensemble length by implied by restart_dirs'])]
)
def test_add_restart_dirs(
    restart_dirs,
    expected,
    init_times
):
    try:
        cycle = CycleSimulation(
            init_times=init_times,
            restart_dirs=restart_dirs,
            forcing_dirs=['.', '.', '.']
        )
        result = cycle._restart_dirs
    except Exception as e:
        result = [str(e)]  # Cludgy but it works
    assert result == expected


@pytest.mark.parametrize(
    ['forcing_dirs', 'expected'],
    [(['.', '.', '.'],  # normal
      [pathlib.PosixPath('.')] * 3),
     (['.', '/foo/bar', -1],  # mixed, should pass
      [pathlib.PosixPath('.'), pathlib.PosixPath('/foo/bar'), pathlib.PosixPath('-1')]),
     (['.', '.', ['.']],  # improper mix, should not pass
      ['Types in forcing_dirs argument are not appropriate.']),
     (['.', '.'],  # wrong length
      ['Length of forcing_dirs does not match that of init_times.']),
     ([['.', '.'], ['.', '/foo/bar'], ['.', '-1']],  # Ensemble, should pass
      [[pathlib.PosixPath('.')] * n_members,
       [pathlib.PosixPath('.'), pathlib.PosixPath('/foo/bar')],
       [pathlib.PosixPath('.'), pathlib.PosixPath('-1')]]),
     ([['.', '.', ['.']], ['.', '.', '/foo/bar'], ['.', '.', '-1']],  # improper mix, fails
      ['Types in ensemble forcing_dirs argument are not appropriate.']),
     ([['.', '.', '.'], ['.', '.', '/foo/bar'], ['.', '-1']],  # ensemble wrong lengths
      ['Inconsistent ensemble length by implied by forcing_dirs'])]
)
def test_add_forcing_dirs(
    forcing_dirs,
    expected,
    restart_dirs,
    init_times
):
    try:
        cycle = CycleSimulation(
            init_times=init_times,
            restart_dirs=restart_dirs,
            forcing_dirs=forcing_dirs
        )
        result = cycle._forcing_dirs
    except Exception as e:
        result = [str(e)]  # Cludgy but it works
    assert result == expected


def test_cycle_init(init_times, restart_dirs):
    cycle = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs
    )
    assert type(cycle) is CycleSimulation
    # Not sure why this dosent vectorize well.
    atts = ['_addforcingdirs', '_addinittimes', '_addjob', '_addrestartdirs',
            '_addscheduler', '_addsimulation', '_forcing_dirs', '_init_times',
            '_job', '_restart_dirs', '_scheduler', 'add', 'casts', 'compose',
            'ncores', 'pickle', 'rm_casts', 'run']
    for kk in cycle.__dict__.keys():
        assert kk in atts


def test_cycle_addsimulation(
    simulation,
    job_restart,
    scheduler,
    simulation_compiled,
    init_times,
    restart_dirs
):
    sim = 'not a simulation object'
    cy1 = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs
    )
    with pytest.raises(Exception) as e_info:
        cy1.add(sim)
    assert str(e_info.value) == 'Object is not of a type expected for a CycleSimulation.'

    sim = simulation
    cy1 = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs
    )
    # This sim does not have the required pre-compiled model
    # with pytest.raises(Exception) as e_info:
    cy1.add(sim)
    # assert str(e_info.value) == \
    #    'Only Simulations with compiled model objects can be added to an ensemble simulation.'

    sim_compiled = simulation_compiled

    # cant add a list, even if pre-compiled
    with pytest.raises(Exception) as e_info:
        cy1.add([sim_compiled])
    assert str(e_info.value) == 'Object is not of a type expected for a CycleSimulation.'

    cy1.add(sim_compiled)
    assert isinstance(cy1._simulation, Simulation)

    # add a sim with job and make sure it is deleted.
    sim_compiled.add(job_restart)
    sim_compiled.add(scheduler)
    cy2 = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs
    )
    cy2.add(sim_compiled)
    assert cy2._simulation.jobs == []
    assert cy2._simulation.scheduler is None


@pytest.mark.parametrize(
    ['restart_dirs', 'expected'],
    [(
        ['.'] * 3,
        {'lsm': ['./NWM/RESTART/RESTART.2011082600_DOMAIN1',
                 './NWM/RESTART/RESTART.2011082600_DOMAIN1',
                 './NWM/RESTART/RESTART.2011082600_DOMAIN1'],
         'hyd': ['./NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
                 './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
                 './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1']}
     ),
     (
        ['0', '-72', '-72'],
        {'lsm': ['../cast_2012121200/RESTART.2012121200_DOMAIN1',
                 '../cast_2012121200/RESTART.2012121500_DOMAIN1',
                 '../cast_2012121500/RESTART.2012121800_DOMAIN1'],
         'hyd': ['../cast_2012121200/HYDRO_RST.2012-12-12_00:00_DOMAIN1',
                 '../cast_2012121200/HYDRO_RST.2012-12-15_00:00_DOMAIN1',
                 '../cast_2012121500/HYDRO_RST.2012-12-18_00:00_DOMAIN1']}
     ),
     (
        ['.', '-72', '../dummy_extant_dir'],
        {'lsm': ['./NWM/RESTART/RESTART.2011082600_DOMAIN1',
                 '../cast_2012121200/RESTART.2012121500_DOMAIN1',
                 '<<tmpdir>>/dummy_extant_dir/RESTART.2012121800_DOMAIN1'],
         'hyd': ['./NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
                 '../cast_2012121200/HYDRO_RST.2012-12-15_00:00_DOMAIN1',
                 '<<tmpdir>>/dummy_extant_dir/HYDRO_RST.2012-12-18_00:00_DOMAIN1']}
     ),
     (
        ['/foo/bar', '-72', '-72'],
        ['No such restart directory: /foo/bar']
     ),
     (
        ['../dummy_extant_dir', '-72', '72'],
        ['Only non-negative integers can be used to specify restart_dirs']
     )]
)
def test_cycle_addsimulation_translate(
    restart_dirs,
    expected,
    init_times,
    simulation_compiled,
    job_restart,
    tmpdir
):
    os.chdir(tmpdir)
    sim = simulation
    cy1 = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs
    )

    sim_compiled = simulation_compiled
    cy1.add(sim_compiled)
    cy1.add(job_restart)

    # translation happens on compose.
    try:
        os.mkdir(tmpdir / 'compose')
        os.chdir(tmpdir / 'compose')
        # This may be use in some of the tests
        pathlib.Path('../dummy_extant_dir').touch()
        cy1.compose(rm_casts_from_memory=False)
        lsm_keys = ['noahlsm_offline', 'restart_filename_requested']
        hyd_keys = ['hydro_nlist', 'restart_file']
        result = {
            'lsm': [cast.base_hrldas_namelist[lsm_keys[0]][lsm_keys[1]] for cast in cy1.casts],
            'hyd': [cast.base_hydro_namelist[hyd_keys[0]][hyd_keys[1]] for cast in cy1.casts]
        }
    except Exception as e:
        result = [str(e)]  # Cludgy but it works

    if isinstance(expected, dict):
        for key, v_list in expected.items():
            for ii, path in enumerate(v_list):
                expected[key][ii] = sub_tmpdir(path, tmpdir)

    assert result == expected


def test_cycle_addensemble(
    ensemble,
    job_restart,
    scheduler,
    init_times,
    restart_dirs,
    restart_dirs_ensemble
):
    # The ensemble necessarily has a compiled model (unlike a Simulation).
    # That is a separate test.

    cy1 = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs
    )
    with pytest.raises(Exception) as e_info:
        cy1.add(ensemble)
    assert str(e_info.value) == \
        'An ensemble cycle simulation requires the restart_dirs to be a list of lists.'

    cy1 = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble
    )
    cy1.add(ensemble)
    assert isinstance(cy1._ensemble, EnsembleSimulation)

    # add an ens with a job and make sure it is deleted.
    ens = ensemble
    ens.add(job_restart)
    ens.add(scheduler)
    cy2 = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble
    )
    cy2.add(ensemble)
    assert cy2._ensemble.jobs == []
    assert cy2._ensemble.scheduler is None


def test_cycle_addjob(job_restart, init_times, restart_dirs):
    cy1 = CycleSimulation(init_times=init_times, restart_dirs=restart_dirs)
    cy1.add(job_restart)
    assert deepdiff.DeepDiff(cy1._job, job_restart) == {}

    job_restart.job_id = 'a_different_id'
    cy1.add(job_restart)
    assert deepdiff.DeepDiff(cy1._job, job_restart) == {}


def test_cycle_addscheduler(scheduler, init_times, restart_dirs):
    cy1 = CycleSimulation(init_times=init_times, restart_dirs=restart_dirs)
    cy1.add(scheduler)
    assert deepdiff.DeepDiff(cy1._scheduler, scheduler) == {}

    sched2 = copy.deepcopy(scheduler)
    sched2.nnodes = 99
    cy1.add(sched2)
    assert deepdiff.DeepDiff(cy1._scheduler, sched2) == {}


def test_cycle_length(
    simulation_compiled,
    init_times,
    restart_dirs
):
    sim = simulation_compiled
    cy1 = CycleSimulation(init_times=init_times, restart_dirs=restart_dirs)
    cy1.add(sim)
    assert len(cy1) == len(init_times)
    # How to assert an error?
    # assert cy1.replicate_member(4) == "WTF mate?"


# @pytest.mark.parametrize when https://github.com/pytest-dev/pytest/issues/349
# Looks like it is close...
def test_cycle_compose(
    simulation,
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    init_times,
    restart_dirs
):
    # These might be parametizable.

    # Compose without adding a simulation.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        ncores=2
    )
    cy.add(job_restart)
    # Adding the scheduler ruins the run in CI.
    # cy.add(scheduler)
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == \
        'Unable to compose, current working directory is not empty. \n' + \
        'Change working directory to an empty directory with os.chdir()'

    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_no_sim_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == 'The cycle does not contain a _simulation or an _ensemble.'

    # Length zero cycle compose.
    cy = CycleSimulation(
        init_times=[],
        restart_dirs=[],
        ncores=1
    )
    cy.add(job_restart)
    cy.add(simulation_compiled)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_sim0_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == "There are no casts (init_times) to compose."

    # This simultion is not compiled. It compiles and composes successfully.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        ncores=1
    )
    cy.add(job_restart)
    cy.add(simulation)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_uncompiled_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    cy.compose()

    # Valid force dir exercise.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        ncores=1,
        forcing_dirs=['.', -72, '../dummy_extant_dir']
    )
    cy.add(job_restart)
    cy.add(simulation)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_forc_dir_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    compose_dir.joinpath('../dummy_extant_dir').touch()
    cy.compose()

    # In valid force dir exercise.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        ncores=1,
        forcing_dirs=['.', 72, '../dummy_extant_dir']
    )
    cy.add(job_restart)
    cy.add(simulation)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_forc_dir_fail_2_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == 'Only non-negative integers can be used to specify forcing_dirs'

    # In valid force dir exercise.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        ncores=1,
        forcing_dirs=['.', 'dummy_non-extant_dir', -72]
    )
    cy.add(job_restart)
    cy.add(simulation)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_forc_dir_fail_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == 'No such forcing directory: dummy_non-extant_dir'


@pytest.mark.xfail(strict=False)
def test_cycle_parallel_compose(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    init_times,
    restart_dirs
):
    """ A more comprehensive test of the object composed."""
    # A compiled simulation passed. Successfull compose in parallel.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        ncores=2
    )
    cy.add(job_restart)
    cy.add(simulation_compiled)

    # Make a copy where we keep the casts in memory for checking.
    cy_check_casts = copy.deepcopy(cy)

    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    cy.compose()

    cy_run_success = cy.run()
    assert cy_run_success == 0
    cy.pickle(str(pathlib.Path(tmpdir) / 'cycle_compose/WrfHydroCycleSim.pkl'))

    # The cycle-in-memory version for checking the casts.
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_compose_check_casts')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    cy_check_casts.compose(rm_casts_from_memory=False)

    # The job gets heavily modified on compose.
    answer = {
        '_entry_cmd': 'bogus entry cmd',
        '_exe_cmd': './wrf_hydro.exe',
        '_exit_cmd': 'bogus exit cmd',
        '_hrldas_namelist': {
            'noahlsm_offline': {
                'btr_option': 1,
                'canopy_stomatal_resistance_option': 1,
                'hrldas_setup_file': './NWM/DOMAIN/wrfinput_d01.nc',
                'indir': './FORCING',
                'output_timestep': 86400,
                'restart_filename_requested': './NWM/RESTART/RESTART.2011082600_DOMAIN1',
                'restart_frequency_hours': 24
            },
            'wrf_hydro_offline': {
                'forc_typ': 1
            }
        },
        '_hrldas_times': {
            'noahlsm_offline': {
                'khour': 282480,
                'restart_frequency_hours': 24,
                'output_timestep': 86400,
                'restart_filename_requested': 'NWM/RESTART/RESTART.2013101300_DOMAIN1',
                'start_day': 12,
                'start_hour': 00,
                'start_min': 00,
                'start_month': 12,
                'start_year': 2012
            }
        },
        '_hydro_namelist': {
            'hydro_nlist': {
                'aggfactrt': 4,
                'channel_option': 2,
                'chanobs_domain': 0,
                'chanrtswcrt': 1,
                'chrtout_domain': 1,
                'geo_static_flnm': './NWM/DOMAIN/geo_em.d01.nc',
                'restart_file': './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
                'udmp_opt': 1,
                'rst_dt': 1440,
                'out_dt': 1440
            },
            'nudging_nlist': {
                'maxagepairsbiaspersist': 3,
                'minnumpairsbiaspersist': 1,
                'nudginglastobsfile': './NWM/RESTART/nudgingLastObs.2011-08-26_00:00:00.nc'
            }
        },
        '_hydro_times': {
            'hydro_nlist': {
                'out_dt': 1440,
                'rst_dt': 1440,
                'restart_file': 'NWM/RESTART/HYDRO_RST.2013-10-13_00:00_DOMAIN1'
            },
            'nudging_nlist': {
                'nudginglastobsfile': 'NWM/RESTART/nudgingLastObs.2013-10-13_00:00:00.nc'
            }
        },
        '_job_end_time': None,
        '_job_start_time': None,
        '_job_submission_time': None,
        '_model_end_time': pandas.Timestamp('2045-03-04 00:00:00'),
        '_model_start_time': pandas.Timestamp('2012-12-12 00:00:00'),
        'exit_status': None,
        'job_id': 'test_job_1',
        'restart_freq_hr_hydro': None,
        'restart_freq_hr_hrldas': None,
        'output_freq_hr_hydro': None,
        'output_freq_hr_hrldas': None,
        'restart': True,
        'restart_dir': None,
        '_restart_dir_hydro': None,
        '_restart_dir_hrldas': None,
        'restart_file_time': '2013-10-13',
        '_restart_file_time_hydro': pandas.Timestamp('2013-10-13 00:00:00'),
        '_restart_file_time_hrldas': pandas.Timestamp('2013-10-13 00:00:00')
    }

    # For the cycle where the compse retains the casts...

    # This fails:
    # deepdiff.DeepDiff(answer, cy.casts[0].jobs[0].__dict__)
    # Instead, iterate on keys to "declass":
    for kk in cy_check_casts.casts[0].jobs[0].__dict__.keys():
        assert cy_check_casts.casts[0].jobs[0].__dict__[kk] == answer[kk]
    # Check the scheduler too
    # assert cy_check_casts.casts[0].scheduler.__dict__ == scheduler.__dict__

    # For the cycle where the compse removes the casts...

    # Check that the casts are all now simply pathlib objects
    assert all([type(mm) is str for mm in cy.casts])

    # the tmpdir gets nuked after the test... ?
    # Test the cast pickle size in terms of load speed.
    # Note that the deletion of the model, domain, and output objects are
    # done for the casts regardless of not removing the casts
    # from memory (currently).
    os.chdir(str(pathlib.Path(tmpdir) / 'cycle_compose/cast_2012121200'))
    time_taken = timeit.timeit(
        setup='import pickle',
        stmt='pickle.load(open("WrfHydroSim.pkl","rb"))',
        number=10000
    )
    # If your system is busy, this could take longer... and spuriously fail the test.
    # Notes(JLM): coverage is the limiting factor
    assert time_taken < 1.5

    # Test the cycle pickle size in terms of load speed.
    os.chdir(str(pathlib.Path(tmpdir) / 'cycle_compose/'))
    time_taken = timeit.timeit(
        setup='import pickle',
        stmt='pickle.load(open("WrfHydroCycleSim.pkl","rb"))',
        number=10000
    )
    # If your system is busy, this could take longer...
    # Notes(JLM): coverage is the limiting factor.
    assert time_taken < 1.2


# @pytest.mark.parametrize when https://github.com/pytest-dev/pytest/issues/349
# Looks like it is close...
def test_cycle_ensemble_compose(
    ensemble,
    job_restart,
    scheduler,
    tmpdir,
    init_times,
    restart_dirs_ensemble
):
    # These might be parametizable.

    # Length zero cycle compose.
    cy = CycleSimulation(
        init_times=[],
        restart_dirs=[],
        ncores=1
    )
    cy.add(job_restart)
    cy.add(ensemble)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_sim0_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == "There are no casts (init_times) to compose."

    # Inconsistent forcing dir length and ensemble length
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble,
        ncores=1,
        forcing_dirs=[
            ['.', '.', '../dummy_extant_dir'],
            ['.', -72, '../dummy_extant_dir'],
            ['.', -72, '../dummy_extant_dir']
        ]
    )
    cy.add(job_restart)
    with pytest.raises(Exception) as e_info:
        cy.add(ensemble)
    assert str(e_info.value) == \
        "Ensemble to add has inconsistent length with existing cycle forcing_dirs"

    # Inconsistent forcing dir length and ensemble length
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=[[rr[0]] for rr in restart_dirs_ensemble],
        ncores=1,
        forcing_dirs=[
            ['.', '../dummy_extant_dir'],
            [-72, '../dummy_extant_dir'],
            [-72, '../dummy_extant_dir']
        ]
    )
    cy.add(job_restart)
    with pytest.raises(Exception) as e_info:
        cy.add(ensemble)
    assert str(e_info.value) == \
        "Ensemble to add has inconsistent length with existing cycle restart_dirs"

    # Valid force dir exercise.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble,
        ncores=1,
        forcing_dirs=[
            ['.', '../dummy_extant_dir'],
            [-72, '../dummy_extant_dir'],
            [-72, '../dummy_extant_dir']
        ]
    )
    cy.add(job_restart)
    cy.add(ensemble)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_forc_dir_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    compose_dir.joinpath('../dummy_extant_dir').touch()
    cy.compose()

    # In valid force dir exercise.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble,
        ncores=1,
        forcing_dirs=[
            [72, '../dummy_extant_dir'],
            [72, '../dummy_extant_dir'],
            [72, '../dummy_extant_dir']
        ]
    )
    cy.add(job_restart)
    cy.add(ensemble)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_forc_dir_fail_1_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    compose_dir.joinpath('../dummy_extant_dir').touch()
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == 'Only non-negative integers can be used to specify forcing_dirs'

    # In valid force dir exercise.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble,
        ncores=1,
        forcing_dirs=[
            ['.', 'dummy_non-extant_dir'],
            ['.', 'dummy_non-extant_dir'],
            ['.', 'dummy_non-extant_dir']
        ]
    )
    cy.add(job_restart)
    cy.add(ensemble)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_forc_dir_fail_2_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    compose_dir.joinpath('../dummy_extant_dir').touch()
    with pytest.raises(Exception) as e_info:
        cy.compose()
    assert str(e_info.value) == 'No such forcing directory: dummy_non-extant_dir'


@pytest.mark.xfail(strict=False)
def test_cycle_ensemble_parallel_compose(
    ensemble,
    job_restart,
    scheduler,
    tmpdir,
    init_times,
    restart_dirs_ensemble
):
    ens = ensemble
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble,
        ncores=2
    )
    cy.add(job_restart)
    # Adding the scheduler ruins the run in CI.
    # cy.add(scheduler)

    # Make a copy where we keep the casts in memory for checking.
    cy_check_casts = copy.deepcopy(cy)
    cy_ens_compose = copy.deepcopy(cy)

    with pytest.raises(Exception) as e_info:
        cy.compose()

    cy_ens_compose.add(ens)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_ensemble_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    pathlib.Path('../dummy_extant_dir').touch()
    cy_ens_compose.compose()

    cy_run_success = cy_ens_compose.run()
    assert cy_run_success == 0
    cy.pickle(str(pathlib.Path(tmpdir) / 'cycle_ensemble_compose/WrfHydroCycleEns.pkl'))
    # Is this pickle used?

    # The cycle-in-memory version for checking the casts.
    cy_check_casts.add(ens)
    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_compose_check_casts')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    pathlib.Path('../dummy_extant_dir').touch()
    cy_check_casts.compose(rm_casts_from_memory=False, rm_members_from_memory=False)

    # The job gets heavily modified on compose.
    answer = {
        '_entry_cmd': 'bogus entry cmd',
        '_exe_cmd': './wrf_hydro.exe',
        '_exit_cmd': 'bogus exit cmd',
        '_hrldas_namelist': {
            'noahlsm_offline': {
                'btr_option': 1,
                'canopy_stomatal_resistance_option': 1,
                'hrldas_setup_file': './NWM/DOMAIN/wrfinput_d01.nc',
                'indir': './FORCING',
                'output_timestep': 86400,
                'restart_filename_requested': './NWM/RESTART/RESTART.2011082600_DOMAIN1',
                'restart_frequency_hours': 24
            },
            'wrf_hydro_offline': {
                'forc_typ': 1
            }
        },
        '_hrldas_times': {
            'noahlsm_offline': {
                'khour': 282480,
                'restart_frequency_hours': 24,
                'output_timestep': 86400,
                'restart_filename_requested': 'NWM/RESTART/RESTART.2013101300_DOMAIN1',
                'start_day': 12,
                'start_hour': 00,
                'start_min': 00,
                'start_month': 12,
                'start_year': 2012
            }
        },
        '_hydro_namelist': {
            'hydro_nlist': {
                'aggfactrt': 4,
                'channel_option': 2,
                'chanobs_domain': 0,
                'chanrtswcrt': 1,
                'chrtout_domain': 1,
                'geo_static_flnm': './NWM/DOMAIN/geo_em.d01.nc',
                'restart_file': './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
                'udmp_opt': 1,
                'rst_dt': 1440,
                'out_dt': 1440
            },
            'nudging_nlist': {
                'maxagepairsbiaspersist': 3,
                'minnumpairsbiaspersist': 1,
                'nudginglastobsfile': './NWM/RESTART/nudgingLastObs.2011-08-26_00:00:00.nc'
            }
        },
        '_hydro_times': {
            'hydro_nlist': {
                'out_dt': 1440,
                'rst_dt': 1440,
                'restart_file': 'NWM/RESTART/HYDRO_RST.2013-10-13_00:00_DOMAIN1'
            },
            'nudging_nlist': {
                'nudginglastobsfile': 'NWM/RESTART/nudgingLastObs.2013-10-13_00:00:00.nc'
            }
        },
        '_job_end_time': None,
        '_job_start_time': None,
        '_job_submission_time': None,
        '_model_end_time': pandas.Timestamp('2045-03-04 00:00:00'),
        '_model_start_time': pandas.Timestamp('2012-12-12 00:00:00'),
        'exit_status': None,
        'job_id': 'test_job_1',
        'restart_freq_hr_hydro': None,
        'restart_freq_hr_hrldas': None,
        'output_freq_hr_hydro': None,
        'output_freq_hr_hrldas': None,
        'restart': True,
        'restart_dir': None,
        '_restart_dir_hydro': None,
        '_restart_dir_hrldas': None,
        'restart_file_time': '2013-10-13',
        '_restart_file_time_hydro': pandas.Timestamp('2013-10-13 00:00:00'),
        '_restart_file_time_hrldas': pandas.Timestamp('2013-10-13 00:00:00')
    }

    # These answer patches respond to the variety of things in restart_dirs_ensemble
    dum_ext = str(tmpdir) + '/dummy_extant_dir/'
    answer_patches = {
        'cast_index': [0, 1, 2],
        'start_time_patch': [
            pandas.Timestamp('2012-12-12 00:00:00'),
            pandas.Timestamp('2012-12-15 00:00:00'),
            pandas.Timestamp('2012-12-18 00:00:00')
        ],
        'end_time_patch': [
            pandas.Timestamp('2045-03-04 00:00:00'),
            pandas.Timestamp('2045-03-07 00:00:00'),
            pandas.Timestamp('2045-03-10 00:00:00')
        ],

        # These "time patches" reveal the awkwardness of that construct.
        'lsm_times_patch': [
            'NWM/RESTART/RESTART.2013101300_DOMAIN1',
            '../../cast_2012121200/member_000/RESTART.2013101300_DOMAIN1',
            dum_ext + 'RESTART.2013101300_DOMAIN1'
        ],
        'hydro_times_patch': [
            'NWM/RESTART/HYDRO_RST.2013-10-13_00:00_DOMAIN1',
            '../../cast_2012121200/member_000/HYDRO_RST.2013-10-13_00:00_DOMAIN1',
            dum_ext + 'HYDRO_RST.2013-10-13_00:00_DOMAIN1'
        ],
        'ndg_times_patch': [
            'NWM/RESTART/nudgingLastObs.2013-10-13_00:00:00.nc',
            '../../cast_2012121200/member_000/nudgingLastObs.2013-10-13_00:00:00.nc',
            dum_ext + 'nudgingLastObs.2013-10-13_00:00:00.nc'
        ],

        # These namelist patches are consistent with the model times except in the
        # first "do nothing" case which leaves the start time != restart file time
        'lsm_nlst_patch': [
            './NWM/RESTART/RESTART.2011082600_DOMAIN1',
            '../../cast_2012121200/member_000/RESTART.2012121500_DOMAIN1',
            dum_ext + 'RESTART.2012121800_DOMAIN1'
        ],
        'hydro_nlst_patch': [
            './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
            '../../cast_2012121200/member_000/HYDRO_RST.2012-12-15_00:00_DOMAIN1',
            dum_ext + 'HYDRO_RST.2012-12-18_00:00_DOMAIN1'
        ],
        'ndg_nlst_patch': [
            './NWM/RESTART/nudgingLastObs.2011-08-26_00:00:00.nc',
            '../../cast_2012121200/member_000/nudgingLastObs.2012-12-15_00:00:00.nc',
            dum_ext + 'nudgingLastObs.2012-12-18_00:00:00.nc'
        ]

    }

    # Check a cycle where the compse retains the casts (otherwise nothing in memory).
    # This fails:
    # deepdiff.DeepDiff(answer, cy.casts[0].jobs[0].__dict__)
    # Instead, iterate on keys to "declass":
    # Just check the first ensemble cast.
    def sub_member(the_string, replace_num, find_num=0):
        replace = "member_{:03d}".format(replace_num)
        find = "member_{:03d}".format(find_num)
        return the_string.replace(find, replace)

    for ii in answer_patches['cast_index']:
        cc = cy_check_casts.casts[ii]

        for mm, member in enumerate(cc.members):

            answer['_model_start_time'] = answer_patches['start_time_patch'][ii]
            answer['_model_end_time'] = answer_patches['end_time_patch'][ii]

            keys = ['noahlsm_offline', 'restart_filename_requested']
            answer['_hrldas_namelist'][keys[0]][keys[1]] = \
                sub_member(answer_patches['lsm_nlst_patch'][ii], mm)
            answer['_hrldas_times'][keys[0]][keys[1]] = \
                sub_member(answer_patches['lsm_times_patch'][ii], mm)

            keys = ['_hydro_namelist', 'hydro_nlist', 'restart_file']
            answer[keys[0]][keys[1]][keys[2]] = \
                sub_member(answer_patches['hydro_nlst_patch'][ii], mm)

            keys = ['_hydro_namelist', 'nudging_nlist', 'nudginglastobsfile']
            answer[keys[0]][keys[1]][keys[2]] = \
                sub_member(answer_patches['ndg_nlst_patch'][ii], mm)

            keys = ['_hydro_times', 'hydro_nlist', 'restart_file']
            answer[keys[0]][keys[1]][keys[2]] =\
                sub_member(answer_patches['hydro_times_patch'][ii], mm)

            keys = ['_hydro_times', 'nudging_nlist', 'nudginglastobsfile']
            answer[keys[0]][keys[1]][keys[2]] = \
                sub_member(answer_patches['ndg_times_patch'][ii], mm)

            # hrldas times
            fmt_keys = {
                '%Y': 'start_year', '%m': 'start_month',
                '%d': 'start_day', '%H': 'start_hour'
            }
            the_mutable = answer['_hrldas_times']['noahlsm_offline']
            for fmt, key in fmt_keys.items():
                the_mutable[key] = int(answer['_model_start_time'].strftime(fmt))

            # Actually check
            for kk in member.jobs[0].__dict__.keys():
                assert member.jobs[0].__dict__[kk] == answer[kk]

    # Check the scheduler too
    # assert cy_check_casts.casts[0].scheduler.__dict__ == scheduler.__dict__

    # For the cycle where the compse removes the casts...
    # Check that the casts are all now simply pathlib objects
    assert all([type(mm) is str for mm in cy.casts])

    # the tmpdir gets nuked after the test... ?
    # Test the cast pickle size in terms of load speed.
    # Note that the deletion of the model, domain, and output objects are
    # done for the casts regardless of not removing the casts
    # from memory (currently).
    os.chdir(str(pathlib.Path(tmpdir) / 'cycle_ensemble_compose/cast_2012121200'))
    time_taken = timeit.timeit(
        setup='import pickle',
        stmt='pickle.load(open("WrfHydroEns.pkl","rb"))',
        number=10000
    )
    # If your system is busy, this could take longer... and spuriously fail the test.
    # Notes(JLM): coverage makes this slow
    assert time_taken < 1.5

    # Test the cycle pickle size in terms of load speed.
    os.chdir(str(pathlib.Path(tmpdir) / 'cycle_ensemble_compose/'))
    time_taken = timeit.timeit(
        setup='import pickle',
        stmt='pickle.load(open("WrfHydroCycleEns.pkl","rb"))',
        number=10000
    )
    # If your system is busy, this could take longer...
    # Notes(JLM): coveage makes this slow
    assert time_taken < 1.5


def test_cycle_serial(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    capfd,
    init_times,
    restart_dirs
):
    sim = simulation_compiled
    tmp = pathlib.Path(tmpdir)
    forcing_dirs = [tmp / letter for letter in list(string.ascii_lowercase)[0:len(init_times)]]
    for dir in forcing_dirs:
        dir.mkdir()
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        forcing_dirs=forcing_dirs
    )
    cy.add(sim)
    cy.add(job_restart)
    # Serial test
    cy_serial = copy.deepcopy(cy)
    cy_dir = pathlib.Path(tmpdir).joinpath('cycle_serial_run')
    os.chdir(tmpdir)
    os.mkdir(str(cy_dir))
    os.chdir(str(cy_dir))
    cy_serial.compose(rm_casts_from_memory=False)
    serial_run_success = cy_serial.run()
    assert serial_run_success == 0, \
        "Some serial cycle casts did not run successfully."
    assert cy_dir.joinpath("WrfHydroCycle.pkl").exists()


def test_cycle_run_parallel(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    capfd,
    init_times,
    restart_dirs
):
    sim = simulation_compiled
    tmp = pathlib.Path(tmpdir)
    forcing_dirs = [tmp / letter for letter in list(string.ascii_lowercase)[0:len(init_times)]]
    for dir in forcing_dirs:
        dir.mkdir()
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        forcing_dirs=forcing_dirs
    )
    cy.add(sim)
    cy.add(job_restart)
    # Parallel test
    cy_parallel = copy.deepcopy(cy)
    cy_dir = pathlib.Path(tmpdir).joinpath('cycle_parallel_run')
    os.chdir(tmpdir)
    os.mkdir(str(cy_dir))
    os.chdir(str(cy_dir))
    cy_parallel.compose()
    cy_run_success = cy_parallel.run(n_concurrent=2)
    assert cy_run_success == 0, \
        "Some parallel cycle casts did not run successfully."
    assert cy_dir.joinpath("WrfHydroCycle.pkl").exists()


def test_cycle_run_parallel_casts_in_memory(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    capfd,
    init_times,
    restart_dirs
):
    sim = simulation_compiled
    tmp = pathlib.Path(tmpdir)
    forcing_dirs = [tmp / letter for letter in list(string.ascii_lowercase)[0:len(init_times)]]
    for dir in forcing_dirs:
        dir.mkdir()
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        forcing_dirs=forcing_dirs
    )
    cy.add(sim)
    cy.add(job_restart)
    # Parallel test with ensemble in memory
    cy_parallel = copy.deepcopy(cy)
    cy_dir = pathlib.Path(tmpdir).joinpath('cy_parallel_run_cy_in_memory')
    os.chdir(tmpdir)
    os.mkdir(str(cy_dir))
    os.chdir(str(cy_dir))
    cy_parallel.compose(rm_casts_from_memory=False)
    cy_run_mem_success = cy_parallel.run(n_concurrent=2)
    assert cy_run_mem_success == 0, \
        "Some parallel cycle casts in memory did not run successfully."
    assert cy_dir.joinpath("WrfHydroCycle.pkl").exists()


def test_cycle_run_parallel_teams(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    capfd,
    init_times,
    restart_dirs
):
    sim = simulation_compiled
    tmp = pathlib.Path(tmpdir)
    forcing_dirs = [tmp / letter for letter in list(string.ascii_lowercase)[0:len(init_times)]]
    for dir in forcing_dirs:
        dir.mkdir()
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs,
        forcing_dirs=forcing_dirs
    )
    cy.add(sim)
    job_restart._entry_cmd = 'echo mpirun entry_cmd > entry_cmd.output'
    job_restart._exit_cmd = 'echo mpirun exit_cmd > exit_cmd.output'
    cy.add(job_restart)

    # Parallel teams test - test casts both in memory and not
    cy_teams_to_test = {
        'cy_teams_not_memory': copy.deepcopy(cy),
        'cy_teams_memory': copy.deepcopy(cy)
    }

    for key, cy_teams in cy_teams_to_test.items():

        cy_dir = pathlib.Path(tmpdir).joinpath(key)
        os.chdir(tmpdir)
        os.mkdir(str(cy_dir))
        os.chdir(str(cy_dir))

        cy_teams_cp = copy.deepcopy(cy_teams)
        if key == 'cy_teams_not_memory':
            cy_teams_cp.compose()
        else:
            cy_teams_cp.compose(rm_casts_from_memory=False)

        with pytest.raises(Exception) as e_info:
            cy_teams_run_fail_xnode = cy_teams_cp.run(
                teams=True,
                teams_exe_cmd=' ./wrf_hydro.exe mpirun --host {nodelist} -np {nproc} {cmd}',
                teams_exe_cmd_nproc=3,
                teams_node_file=node_file
            )
            the_error = ("ValueError('teams_exe_cmd_nproc > number of cores/node: "
                         "teams does not currently function in this capacity.',)" )
            assert repr(e_info._excinfo[1]) == the_error, 'Teams is not failing on xnode request'

        os.chdir(tmpdir)
        shutil.rmtree(str(cy_dir))
        os.mkdir(str(cy_dir))
        os.chdir(str(cy_dir))
        if key == 'cy_teams_not_memory':
            cy_teams.compose()
        else:
            cy_teams.compose(rm_casts_from_memory=False)

        cy_teams_run_success = cy_teams.run(
            teams=True,
            teams_exe_cmd=' ./wrf_hydro.exe mpirun --host {nodelist} -np {nproc} {cmd}',
            teams_exe_cmd_nproc=2,
            teams_node_file=node_file
        )
        assert cy_teams_run_success == 0, \
            "Some parallel team cycle casts did not run successfully."
        assert cy_dir.joinpath("WrfHydroCycle.pkl").exists()

        # Check for command correctness in output files.
        file_check = {
            ('cast_2012121200/entry_cmd.output',
             'cast_2012121500/entry_cmd.output',
             'cast_2012121800/entry_cmd.output'): 'mpirun entry_cmd\n',
            ('cast_2012121200/exit_cmd.output',
             'cast_2012121500/exit_cmd.output',
             'cast_2012121800/exit_cmd.output'): 'mpirun exit_cmd\n',
            ('cast_2012121200/job_test_job_1/diag_hydro.00000',):
                 'mpirun --host r10i1n1.ib0.cheyenne.ucar.edu,r10i1n1.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
            ('cast_2012121500/job_test_job_1/diag_hydro.00000',):
                 'mpirun --host r10i1n2.ib0.cheyenne.ucar.edu,r10i1n2.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
            ('cast_2012121800/job_test_job_1/diag_hydro.00000',):
                 'mpirun --host r10i1n3.ib0.cheyenne.ucar.edu,r10i1n3.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
        }
        for tup, ans in file_check.items():
            for file in tup:
                check_first_line(file, ans)


def test_cycle_self_dependent_run(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    capfd,
    init_times
):

    sim = simulation_compiled
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=['.', '-3', '-3']
    )
    cy.add(job_restart)
    cy.add(sim)

    # Serial test
    cy_serial = copy.deepcopy(cy)
    cy_dir = pathlib.Path(tmpdir).joinpath('cycle_serial_run')
    os.chdir(tmpdir)
    os.mkdir(str(cy_dir))
    os.chdir(str(cy_dir))
    cy_serial.compose(rm_casts_from_memory=False)
    serial_run_success = cy_serial.run()
    assert serial_run_success == 0, \
        "Some serial cycle casts did not run successfully."

    # Parallel test
    # TODO: This test should fail in a real run
    # cy_parallel = copy.deepcopy(cy)
    # cy_dir = pathlib.Path(tmpdir).joinpath('cycle_parallel_run')
    # os.chdir(tmpdir)
    # os.mkdir(str(cy_dir))
    # os.chdir(str(cy_dir))
    # cy_parallel.compose()

    # cy_run_success = cy_parallel.run(n_concurrent=2)
    # assert cy_run_success == 0, \
    #     "Some parallel cycle casts did not run successfully."


def test_cycle_ensemble_run(
    ensemble,
    job_restart,
    scheduler,
    tmpdir,
    capfd,
    init_times,
    restart_dirs_ensemble
):
    # Valid force dir exercise.
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=restart_dirs_ensemble,
        ncores=1,
        forcing_dirs=[
            ['.', '../dummy_extant_dir'],
            [-72, '../dummy_extant_dir'],
            [-72, '../dummy_extant_dir']
        ]
    )
    job_restart._entry_cmd = 'echo mpirun entry_cmd > entry_cmd.output'
    job_restart._exit_cmd = 'echo mpirun exit_cmd > exit_cmd.output'
    cy.add(job_restart)
    cy.add(ensemble)

    # Parallel teams test - test casts both in memory and not
    cy_teams_to_test = {
        'cy_teams_not_memory': copy.deepcopy(cy),
        'cy_teams_memory': copy.deepcopy(cy)
    }

    for key, cy_teams in cy_teams_to_test.items():

        cy_dir = pathlib.Path(tmpdir).joinpath(key)
        os.chdir(tmpdir)
        os.mkdir(str(cy_dir))
        os.chdir(str(cy_dir))
        cy_dir.joinpath('../dummy_extant_dir').touch()

        if key == 'cy_teams_not_memory':
            cy_teams.compose()
        else:
            cy_teams.compose(rm_casts_from_memory=False)

        cy_teams_run_success = cy_teams.run(
            teams=True,
            teams_exe_cmd=(
                ' ./wrf_hydro.exe mpirun --host {nodelist} -np {nproc} {cmd}'),
            teams_exe_cmd_nproc=2,
            teams_node_file=node_file
        )
        assert cy_teams_run_success == 0, \
            "Some parallel team cycle casts did not run successfully."
        assert cy_dir.joinpath("WrfHydroCycle.pkl").exists()

        # Check for command correctness in output files.
        file_check = {
            ('cast_2012121200/member_000/entry_cmd.output',
             'cast_2012121200/member_001/entry_cmd.output',
             'cast_2012121500/member_000/entry_cmd.output',
             'cast_2012121500/member_001/entry_cmd.output',
             'cast_2012121800/member_000/entry_cmd.output',
             'cast_2012121800/member_001/entry_cmd.output'):
            'mpirun entry_cmd\n',

            ('cast_2012121200/member_000/exit_cmd.output',
             'cast_2012121200/member_001/exit_cmd.output',
             'cast_2012121500/member_000/exit_cmd.output',
             'cast_2012121500/member_001/exit_cmd.output',
             'cast_2012121800/member_000/exit_cmd.output',
             'cast_2012121800/member_001/exit_cmd.output'):
            'mpirun exit_cmd\n',

            ('cast_2012121200/member_000/job_test_job_1/diag_hydro.00000',
             'cast_2012121200/member_001/job_test_job_1/diag_hydro.00000'):
             'mpirun --host r10i1n1.ib0.cheyenne.ucar.edu,r10i1n1.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
            ('cast_2012121500/member_000/job_test_job_1/diag_hydro.00000',
             'cast_2012121500/member_001/job_test_job_1/diag_hydro.00000'):
             'mpirun --host r10i1n2.ib0.cheyenne.ucar.edu,r10i1n2.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
            ('cast_2012121800/member_000/job_test_job_1/diag_hydro.00000',
             'cast_2012121800/member_001/job_test_job_1/diag_hydro.00000'):
             'mpirun --host r10i1n3.ib0.cheyenne.ucar.edu,r10i1n3.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
        }
        for tup, ans in file_check.items():
            for file in tup:
                check_first_line(file, ans)
