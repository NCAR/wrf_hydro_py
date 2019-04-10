import copy
import datetime
import deepdiff
import os
import pathlib
import pandas
import pytest
import string
import timeit

from wrfhydropy.core.simulation import Simulation
from wrfhydropy.core.cycle import CycleSimulation


@pytest.fixture(scope='function')
def init_times():
    some_time = datetime.datetime(2012, 12, 12, 0, 0)
    init_times = [some_time + datetime.timedelta(dd) for dd in range(0, 9, 3)]
    return init_times


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


def test_cycle_init(init_times):
    cycle = CycleSimulation(
        init_times=init_times,
        restart_dirs=['.'] * len(init_times)
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
    init_times    
):
    sim = simulation
    cy1 = CycleSimulation(
        init_times=init_times,
        restart_dirs=['.'] * len(init_times)
    )

    # This sim does not have the required pre-compiled model
    with pytest.raises(Exception) as e_info:
        cy1.add(sim)

    sim_compiled = simulation_compiled

    # cant add a list, even if pre-compiled
    with pytest.raises(Exception) as e_info:
        cy1.add([sim_compiled])

    cy1.add(sim_compiled)

    # add a sim with job and make sure it is deleted.
    sim_compiled.add(job_restart)
    sim_compiled.add(scheduler)
    cy2 = CycleSimulation(
        init_times=init_times,
        restart_dirs=['.'] * len(init_times)
    )
    cy2.add(sim_compiled)

    assert all([len(cc.jobs) == 0 for cc in cy2.casts])
    assert all([cc.scheduler is None for cc in cy2.casts])


def test_cycle_addjob(simulation, job_restart, init_times):
    cy1 = CycleSimulation(init_times=init_times, restart_dirs=['.'] * len(init_times))
    cy1.add(job_restart)
    assert deepdiff.DeepDiff(cy1._job, job_restart) == {}

    job_restart.job_id = 'a_different_id'
    cy1.add(job_restart)
    assert deepdiff.DeepDiff(cy1._job, job_restart) == {}


def test_cycle_addscheduler(
    simulation,
    scheduler,
    init_times
):
    cy1 = CycleSimulation(init_times=init_times, restart_dirs=['.'] * len(init_times))
    cy1.add(scheduler)
    assert deepdiff.DeepDiff(cy1._scheduler, scheduler) == {}

    sched2 = copy.deepcopy(scheduler)
    sched2.nnodes = 99
    cy1.add(sched2)
    assert deepdiff.DeepDiff(cy1._scheduler, sched2) == {}


def test_cycle_length(
    simulation_compiled,
    init_times
):
    sim = simulation_compiled
    cy1 = CycleSimulation(init_times=init_times, restart_dirs=['.'] * len(init_times))
    cy1.add(sim)
    assert len(cy1) == len(init_times)
    assert cy1.N == len(init_times)
    # How to assert an error?
    # assert cy1.replicate_member(4) == "WTF mate?"


def test_cycle_parallel_compose(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    init_times
):
    sim = simulation_compiled
    cy = CycleSimulation(
        init_times=init_times,
        restart_dirs=['.'] * len(init_times),
        ncores=2
    )
    cy.add(job_restart)
    # Adding the scheduler ruins the run in CI.
    #cy.add(scheduler)

    with pytest.raises(Exception) as e_info:
        cy.compose()

    cy.add(sim)

    # Make a copy where we keep the casts in memory for checking.
    cy_check_casts = copy.deepcopy(cy)

    compose_dir = pathlib.Path(tmpdir).joinpath('cycle_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    cy.compose()

    cy_run_success = cy.run()
    assert cy_run_success
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
    #assert cy_check_casts.casts[0].scheduler.__dict__ == scheduler.__dict__

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
    # Notes(JLM): OSX spinning disk is < .5, cheyenne scratch is < 1.2
    assert time_taken < 1.2

    # Test the cycle pickle size in terms of load speed.
    os.chdir(str(pathlib.Path(tmpdir) / 'cycle_compose/'))
    time_taken = timeit.timeit(
        setup='import pickle',
        stmt='pickle.load(open("WrfHydroCycleSim.pkl","rb"))',
        number=10000
    )
    # If your system is busy, this could take longer...
    # Notes(JLM): .6 seems to work on OSX spinning disk and chyenne scratch.
    assert time_taken < .6


def test_cycle_run(
    simulation_compiled,
    job_restart,
    scheduler,
    tmpdir,
    capfd,
    init_times
):

    sim = simulation_compiled

    tmp = pathlib.Path(tmpdir)
    forcing_dirs = [tmp / letter for letter in list(string.ascii_lowercase)[0:len(init_times)]]
    for dir in forcing_dirs:
        dir.mkdir()

    restart_dirs = ['.'] *len(init_times)
        
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
    assert serial_run_success, \
        "Some serial cycle casts did not run successfully."

    # Parallel test
    cy_parallel = copy.deepcopy(cy)
    cy_dir = pathlib.Path(tmpdir).joinpath('cycle_parallel_run')
    os.chdir(tmpdir)
    os.mkdir(str(cy_dir))
    os.chdir(str(cy_dir))
    cy_parallel.compose()

    cy_run_success = cy_parallel.run(n_concurrent=2)
    assert cy_run_success, \
        "Some parallel cycle casts did not run successfully."

    # Parallel test with ensemble in memory
    cy_parallel = copy.deepcopy(cy)
    cy_dir = pathlib.Path(tmpdir).joinpath('cy_parallel_run_cy_in_memory')
    os.chdir(tmpdir)
    os.mkdir(str(cy_dir))
    os.chdir(str(cy_dir))
    cy_parallel.compose(rm_casts_from_memory=False)
    cy_run_mem_success = cy_parallel.run(n_concurrent=2)
    assert cy_run_mem_success, \
        "Some parallel cycle casts in memory did not run successfully."


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
    assert serial_run_success, \
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
    # assert cy_run_success, \
    #     "Some parallel cycle casts did not run successfully."
