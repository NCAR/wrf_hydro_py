import copy
import datetime
import deepdiff
import os
import pathlib
import pandas
import pytest
import timeit
import warnings

from wrfhydropy.core.simulation import Simulation
from wrfhydropy.core.ensemble import EnsembleSimulation
from wrfhydropy.core.ensemble_tools import get_ens_dotfile_end_datetime
from wrfhydropy.core.outputdiffs import check_unprocessed_diffs
test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))
node_file = test_dir / 'data/nodefile_pbs_example_copy.txt'


@pytest.fixture(scope='function')
def simulation(model, domain):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    return sim


@pytest.fixture(scope='function')
def simulation_compiled(model, domain, job, tmpdir):
    sim_dir = pathlib.Path(tmpdir).joinpath('sim_compiled_dir')
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job)
    os.mkdir(sim_dir)
    os.chdir(sim_dir)
    sim.compose()
    return sim


def test_ensemble_init():
    ens = EnsembleSimulation()
    assert type(ens) is EnsembleSimulation
    # Not sure why this dosent vectorize well.
    atts = ['members', '_EnsembleSimulation__member_diffs', 'jobs',
            'scheduler', 'ncores', 'ens_dir']
    for kk in ens.__dict__.keys():
        assert kk in atts


def test_ensemble_addsimulation(simulation, job, scheduler, simulation_compiled):
    sim = simulation
    ens1 = EnsembleSimulation()
    ens2 = EnsembleSimulation()

    # This sim does not have a pre-compiled model
    with pytest.raises(Exception) as e_info:
        ens1.add([sim])

    sim = simulation_compiled
    ens1.add([sim])
    ens2.add(sim)
    ens_diff = deepdiff.DeepDiff(ens1, ens2)
    unprocessed_diffs = ens_diff.pop('unprocessed', [])
    if unprocessed_diffs:
        check_unprocessed_diffs(unprocessed_diffs)
    assert ens_diff == {}

    # add a sim with job and make sure it is deleted.
    sim.add(job)
    sim.add(scheduler)
    ens1 = EnsembleSimulation()
    ens1.add(sim)
    assert all([len(mm.jobs) == 0 for mm in ens1.members])
    assert all([mm.scheduler is None for mm in ens1.members])


def test_ensemble_addjob(simulation, job):
    ens1 = EnsembleSimulation()
    ens1.add(job)
    assert deepdiff.DeepDiff(ens1.jobs[0], job) == {}

    job.job_id = 'a_different_id'
    ens1.add(job)
    assert deepdiff.DeepDiff(ens1.jobs[1], job) == {}


def test_ensemble_addscheduler(simulation, scheduler):
    ens1 = EnsembleSimulation()
    ens1.add(scheduler)
    assert deepdiff.DeepDiff(ens1.scheduler, scheduler) == {}

    sched2 = copy.deepcopy(scheduler)
    sched2.nnodes = 99
    ens1.add(sched2)
    assert deepdiff.DeepDiff(ens1.scheduler, sched2) == {}


def test_ensemble_replicate(simulation_compiled):
    sim = simulation_compiled
    ens1 = EnsembleSimulation()
    ens2 = EnsembleSimulation()
    ens1.add(sim)
    ens1.replicate_member(4)
    ens2.add([sim, sim, sim, sim])
    ens_diff = deepdiff.DeepDiff(ens1, ens2)
    unprocessed_diffs = ens_diff.pop('unprocessed', [])
    if unprocessed_diffs:
        check_unprocessed_diffs(unprocessed_diffs)
    assert ens_diff == {}


def test_ensemble_length(simulation_compiled):
    sim = simulation_compiled
    ens1 = EnsembleSimulation()
    ens1.add(sim)
    ens1.replicate_member(4)
    assert len(ens1) == 4
    assert ens1.N == 4
    # How to assert an error?
    # assert ens1.replicate_member(4) == "WTF mate?"


def test_ens_get_diff_dicts(simulation_compiled):
    sim = simulation_compiled
    ens = EnsembleSimulation()
    ens.add([sim, sim, sim, sim])
    answer = {
        'number': ['000', '001', '002', '003'],
        'run_dir': ['member_000', 'member_001', 'member_002', 'member_003']
    }
    assert ens.member_diffs == answer


def test_ens_set_diff_dicts(simulation_compiled):
    sim = simulation_compiled
    ens = EnsembleSimulation()
    ens.add([sim, sim, sim, sim, sim])
    ens.set_member_diffs(('base_hrldas_namelist', 'noahlsm_offline', 'indir'),
                         ['./FOO' if mm == 2 else './FORCING' for mm in range(len(ens))])
    answer = {
        ('base_hrldas_namelist', 'noahlsm_offline', 'indir'):
            ['./FORCING', './FORCING', './FOO', './FORCING', './FORCING'],
        'number': ['000', '001', '002', '003', '004'],
        'run_dir': ['member_000', 'member_001', 'member_002', 'member_003', 'member_004']
    }
    assert ens.member_diffs == answer


def test_ens_compose_restore(simulation_compiled, job_restart, scheduler, tmpdir):
    sim = simulation_compiled
    ens = EnsembleSimulation()
    ens.add(job_restart)
    ens.add([sim, sim])

    # Do not keep the members in memory
    ens_disk = copy.deepcopy(ens)
    compose_dir = pathlib.Path(tmpdir).joinpath('ensemble_disk')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    ens_disk.compose()
    ens_disk_run_success = ens_disk.run()
    assert ens_disk_run_success == 0

    # Keep the members in memory
    compose_dir = pathlib.Path(tmpdir).joinpath('ensemble_memory')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    ens.compose(rm_members_from_memory=False)
    ens_run_success = ens.run()
    assert ens_run_success == 0

    # Check that the members are all now simply pathlib objects
    assert all([type(mm) is str for mm in ens_disk.members])
    ens_disk.restore_members()
    # Since the ens_disk has data from the run. Collect data from the run:
    ens.collect(output=False)
    # The members are not restored, the simultaion sub objects are:
    ens.restore_members()

    # These will never be the same for two different runs.
    for mem, dsk in zip(ens.members, ens_disk.members):
        del mem.jobs[0].job_end_time, mem.jobs[0].job_start_time
        del dsk.jobs[0].job_end_time, dsk.jobs[0].job_start_time

    from pprint import pprint

    ens_diff = deepdiff.DeepDiff(ens, ens_disk)
    unprocessed_diffs = ens_diff.pop('unprocessed', [])
    if unprocessed_diffs:
        check_unprocessed_diffs(unprocessed_diffs)

    assert ens_diff == {}


@pytest.mark.xfail(strict=False)
def test_ens_parallel_compose(simulation_compiled, job_restart, scheduler, tmpdir):
    sim = simulation_compiled
    ens = EnsembleSimulation()
    ens.add(job_restart)

    with pytest.raises(Exception) as e_info:
        ens.compose()

    ens.add([sim, sim])

    # Check the scheduler upon compose (dont run with scheduler)
    ens_w_sched = copy.deepcopy(ens)
    ens_w_sched.add(scheduler)
    compose_dir = pathlib.Path(tmpdir).joinpath('ensemble_compose_sched')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    ens_w_sched.compose(rm_members_from_memory=False)
    assert ens_w_sched.members[0].scheduler.__dict__ == scheduler.__dict__

    # Test a run where the members were not kept in memory
    # Make a copy to test against later
    ens_check_members = copy.deepcopy(ens)
    compose_dir = pathlib.Path(tmpdir).joinpath('ensemble_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    ens.compose()
    ens_run_success = ens.run()
    assert ens_run_success == 0
    # Check that the members are all now simply pathlib objects
    assert all([type(mm) is str for mm in ens.members])

    # Why pickle?
    ens.pickle(str(pathlib.Path(tmpdir) / 'ensemble_compose/WrfHydroEnsSim.pkl'))

    # The ensemble-in-memory version for checking the members.
    compose_dir = pathlib.Path(tmpdir).joinpath('ensemble_compose_check_members')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))
    ens_check_members.compose(rm_members_from_memory=False)

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
                'restart_filename_requested': './NWM/RESTART/RESTART.2011082600_DOMAIN1',
                'restart_frequency_hours': 24,
                'output_timestep': 86400
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
                'start_day': 14,
                'start_hour': 0,
                'start_min': 0,
                'start_month': 10,
                'start_year': 1984
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
                'restart_file': 'NWM/RESTART/HYDRO_RST.2013-10-13_00:00_DOMAIN1',
                'rst_dt': 1440,
                'out_dt': 1440
            },
            'nudging_nlist': {
                'nudginglastobsfile': 'NWM/RESTART/nudgingLastObs.2013-10-13_00:00:00.nc'
            }
        },
        '_job_end_time': None,
        '_job_start_time': None,
        '_job_submission_time': None,
        '_model_end_time': pandas.Timestamp('2017-01-04 00:00:00'),
        '_model_start_time': pandas.Timestamp('1984-10-14 00:00:00'),
        'exit_status': None,
        'job_id': 'test_job_1',
        'restart_dir': None,
        '_restart_dir_hydro': None,
        '_restart_dir_hrldas': None,
        'restart_freq_hr_hydro': None,
        'restart_freq_hr_hrldas': None,
        'output_freq_hr_hydro': None,
        'output_freq_hr_hrldas': None,
        'restart': True,
        'restart_file_time': '2013-10-13',
        '_restart_file_time_hrldas': pandas.Timestamp('2013-10-13 00:00:00'),
        '_restart_file_time_hydro': pandas.Timestamp('2013-10-13 00:00:00')
    }

    # For the ensemble where the compse retains the members...

    # This fails:
    # deepdiff.DeepDiff(answer, ens.members[0].jobs[0].__dict__)
    # Instead, iterate on keys to "declass":
    for kk in ens_check_members.members[0].jobs[0].__dict__.keys():
        assert ens_check_members.members[0].jobs[0].__dict__[kk] == answer[kk]

    # The tmpdir gets nuked after the test... ?
    # Test the member pickle size in terms of load speed.
    # Note that the deletion of the model, domain, and output objects are
    # done for the members regardless of not removing the members
    # from memory (currently).
    os.chdir(str(pathlib.Path(tmpdir) / 'ensemble_compose/member_000'))
    time_taken = timeit.timeit(
        setup='import pickle',
        stmt='pickle.load(open("WrfHydroSim.pkl","rb"))',
        number=10000
    )
    # If your system is busy, this could take longer... and spuriously fail the test.
    # Notes(JLM): coverage is the limiting factor here.
    assert time_taken < 2.0

    # Test the ensemble pickle size in terms of load speed.
    os.chdir(str(pathlib.Path(tmpdir) / 'ensemble_compose/'))
    time_taken = timeit.timeit(
        setup='import pickle',
        stmt='pickle.load(open("WrfHydroEnsSim.pkl","rb"))',
        number=10000
    )
    # If your system is busy, this could take longer...
    # Notes(JLM): chyenne scratch is slow sometimes. so is CI
    assert time_taken < 1.0


def test_ens_parallel_run(simulation_compiled, job, scheduler, tmpdir, capfd):
    sim = simulation_compiled
    ens = EnsembleSimulation()
    ens.add(job)
    ens.add([sim, sim])

    # Serial test
    ens_serial = copy.deepcopy(ens)
    ens_dir = pathlib.Path(tmpdir).joinpath('ens_serial_run')
    os.chdir(tmpdir)
    os.mkdir(str(ens_dir))
    os.chdir(str(ens_dir))
    ens_serial.compose(rm_members_from_memory=False)
    serial_run_success = ens_serial.run()
    assert serial_run_success == 0, \
        "Some serial ensemble members did not run successfully."
    assert get_ens_dotfile_end_datetime(ens_dir) == datetime.datetime(2017, 1, 4, 0, 0)
    assert ens_dir.joinpath("WrfHydroEns.pkl").exists()

    # Parallel test
    ens_parallel = copy.deepcopy(ens)
    ens_dir = pathlib.Path(tmpdir).joinpath('ens_parallel_run')
    os.chdir(tmpdir)
    os.mkdir(str(ens_dir))
    os.chdir(str(ens_dir))
    ens_parallel.compose()
    ens_run_success = ens_parallel.run(n_concurrent=2)
    assert ens_run_success == 0, \
        "Some parallel ensemble members did not run successfully."
    assert get_ens_dotfile_end_datetime(ens_dir) == datetime.datetime(2017, 1, 4, 0, 0)
    assert ens_dir.joinpath("WrfHydroEns.pkl").exists()

    # Parallel test with ensemble in memory
    ens_parallel = copy.deepcopy(ens)
    ens_dir = pathlib.Path(tmpdir).joinpath('ens_parallel_run_ens_in_memory')
    os.chdir(tmpdir)
    os.mkdir(str(ens_dir))
    os.chdir(str(ens_dir))
    ens_parallel.compose(rm_members_from_memory=False)
    ens_run_mem_success = ens_parallel.run(n_concurrent=2)
    assert ens_run_mem_success == 0, \
        "Some parallel ensemble members in memory did not run successfully."
    assert get_ens_dotfile_end_datetime(ens_dir) == datetime.datetime(2017, 1, 4, 0, 0)
    assert ens_dir.joinpath("WrfHydroEns.pkl").exists()


# Some helper function for the following test_ens_teams_run*
def get_mem_start_file_time(mem_int, ens_dir):
    return os.path.getmtime(
        ens_dir /
        ('member_00' + str(mem_int) + '/.model_start_time'))


def check_first_line(file, answer):
    with open(file) as f:
        first_line = f.readline()
    assert first_line == answer


def test_ens_teams_run_dict(simulation_compiled, job, scheduler, tmpdir, capfd):
    ens_dir = pathlib.Path(tmpdir).joinpath('ens_teams_run')
    teams_dict = {
        '0': {
            'members': ['member_000', 'member_002'],
            'nodes': ['hostname0'],
            'env': None,
            'exe_cmd': 'sleep 2; ./wrf_hydro.exe mpirun --host {nodelist} -np {nproc} {cmd}', },
        '1': {
            'members': ['member_001', 'member_003'],
            'nodes': ['hostname1', 'hostname1'],
            'env': None,
            'exe_cmd': 'sleep 2; ./wrf_hydro.exe mpirun --host {nodelist} -np {nproc} {cmd}', } }

    sim = simulation_compiled
    ens = EnsembleSimulation()
    job._entry_cmd = 'echo mpirun entry_cmd > entry_cmd.output'
    job._exit_cmd = 'echo mpirun exit_cmd > exit_cmd.output'
    ens.add(job)
    ens.add([sim, sim, sim, sim])

    ens_parallel = copy.deepcopy(ens)
    os.mkdir(str(ens_dir))
    os.chdir(str(ens_dir))
    ens_parallel.compose()

    ens_run_success = ens_parallel.run(teams_dict=teams_dict)

    assert ens_run_success == 0, \
        "Some teams ensemble members did not run successfully."

    # If the above members are run in parallel, the above sleep  in the
    # exe_cmd should not affect the difference in start times.
    # Members on the same teams will have a >=2 sec delay,
    # but collated members on different teams should be run with ~=0sec delay
    # 0 and 1 are collated members on different teams
    assert abs(get_mem_start_file_time(1, ens_dir) -
               get_mem_start_file_time(0, ens_dir)) < 0.99

    # Check for command correctness in output files.
    file_check = {
        ('member_000/entry_cmd.output',
         'member_001/entry_cmd.output',
         'member_002/entry_cmd.output',
         'member_003/entry_cmd.output'): 'mpirun entry_cmd\n',
        ('member_000/exit_cmd.output',
         'member_001/exit_cmd.output',
         'member_002/exit_cmd.output',
         'member_003/exit_cmd.output'): 'mpirun exit_cmd\n',
        ('member_000/job_test_job_1/diag_hydro.00000',
         'member_002/job_test_job_1/diag_hydro.00000'):
             'mpirun --host hostname0 -np 1 ./wrf_hydro.exe\n',
        ('member_001/job_test_job_1/diag_hydro.00000',
         'member_003/job_test_job_1/diag_hydro.00000'):
             'mpirun --host hostname1,hostname1 -np 2 ./wrf_hydro.exe\n'
    }
    for tup, ans in file_check.items():
        for file in tup:
            check_first_line(file, ans)


def test_ens_teams_run_args(simulation_compiled, job, scheduler, tmpdir, capfd):
    ens_dir = pathlib.Path(tmpdir).joinpath('ens_teams_run')

    sim = simulation_compiled
    ens = EnsembleSimulation()
    job._entry_cmd = 'echo mpirun entry_cmd > entry_cmd.output'
    job._exit_cmd = 'echo mpirun exit_cmd > exit_cmd.output'
    ens.add(job)
    ens.add([sim, sim, sim, sim])

    ens_parallel = copy.deepcopy(ens)
    os.mkdir(str(ens_dir))
    os.chdir(str(ens_dir))
    ens_parallel.compose()

    exe_cmd = (
        'sleep 1; ./wrf_hydro.exe mpirun --host {nodelist} -np {nproc} {cmd}')
    ens_run_success = ens_parallel.run(
        teams=True,
        teams_exe_cmd_nproc=2,
        teams_node_file=node_file,
        teams_exe_cmd=exe_cmd)

    assert ens_run_success == 0, \
        "Some teams ensemble members did not run successfully."

    # If the above members are run in parallel, the above sleep  in the
    # exe_cmd should not affect the difference in start times.
    assert abs(get_mem_start_file_time(1, ens_dir) -
               get_mem_start_file_time(0, ens_dir)) < 0.99

    # Check for command correctness in output files.
    file_check = {
        ('member_000/entry_cmd.output',
         'member_001/entry_cmd.output',
         'member_002/entry_cmd.output',
         'member_003/entry_cmd.output'): 'mpirun entry_cmd\n',
        ('member_000/exit_cmd.output',
         'member_001/exit_cmd.output',
         'member_002/exit_cmd.output',
         'member_003/exit_cmd.output'): 'mpirun exit_cmd\n',
        ('member_000/job_test_job_1/diag_hydro.00000',
         'member_003/job_test_job_1/diag_hydro.00000'):
             'mpirun --host r10i1n1.ib0.cheyenne.ucar.edu,r10i1n1.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
        ('member_001/job_test_job_1/diag_hydro.00000',):
             'mpirun --host r10i1n2.ib0.cheyenne.ucar.edu,r10i1n2.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
        ('member_002/job_test_job_1/diag_hydro.00000',):
             'mpirun --host r10i1n3.ib0.cheyenne.ucar.edu,r10i1n3.ib0.cheyenne.ucar.edu -np 2 ./wrf_hydro.exe\n',
    }
    for tup, ans in file_check.items():
        for file in tup:
            check_first_line(file, ans)
