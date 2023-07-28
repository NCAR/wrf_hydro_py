import copy
import deepdiff
import os
import pathlib
import pickle
import pytest

from wrfhydropy.core.simulation import Simulation, SimulationOutput
from wrfhydropy.core.ioutils import WrfHydroTs
from wrfhydropy.core.ensemble_tools import DeepDiffEq
from wrfhydropy.core.outputdiffs import check_unprocessed_diffs


def test_simulation_add_model_domain(model, domain):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)

    assert sim.base_hydro_namelist == \
        {'hydro_nlist':
         {
             'channel_option': 2,
             'chanobs_domain': 0,
             'chanrtswcrt': 1,
             'chrtout_domain': 1,
             'geo_static_flnm': './NWM/DOMAIN/geo_em.d01.nc',
             'restart_file': './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
             'aggfactrt': 4,
             'udmp_opt': 1,
             'out_dt': 1440,
             'rst_dt': 1440
         },
         'nudging_nlist': {
             'maxagepairsbiaspersist': 3,
             'minnumpairsbiaspersist': 1,
             'nudginglastobsfile':
             './NWM/RESTART/nudgingLastObs.2011-08-26_00:00:00.nc'
         }
        }

    assert sim.base_hrldas_namelist == \
        {'noahlsm_offline':
         {
             'btr_option': 1,
             'canopy_stomatal_resistance_option': 1,
             'hrldas_setup_file': './NWM/DOMAIN/wrfinput_d01.nc',
             'restart_filename_requested':
             './NWM/RESTART/RESTART.2011082600_DOMAIN1',
             'indir': './FORCING',
             'output_timestep': 86400,
             'restart_frequency_hours': 24
         },
         'wrf_hydro_offline': {'forc_typ': 1}
        }


def test_simulation_add_job(model, domain, job):
    sim = Simulation()
    with pytest.raises(Exception) as e_info:
        sim.add(job)

    sim.add(model)
    sim.add(domain)
    sim.add(job)


def test_simulation_compose(model, domain, job, capfd, tmpdir):

    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job)

    # copy before compose
    sim_opts = copy.deepcopy(sim)
    sim_tbls = copy.deepcopy(sim)

    compose_dir = pathlib.Path(tmpdir).joinpath('sim_compose')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))

    sim.compose()

    # Doing this thrice kinda asks for function...
    # This compose exercises the options to compose. Gives the same result.
    compose_dir_opts = pathlib.Path(tmpdir).joinpath('sim_compose_opts')
    os.mkdir(str(compose_dir_opts))
    os.chdir(str(compose_dir_opts))

    sim_opts.compose(
        symlink_domain=False,
        force=True,
        check_nlst_warn=True
    )

    actual_files = list(compose_dir.rglob('./*'))
    domain_files = domain.domain_top_dir.rglob('*')
    expected_files = [
        'namelist.hrldas',
        'hydro.namelist',
        'job_test_job_1',
        '.uid',
        'NWM',
        'WrfHydroModel.pkl',
        'FORCING',
        'DUMMY.TBL',
        'wrf_hydro.exe'
    ]

    for file in domain_files:
        expected_files.append(file.name)

    for file in actual_files:
        assert file.name in expected_files

    assert sim.model.table_files == sim_opts.model.table_files
    assert [str(ff.name) for ff in sim.model.table_files] == ['DUMMY.TBL']

    # These composes result in alternative, user selected table files.
    # Do it before and after model.compile()
    sim_tbls_postcompile = copy.deepcopy(sim_tbls)

    dummy_user_tbl = pathlib.Path(tmpdir).joinpath('DUMMY_USER.TBL')
    with dummy_user_tbl.open('w') as f:
        f.write('# dummy TBL \n')

    compose_dir_tbls = pathlib.Path(tmpdir).joinpath('sim_compose_tbls')
    os.mkdir(str(compose_dir_tbls))
    os.chdir(str(compose_dir_tbls))
    # before compile
    sim_tbls.model.table_files = [dummy_user_tbl]
    sim_tbls.compose()

    compose_dir_tbls_postcompile = pathlib.Path(tmpdir).joinpath('sim_compose_tbls_postcompile')
    compile_dir_tbls_postcompile = pathlib.Path(tmpdir).joinpath('sim_compile_tbls_postcompile')
    os.mkdir(str(compose_dir_tbls_postcompile))
    os.chdir(str(compose_dir_tbls_postcompile))
    sim_tbls_postcompile.model.compile(compile_dir_tbls_postcompile)
    sim_tbls_postcompile.model.table_files = [dummy_user_tbl]
    sim_tbls_postcompile.compose()

    assert sim_tbls.model.table_files == sim_tbls_postcompile.model.table_files
    assert sim_tbls.model.table_files == [dummy_user_tbl]

    actual_files = list(compose_dir_tbls.rglob('./*'))
    domain_files = domain.domain_top_dir.rglob('*')
    expected_files = [
        'namelist.hrldas',
        'hydro.namelist',
        'job_test_job_1',
        '.uid',
        'NWM',
        'WrfHydroModel.pkl',
        'FORCING',
        'DUMMY_USER.TBL',
        'wrf_hydro.exe'
    ]

    for file in domain_files:
        expected_files.append(file.name)

    for file in actual_files:
        assert file.name in expected_files


def test_simulation_run_no_scheduler(model, domain, job, tmpdir, capfd):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job)

    compose_dir = pathlib.Path(tmpdir).joinpath('sim_run_no_sched')
    os.mkdir(str(compose_dir))
    os.chdir(str(compose_dir))

    sim.compose()
    sim.run()
    assert sim.jobs[0].exit_status == 0, \
        "The job did not exit successfully."


def test_simulation_collect(sim_output):
    sim = Simulation()
    sim.collect(sim_dir=sim_output)
    assert sim.output is not None
    assert type(sim.output) is SimulationOutput


def test_simulation_output_checknans(sim_output):
    output = SimulationOutput()
    output.collect_output(sim_dir=sim_output)
    public_atts = [att for att in dir(output) if not att.startswith('__')]
    for att in public_atts:
        assert getattr(output, att) is not None
    assert output.check_output_nans() is None


def test_simulation_pickle(model, domain, job, tmpdir):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job)
    pickle_path = pathlib.Path(tmpdir).joinpath('Sim.pkl')
    sim.pickle(pickle_path)
    sim0 = copy.deepcopy(sim)
    del sim
    sim = pickle.load(pickle_path.open(mode='rb'))

    sim_diff = deepdiff.DeepDiff(sim, sim0)
    unprocessed_diffs = sim_diff.pop('unprocessed', [])
    if unprocessed_diffs:
        check_unprocessed_diffs(unprocessed_diffs)
    assert sim_diff == {}


def test_simulation_sub_obj_pickle(model, domain, job, tmpdir):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job)

    os.chdir(tmpdir)
    domain_path = pathlib.Path(tmpdir).joinpath('WrfHydroDomain.pkl')
    model_path = pathlib.Path(tmpdir).joinpath('WrfHydroModel.pkl')
    sim.pickle_sub_objs()
    assert sim.domain.resolve() == domain_path
    assert sim.model.resolve() == model_path

    sim.restore_sub_objs()
    domain_diff = deepdiff.DeepDiff(sim.domain, domain)
    unprocessed_diffs = domain_diff.pop('unprocessed', [])
    if unprocessed_diffs:
        check_unprocessed_diffs(unprocessed_diffs)
    assert domain_diff == {}

    model_diff = deepdiff.DeepDiff(sim.model, model)
    unprocessed_diffs = model_diff.pop('unprocessed', [])
    if unprocessed_diffs:
        check_unprocessed_diffs(unprocessed_diffs)
    assert model_diff == {}
