import os
import pathlib
import pytest
import shutil
from wrfhydropy import open_whp_dataset
from .data import collection_data_download

# The answer reprs are found here.
from .data.collection_data_answer_reprs import *

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))

# The data are found here.
collection_data_download.download()

# Issues raised by these tests
# https://github.com/NCAR/wrf_hydro_nwm_public/issues/301
# https://github.com/NCAR/wrf_hydro_nwm_public/issues/302
# Make an issue: The restart files should have reference time and time just like the other files.

# TODO: Test multiple versions (current and previous)
version_file = test_dir.joinpath('data/collection_data/croton_NY/.version')
version = version_file.open('r').read().split('-')[0]


# Simulation
# Make a sim dir to a single simulation.
sim_dir = test_dir / 'data/collection_data/simulation'
if sim_dir.exists():
    sim_dir.unlink()
sim_dir.symlink_to(test_dir / 'data/collection_data/ens_ana/cast_2011082600/member_000')


@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores'],
    [
        ('*CHRTOUT_DOMAIN1', simulation_answer_reprs[version]['*CHRTOUT_DOMAIN1'], 1),
        ('*LAKEOUT_DOMAIN1', simulation_answer_reprs[version]['*LAKEOUT_DOMAIN1'], 1),
        ('*CHANOBS_DOMAIN1', simulation_answer_reprs[version]['*CHANOBS_DOMAIN1'], 1),
        ('*GWOUT_DOMAIN1', simulation_answer_reprs[version]['*GWOUT_DOMAIN1'], 1),
        ('*[0-9].RTOUT_DOMAIN1', simulation_answer_reprs[version]['*RTOUT_DOMAIN1'], 2),
        ('*LDASOUT_DOMAIN1', simulation_answer_reprs[version]['*LDASOUT_DOMAIN1'], 3),
        ('*LSMOUT_DOMAIN', simulation_answer_reprs[version]['*LSMOUT_DOMAIN'], 2),
        ('RESTART.*_DOMAIN1', simulation_answer_reprs[version]['RESTART.*_DOMAIN1'], 2),
        ('HYDRO_RST.*_DOMAIN1', simulation_answer_reprs[version]['HYDRO_RST.*_DOMAIN1'], 3),
    ],
    ids=[
        'simulation-CHRTOUT_DOMAIN1',
        'simulation-LAKEOUT_DOMAIN1',
        'simulation-CHANOBS_DOMAIN1',
        'simulation-GWOUT_DOMAIN1',
        'simulation-RTOUT_DOMAIN1',
        'simulation-LDASOUT_DOMAIN1',
        'simulation-LSMOUT_DOMAIN',
        'simulation-RESTART.*_DOMAIN1',
        'simulation-HYDRO_RST.*_DOMAIN1'
    ]
)
def test_collect_simulation(
    file_glob,
    expected,
    n_cores
):
    sim_path = test_dir.joinpath(sim_dir)
    files = sorted(sim_path.glob(file_glob))
    sim_ds = open_whp_dataset(files, n_cores=n_cores)
    # This checks everything about the metadata.
    assert repr(sim_ds) == expected


# Cycle
# Make a cycle dir and set it up from the ensemble cycle.
cycle_dir = test_dir / 'data/collection_data/cycle'
# delete the directory here.
if cycle_dir.exists():
    shutil.rmtree(str(cycle_dir))
cycle_dir.mkdir()
os.chdir(str(cycle_dir))
cycle_dir.joinpath('WrfHydroCycle.pkl').symlink_to(
    test_dir.joinpath('data/collection_data/ens_ana/WrfHydroCycle.pkl')
)
for cast in test_dir.joinpath('data/collection_data/ens_ana').glob('cast_*'):
    cast_name = pathlib.Path(cast.name)
    cast_name.symlink_to(cast.joinpath('member_000'))


@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores'],
    [
        ('*/*CHRTOUT_DOMAIN1', cycle_answer_reprs[version]['*/*CHRTOUT_DOMAIN1'], 1),
        ('*/*LAKEOUT_DOMAIN1', cycle_answer_reprs[version]['*/*LAKEOUT_DOMAIN1'], 1),
        ('*/*CHANOBS_DOMAIN1', cycle_answer_reprs[version]['*/*CHANOBS_DOMAIN1'], 1),
        ('*/*GWOUT_DOMAIN1', cycle_answer_reprs[version]['*/*GWOUT_DOMAIN1'], 1),
        ('*/*[0-9].RTOUT_DOMAIN1', cycle_answer_reprs[version]['*/*RTOUT_DOMAIN1'], 2),
        ('*/*LDASOUT_DOMAIN1', cycle_answer_reprs[version]['*/*LDASOUT_DOMAIN1'], 3),
        ('*/*LSMOUT_DOMAIN', cycle_answer_reprs[version]['*/*LSMOUT_DOMAIN'], 2),
        ('*/RESTART.*_DOMAIN1', cycle_answer_reprs[version]['*/RESTART.*_DOMAIN1'], 3),
        ('*/HYDRO_RST.*_DOMAIN1', cycle_answer_reprs[version]['*/HYDRO_RST.*_DOMAIN1'], 3)
    ],
    ids=[
        'cycle-CHRTOUT_DOMAIN1',
        'cycle-LAKEOUT_DOMAIN1',
        'cycle-CHANOBS_DOMAIN1',
        'cycle-GWOUT_DOMAIN1',
        'cycle-RTOUT_DOMAIN1',
        'cycle-LDASOUT_DOMAIN1',
        'cycle-LSMOUT_DOMAIN',
        'cycle-RESTART.*_DOMAIN1',
        'cycle-HYDRO_RST.*_DOMAIN1'
    ]
)
def test_collect_cycle(
    file_glob,
    expected,
    n_cores
):
    cycle_path = test_dir.joinpath(cycle_dir)
    files = sorted(cycle_path.glob(file_glob))
    cycle_ds = open_whp_dataset(files, n_cores=n_cores)
    # This checks everything about the metadata.
    from pprint import pprint
    assert repr(cycle_ds) == expected


# Ensemble
# Make an ensemble dir and set it up from the ensemble cycle.
ens_dir = test_dir / 'data/collection_data/ensemble'
# delete the directory here.
if ens_dir.exists():
    ens_dir.unlink()
ens_dir.symlink_to(test_dir / 'data/collection_data/ens_ana/cast_2011082600')


@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores'],
    [
        ('*/*CHRTOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*CHRTOUT_DOMAIN1'], 1),
        ('*/*LAKEOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*LAKEOUT_DOMAIN1'], 1),
        ('*/*CHANOBS_DOMAIN1', ensemble_answer_reprs[version]['*/*CHANOBS_DOMAIN1'], 1),
        ('*/*GWOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*GWOUT_DOMAIN1'], 1),
        ('*/*[0-9].RTOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*RTOUT_DOMAIN1'], 2),
        ('*/*LDASOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*LDASOUT_DOMAIN1'], 3),
        ('*/*LSMOUT_DOMAIN', ensemble_answer_reprs[version]['*/*LSMOUT_DOMAIN'], 2),
        ('*/RESTART.*_DOMAIN1', ensemble_answer_reprs[version]['*/RESTART.*_DOMAIN1'], 3),
        ('*/HYDRO_RST.*_DOMAIN1', ensemble_answer_reprs[version]['*/HYDRO_RST.*_DOMAIN1'], 3),
    ],
    ids=[
        'ensemble-CHRTOUT_DOMAIN1',
        'ensemble-LAKEOUT_DOMAIN1',
        'ensemble-CHANOBS_DOMAIN1',
        'ensemble-GWOUT_DOMAIN1',
        'ensemble-RTOUT_DOMAIN1',
        'ensemble-LDASOUT_DOMAIN1',
        'ensemble-LSMOUT_DOMAIN',
        'ensemble-RESTART.*_DOMAIN1',
        'ensemble-HYDRO_RST.*_DOMAIN1'
    ]
)
def test_collect_ensemble(
    file_glob,
    expected,
    n_cores
):
    ens_path = test_dir.joinpath(ens_dir)
    files = sorted(ens_path.glob(file_glob))
    ens_ds = open_whp_dataset(files, n_cores=n_cores)
    # This checks everything about the metadata.
    assert repr(ens_ds) == expected


# Ensemble Cycle
@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores'],
    [
        (
            '*/*/*CHRTOUT_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/*CHRTOUT_DOMAIN1'],
            1
        ),
        (
            '*/*/*LAKEOUT_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/*LAKEOUT_DOMAIN1'],
            2
        ),
        (
            '*/*/*CHANOBS_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/*CHANOBS_DOMAIN1'],
            1
        ),
        (
            '*/*/*GWOUT_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/*GWOUT_DOMAIN1'],
            1
        ),
        (
            '*/*/*[0-9].RTOUT_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/*RTOUT_DOMAIN1'],
            1),
        (
            '*/*/*LDASOUT_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/*LDASOUT_DOMAIN1'],
            3
        ),
        (
            '*/*/*LSMOUT_DOMAIN',
            ensemble_cycle_answer_reprs[version]['*/*/*LSMOUT_DOMAIN'],
            2
        ),
        (
            '*/*/RESTART.*_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/RESTART.*_DOMAIN1'],
            3
        ),
        (
            '*/*/HYDRO_RST.*_DOMAIN1',
            ensemble_cycle_answer_reprs[version]['*/*/HYDRO_RST.*_DOMAIN1'],
            3
        ),
    ],
    ids=[
        'ensemble_cycle-CHRTOUT_DOMAIN1',
        'ensemble_cycle-LAKEOUT_DOMAIN1',
        'ensemble_cycle-CHANOBS_DOMAIN1',
        'ensemble_cycle-GWOUT_DOMAIN1',
        'ensemble_cycle-RTOUT_DOMAIN1',
        'ensemble_cycle-LDASOUT_DOMAIN1',
        'ensemble_cycle-LSMOUT_DOMAIN',
        'ensemble_cycle-RESTART.*_DOMAIN1',
        'ensemble_cycle-HYDRO_RST.*_DOMAIN1'
    ]
)
def test_collect_ensemble_cycle(
    file_glob,
    expected,
    n_cores
):
    ens_cycle_path = test_dir.joinpath('data/collection_data/ens_ana')
    files = sorted(ens_cycle_path.glob(file_glob))
    ens_cycle_ds = open_whp_dataset(files, n_cores=n_cores)
    # This checks everything about the metadata.
    assert repr(ens_cycle_ds) == expected

    # Test that hierarchical collects are identical
    ens_cycle_ds_hier = open_whp_dataset(files, n_cores=n_cores, file_chunk_size=1)
    assert ens_cycle_ds_hier.equals(ens_cycle_ds)


# Missing/bogus files.
# Do this for ensemble cycle as that's the most complicated relationship to the missing file.
miss_ens_cycle_dir = test_dir / 'data/collection_data/miss_ens_cycle'
if miss_ens_cycle_dir.exists():
    shutil.rmtree(str(miss_ens_cycle_dir))
miss_ens_cycle_dir.mkdir()
os.chdir(str(miss_ens_cycle_dir))
orig_dir = test_dir / 'data/collection_data/ens_ana/'
casts = sorted(orig_dir.glob('cast_*'))
pkl_file = sorted(orig_dir.glob("*.pkl"))[0]
pathlib.Path(pkl_file.name).symlink_to(pkl_file)
for cc in casts:
    pathlib.Path(cc.name).symlink_to(cc)
# Break the last one.
pathlib.Path(cc.name).unlink()
pathlib.Path(cc.name).mkdir()
os.chdir(cc.name)
member_dirs = \
    sorted((test_dir / ('data/collection_data/ens_ana/' + cc.name)).glob('member_*'))
for mm in member_dirs:
    pathlib.Path(mm.name).symlink_to(mm)
# Break the last one.
pathlib.Path(mm.name).unlink()
pathlib.Path(mm.name).mkdir()
orig_ens_dir = test_dir / ('data/collection_data/ens_ana/' + cc.name)
orig_sim_dir = orig_ens_dir / mm.name
pkl_file = sorted(orig_ens_dir.glob("*.pkl"))[0]
pathlib.Path(pkl_file.name).symlink_to(pkl_file)
os.chdir(mm.name)
chrtout_files = sorted(orig_sim_dir.glob('*CHRTOUT*'))
for cc in chrtout_files:
    pathlib.Path(cc.name).symlink_to(cc)
pathlib.Path(cc.name).unlink()
pathlib.Path(cc.name).symlink_to('/foo/bar')


@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores'],
    [
        (
            '*/*/*CHRTOUT_DOMAIN1',
            miss_ens_cycle_answer_reprs[version]['*/*/*CHRTOUT_DOMAIN1'],
            1
        ),
        (
            '*/*/RESTART.*_DOMAIN1',
            miss_ens_cycle_answer_reprs[version]['*/*/RESTART.*_DOMAIN1'],
            2
        ),
        (
            '*/*/HYDRO_RST.*_DOMAIN1',
            miss_ens_cycle_answer_reprs[version]['*/*/HYDRO_RST.*_DOMAIN1'],
            3
        )
    ],
    ids=[
        'missing_ens_cycle-CHRTOUT_DOMAIN1',
        'missing_ens_cycle-RESTART.*_DOMAIN1',
        'missing_ens_cycle-HYDRO_RST.*_DOMAIN1'
    ]
)
def test_collect_missing_ens_cycle(
    file_glob,
    expected,
    n_cores
):
    miss_ens_cycle_path = test_dir.joinpath(miss_ens_cycle_dir)
    files = sorted(miss_ens_cycle_path.glob(file_glob))
    ens_cycle_ds = open_whp_dataset(files, n_cores=n_cores)
    # This checks everything about the metadata.
    assert repr(ens_cycle_ds) == expected

    ens_cycle_ds_hier = open_whp_dataset(files, n_cores=n_cores, file_chunk_size=1)
    assert ens_cycle_ds_hier.equals(ens_cycle_ds)


# Exercise profile and chunking.
@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores'],
    [
        ('*CHRTOUT_DOMAIN1', profile_chunking_answer_reprs[version]['*CHRTOUT_DOMAIN1'], 1)
    ],
    ids=[
        'profile_chunking-CHRTOUT_DOMAIN1'
    ]
)
def test_collect_profile_chunking(
    file_glob,
    expected,
    n_cores
):
    sim_path = test_dir.joinpath(sim_dir)
    files = sorted(sim_path.glob(file_glob))
    sim_ds = open_whp_dataset(files, n_cores=n_cores, profile=True, chunks=15)
    # This checks everything about the metadata.
    assert repr(sim_ds) == expected

    # if file_chunk_size > and chunk is not None there is an error.
    sim_ds_hier = open_whp_dataset(
        files, n_cores=n_cores, profile=True, chunks=15, file_chunk_size=2)
    assert sim_ds_hier.equals(sim_ds)


# Test spatial index selection
# Ensemble Cycle
@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores', 'isel'],
    [
        (
            '*/*/*CHRTOUT_DOMAIN1',
            ensemble_cycle_isel_answer_reprs[version]['*/*/*CHRTOUT_DOMAIN1'],
            1,
            {'feature_id': [1, 2]}
        ),
        (
            '*/*/RESTART.*_DOMAIN1',
            ensemble_cycle_isel_answer_reprs[version]['*/*/RESTART.*_DOMAIN1'],
            3,
            {'snow_layers': [1, 2], 'west_east': [0, 1, 2]}
        ),
        (
            '*/*/HYDRO_RST.*_DOMAIN1',
            ensemble_cycle_isel_answer_reprs[version]['*/*/HYDRO_RST.*_DOMAIN1'],
            3,
            {'links': [0], 'lakes':[0], 'iy':[0, 1]}
        ),
    ],
    ids=[
        'ensemble_cycle_isel-CHRTOUT_DOMAIN1',
        'ensemble_cycle_isel-RESTART.*_DOMAIN1',
        'ensemble_cycle_isel-HYDRO_RST.*_DOMAIN1'
    ]
)
def test_collect_ensemble_cycle_isel(
    file_glob,
    expected,
    n_cores,
    isel
):
    ens_cycle_path = test_dir.joinpath('data/collection_data/ens_ana')
    files = sorted(ens_cycle_path.glob(file_glob))
    ens_cycle_ds = open_whp_dataset(files, n_cores=n_cores, isel=isel)
    # This checks everything about the metadata.
    assert repr(ens_cycle_ds) == expected

    ens_cycle_ds_hier = open_whp_dataset(files, n_cores=n_cores, isel=isel, file_chunk_size=1)
    assert ens_cycle_ds_hier.equals(ens_cycle_ds)


# Test dropping/keeping variables
# Ensemble Cycle
@pytest.mark.parametrize(
    ['file_glob', 'expected', 'n_cores', 'drop_vars'],
    [
        (
            '*/*/*CHRTOUT_DOMAIN1',
            ensemble_cycle_drop_vars_answer_reprs[version]['*/*/*CHRTOUT_DOMAIN1'],
            1,
            ['Head', 'crs']
        ),
        (
            '*/*/RESTART.*_DOMAIN1',
            ensemble_cycle_drop_vars_answer_reprs[version]['*/*/RESTART.*_DOMAIN1'],
            3,
            ['SOIL_T', 'SNOW_T', 'SMC', 'SH2O', 'ZSNSO']
        ),
        (
            '*/*/HYDRO_RST.*_DOMAIN1',
            ensemble_cycle_drop_vars_answer_reprs[version]['*/*/HYDRO_RST.*_DOMAIN1'],
            3,
            ['z_gwsubbas', 'resht', 'sfcheadsubrt']
        ),
    ],
    ids=[
        'ensemble_cycle_drop_vars-CHRTOUT_DOMAIN1',
        'ensemble_cycle_drop_vars-RESTART.*_DOMAIN1',
        'ensemble_cycle_drop_vars-HYDRO_RST.*_DOMAIN1'
    ]
)
def test_collect_ensemble_cycle_drop_vars(
    file_glob,
    expected,
    n_cores,
    drop_vars
):
    ens_cycle_path = test_dir.joinpath('data/collection_data/ens_ana')
    files = sorted(ens_cycle_path.glob(file_glob))
    ens_cycle_ds = open_whp_dataset(files, n_cores=n_cores, drop_variables=drop_vars)
    # This checks everything about the metadata.
    assert repr(ens_cycle_ds) == expected

    ens_cycle_ds_hier = open_whp_dataset(
        files, n_cores=n_cores, drop_variables=drop_vars, file_chunk_size=1)
    assert ens_cycle_ds_hier.equals(ens_cycle_ds)
