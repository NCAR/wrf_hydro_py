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
    ['file_glob', 'expected'],
    [
        ('*CHRTOUT_DOMAIN1', simulation_answer_reprs[version]['*CHRTOUT_DOMAIN1']),
        ('*LAKEOUT_DOMAIN1', simulation_answer_reprs[version]['*LAKEOUT_DOMAIN1']),
        ('*CHANOBS_DOMAIN1', simulation_answer_reprs[version]['*CHANOBS_DOMAIN1']),
        ('*GWOUT_DOMAIN1', simulation_answer_reprs[version]['*GWOUT_DOMAIN1']),
        ('*[0-9].RTOUT_DOMAIN1', simulation_answer_reprs[version]['*RTOUT_DOMAIN1']),
        ('*LDASOUT_DOMAIN1', simulation_answer_reprs[version]['*LDASOUT_DOMAIN1']),
        ('*LSMOUT_DOMAIN', simulation_answer_reprs[version]['*LSMOUT_DOMAIN']),
        ('RESTART.*_DOMAIN1', simulation_answer_reprs[version]['RESTART.*_DOMAIN1']),
        ('HYDRO_RST.*_DOMAIN1', simulation_answer_reprs[version]['HYDRO_RST.*_DOMAIN1']),
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
    expected
):
    sim_path = test_dir.joinpath(sim_dir)
    files = sorted(sim_path.glob(file_glob))
    sim_ds = open_whp_dataset(files)
    # This checks everything about the metadata.
    assert repr(sim_ds) == expected


# Cycle

# TODO: add a valid_time variable.

# Make a cycle dir and set it up from the ensemble cycle.
cycle_dir = test_dir / 'data/collection_data/cycle'
# delete the directory here.
if cycle_dir.exists():
    shutil.rmtree(str(cycle_dir))
cycle_dir.mkdir()
os.chdir(str(cycle_dir))
for cast in test_dir.joinpath('data/collection_data/ens_ana').glob('cast_*'):
    cast_name = pathlib.Path(cast.name)
    cast_name.symlink_to(cast.joinpath('member_000'))


@pytest.mark.parametrize(
    ['file_glob', 'expected'],
    [
        ('*/*CHRTOUT_DOMAIN1', cycle_answer_reprs[version]['*/*CHRTOUT_DOMAIN1']),
        ('*/*LAKEOUT_DOMAIN1', cycle_answer_reprs[version]['*/*LAKEOUT_DOMAIN1']),
        ('*/*CHANOBS_DOMAIN1', cycle_answer_reprs[version]['*/*CHANOBS_DOMAIN1']),
        ('*/*GWOUT_DOMAIN1', cycle_answer_reprs[version]['*/*GWOUT_DOMAIN1']),
        ('*/*[0-9].RTOUT_DOMAIN1', cycle_answer_reprs[version]['*/*RTOUT_DOMAIN1']),
        ('*/*LDASOUT_DOMAIN1', cycle_answer_reprs[version]['*/*LDASOUT_DOMAIN1']),
        ('*/*LSMOUT_DOMAIN', cycle_answer_reprs[version]['*/*LSMOUT_DOMAIN']),
        ('*/RESTART.*_DOMAIN1', cycle_answer_reprs[version]['*/RESTART.*_DOMAIN1']),
        ('*/HYDRO_RST.*_DOMAIN1', cycle_answer_reprs[version]['*/HYDRO_RST.*_DOMAIN1'])
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
    expected
):
    cycle_path = test_dir.joinpath(cycle_dir)
    files = sorted(cycle_path.glob(file_glob))
    cycle_ds = open_whp_dataset(files)
    # This checks everything about the metadata.
    from pprint import pprint
    assert repr(cycle_ds) == expected

# =============================================================================    
# Ensemble
    
@pytest.mark.parametrize(
    ['file_glob', 'expected'],
    [
        ('*/*CHRTOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*CHRTOUT_DOMAIN1']),
        ('*/*LAKEOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*LAKEOUT_DOMAIN1']),
        ('*/*CHANOBS_DOMAIN1', ensemble_answer_reprs[version]['*/*CHANOBS_DOMAIN1']),
        ('*/*GWOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*GWOUT_DOMAIN1']),
        ('*/*[0-9].RTOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*RTOUT_DOMAIN1']),
        ('*/*LDASOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*LDASOUT_DOMAIN1']),
        ('*/*LSMOUT_DOMAIN', ensemble_answer_reprs[version]['*/*LSMOUT_DOMAIN']),
        ('*/RESTART.*_DOMAIN1', ensemble_answer_reprs[version]['*/RESTART.*_DOMAIN1']),
        ('*/HYDRO_RST.*_DOMAIN1', ensemble_answer_reprs[version]['*/HYDRO_RST.*_DOMAIN1']),
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
    expected
):
    ens_path = test_dir.joinpath('data/collection_data/ens_ana/cast_2011082600/')
    files = sorted(ens_path.glob(file_glob))
    ens_ds = open_whp_dataset(files)
    # This checks everything about the metadata.
    assert repr(ens_ds) == expected


# Test dropping/keeping variables
# Test spatial index selection
# Test when files are missing or bogus
