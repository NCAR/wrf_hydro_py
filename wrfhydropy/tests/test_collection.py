import os
import pathlib
import pytest
from wrfhydropy import open_ensemble_dataset
from .data import collection_data_download

# The answer reprs are found here.
from .data.collection_data_answer_reprs import *

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))

# The data are found here.
collection_data_download.download()

# Issues raised by these tests
# https://github.com/NCAR/wrf_hydro_nwm_public/issues/301
# https://github.com/NCAR/wrf_hydro_nwm_public/issues/302

# TODO: Test multiple versions (current and previous)
version_file = test_dir.joinpath('data/collection_data/croton_NY/.version')
version = version_file.open('r').read().split('-')[0]


# Simulation
# Make a sim dir to a single simulation.
sim_dir = test_dir / 'data/simulation'
if not sim_dir.exists():
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
    sim_ds = open_ensemble_dataset(files)
    # This checks everything about the metadata.
    assert repr(sim_ds) == expected


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
    ens_ds = open_ensemble_dataset(files)
    # This checks everything about the metadata.
    assert repr(ens_ds) == expected


# Test dropping/keeping variables
# Test spatial index selection
# Test when files are missing or bogus
