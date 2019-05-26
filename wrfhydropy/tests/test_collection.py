import pathlib
import pytest
from wrfhydropy import open_ensemble_dataset

# The answer reprs are found here.
from .data.collection_data_answer_reprs import *

# Issues raised by these tests
# https://github.com/NCAR/wrf_hydro_nwm_public/issues/301
# https://github.com/NCAR/wrf_hydro_nwm_public/issues/302

version_file = pathlib.Path('data/collection_data/croton_NY/.version')
version = version_file.open('r').read().split('-')[0]


@pytest.mark.parametrize(
    ['file_glob', 'expected'],
    [
        ('*/RESTART.*_DOMAIN1', ensemble_answer_reprs[version]['*/RESTART.*_DOMAIN1']),
        ('*/*CHRTOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*CHRTOUT_DOMAIN1']),
        ('*/*LAKEOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*LAKEOUT_DOMAIN1']),
        ('*/*CHANOBS_DOMAIN1', ensemble_answer_reprs[version]['*/*CHANOBS_DOMAIN1']),
        ('*/*GWOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*GWOUT_DOMAIN1']),
        ('*/*[0-9].RTOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*RTOUT_DOMAIN1']),
        ('*/*LDASOUT_DOMAIN1', ensemble_answer_reprs[version]['*/*LDASOUT_DOMAIN1']),
        ('*/*LSMOUT_DOMAIN', ensemble_answer_reprs[version]['*/*LSMOUT_DOMAIN']),
        ('*/HYDRO_RST.*_DOMAIN1', ensemble_answer_reprs[version]['*/HYDRO_RST.*_DOMAIN1']),
    ]
)
def test_collect_ensemble(
    file_glob,
    expected
):
    sim_path = pathlib.Path('data/collection_data/ens_ana/cast_2011082600/')
    files = sorted(sim_path.glob(file_glob))
    ens_ds = open_ensemble_dataset(files)
    # This checks everything about the metadata.
    assert repr(ens_ds) == expected


# Test dropping/keeping variables
# Test spatial index selection
# Test when files are missing or bogus
