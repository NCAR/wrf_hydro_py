import os
import pathlib
import pandas
import pytest
import warnings

from wrfhydropy import Evaluation, open_whp_dataset
from .data import collection_data_download
from .data.evaluation_answer_reprs import *

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))
# The data are found here. Uses the same data as collection.
collection_data_download.download()


@pytest.mark.parametrize(
    ['mod_dir', 'mod_glob'],
    [
        (test_dir / 'data/collection_data/simulation', '*CHRTOUT_DOMAIN1'),
    ],
    ids=[
        'init-simulation',
    ]
)
def test_init(mod_dir, mod_glob):

    files = sorted(mod_dir.glob(mod_glob))
    mod = open_whp_dataset(files)
    mod_df = mod.streamflow.to_dataframe()
    obs_df = mod_df
    streamflow_eval = Evaluation(mod_df, obs_df)
    assert type(streamflow_eval) == Evaluation


# Should there be a "stage" prt of collection_data_download? I
# would have to look more closely at test_collection. The
#  following is certainly repeated code
sim_dir = test_dir / 'data/collection_data/simulation'
if sim_dir.exists():
    sim_dir.unlink()
sim_dir.symlink_to(test_dir / 'data/collection_data/ens_ana/cast_2011082600/member_000')

@pytest.mark.parametrize(
    ['mod_dir', 'mod_glob', 'indices_dict', 'variable', 'expected'],
    [
        (test_dir / 'data/collection_data/simulation',
         '*CHRTOUT_DOMAIN1',
         {'feature_id': [1, 39, 56, 34]},
         'streamflow',
         gof_answer_reprs['*CHRTOUT_DOMAIN1']
        ),
        (test_dir / 'data/collection_data/simulation',
         '*LDASOUT_DOMAIN1',
         {'x': [1, 3, 5], 'y': [2, 4, 6], 'soil_layers_stag': [2]},
         'SOIL_M',
         gof_answer_reprs['*LDASOUT_DOMAIN1']
        ),
    ],
    ids=[
        'gof-simulation-CHRTOUT',
        'gof-simulation-LSMOUT',
    ]
)
def test_gof(mod_dir, mod_glob, indices_dict, variable, expected):
    # Keep this variable agnostic
    files = sorted(mod_dir.glob(mod_glob))
    mod = open_whp_dataset(files).isel(indices_dict)
    mod_df = mod[variable].to_dataframe().rename(
        columns={variable: 'modeled'})
    obs_df = mod[variable].to_dataframe().rename(
        columns={variable: 'observed'})
    the_eval = Evaluation(mod_df, obs_df, join_on=[*indices_dict])
    gof = the_eval.gof()
    assert repr(gof) == expected
