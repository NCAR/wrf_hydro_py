import os
import pathlib
import pandas
import pytest
import warnings

from wrfhydropy import Evaluation, open_whp_dataset

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))


@pytest.mark.parametrize(
    ['mod_dir', 'mod_glob'],
    [
        (test_dir / 'data/collection_data/simulation',
         '*CHRTOUT_DOMAIN1'),
    ],
    ids=[
        'evaluation_init-simulation',
    ]
)
def test_evaluation_init(mod_dir, mod_glob):

    files = sorted(mod_dir.glob(mod_glob))
    mod = open_whp_dataset(files)
    mod_df = mod.streamflow.to_dataframe()
    obs_df = mod_df
    streamflow_eval = Evaluation(mod_df, obs_df)
    assert type(streamflow_eval) == Evaluation
