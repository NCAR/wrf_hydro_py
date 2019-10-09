import copy
import os
import pathlib
import pandas
import pytest
import warnings

from wrfhydropy import Evaluation, open_whp_dataset
from .data import collection_data_download
from .data.evaluation_answer_reprs import *

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))

# Get the full reprs
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)

# The data are found here. Uses the same data as collection.
os.chdir(str(test_dir))
collection_data_download.download()

engine = ['pd', 'xr']

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


@pytest.mark.parametrize('engine', engine)
@pytest.mark.parametrize('group_by_in', [None, 'space'])
@pytest.mark.parametrize(
    ['mod_dir', 'mod_glob', 'indices_dict', 'join_on', 'variable', 'transform', 'expected_key'],
    [
        (test_dir / 'data/collection_data/simulation',
         '*CHRTOUT_DOMAIN1',
         {'feature_id': [1, 39, 56, 34]},
         ['time', 'feature_id'],
         'streamflow',
         lambda x: x,
         '*CHRTOUT_DOMAIN1'
        ),
        (test_dir / 'data/collection_data/simulation',
         '*LDASOUT_DOMAIN1',
         {'x': [1, 3, 5], 'y': [2, 4, 6], 'soil_layers_stag': [2]},
         ['time', 'x', 'y', 'soil_layers_stag'],
         'SOIL_M',
         lambda x: x,
         '*LDASOUT_DOMAIN1'
        ),
    ],
    ids=[
        'gof-simulation-CHRTOUT',
        'gof-simulation-LSMOUT',
    ]
)
def test_gof_perfect(
    engine,
    mod_dir,
    mod_glob,
    indices_dict,
    join_on,
    variable,
    group_by_in,
    transform,
    expected_key
):
    # Keep this variable agnostic
    files = sorted(mod_dir.glob(mod_glob))
    mod = open_whp_dataset(files).isel(indices_dict)

    if group_by_in is None:
        group_by_key = ''
        group_by = None
    elif group_by_in == 'space':
        group_by_key = '-' + group_by_in
        group_by = copy.deepcopy(join_on)
        group_by.remove('time')
    else:
        raise ValueError("not a valid grouping for this test: ", group_by)

    expected = gof_answer_reprs[expected_key + group_by_key]

    if engine == 'pd':
        mod_df = mod[variable].to_dataframe().rename(
            columns={variable: 'modeled'})
        obs_df = mod[variable].to_dataframe().rename(
            columns={variable: 'observed'})
        #mod_df = transform(mod_df)
        the_eval = Evaluation(mod_df, obs_df, join_on=join_on)
        gof = the_eval.gof(group_by=group_by)
        assert repr(gof) == expected

    elif engine == 'xr':
        if group_by_in is not None:
            pytest.skip("Currently not grouping using xarray.")
        mod_ds = mod.rename({variable: 'modeled'})['modeled']
        obs_ds = mod.rename({variable: 'observed'})['observed']
        the_eval = Evaluation(mod_ds, obs_ds, join_on=join_on)
        gof = the_eval.gof(group_by=group_by)

        assert repr(gof) == expected
