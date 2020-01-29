import copy
import datetime
import math
import numpy as np
import os
import pathlib
import pandas as pd
import pytest
import warnings
import xarray as xr

from io import StringIO
from pandas.testing import assert_frame_equal
from wrfhydropy import Evaluation, open_whp_dataset
from .data import collection_data_download
from .data.evaluation_answer_reprs import *

# Testing helper functons for data frames. Serialization is a PITA.
float_form = '.2f'


def assert_frame_close(df1, df2):
    assert_frame_equal(df1, df2, check_exact=False)


def str_to_frame(string: str):
    return(pd.read_csv(StringIO(string)))


def frame_to_str(frame: pd.DataFrame):
    return(frame.to_csv(float_format='%' + float_form))


def round_trip_df_serial(frame: pd.DataFrame):
    return(str_to_frame(frame_to_str(frame)))


pd.options.display.float_format = ('{:' + float_form + '}').format

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))

# Get the full reprs
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

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
    ['transform', 'transform_key'],
    [(lambda x: x, 'identity'),
     (lambda x: [ii for ii in range(len(x))], 'index')],
    ids=['lambda_identity', 'lambda_index'])
@pytest.mark.parametrize(
    ['mod_dir', 'mod_glob', 'indices_dict', 'join_on', 'variable', 'expected_key'],
    [
        (test_dir / 'data/collection_data/simulation',
         '*CHRTOUT_DOMAIN1',
         {'feature_id': [1, 39, 56, 34]},
         ['time', 'feature_id'],
         'streamflow',
         '*CHRTOUT_DOMAIN1'),
        (test_dir / 'data/collection_data/simulation',
         '*LDASOUT_DOMAIN1',
         {'x': [1, 3, 5], 'y': [2, 4, 6], 'soil_layers_stag': [2]},
         ['time', 'x', 'y', 'soil_layers_stag'],
         'SOIL_M',
         '*LDASOUT_DOMAIN1'),
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
    transform_key,
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

    expected_answer_key = expected_key + group_by_key + '_' + transform_key
    # expected = gof_answer_reprs[expected_answer_key]
    expected = str_to_frame(gof_answer_reprs[expected_answer_key])

    if engine == 'pd':
        mod_df = mod[variable].to_dataframe().rename(
            columns={variable: 'modeled'})
        obs_df = mod[variable].to_dataframe().rename(
            columns={variable: 'observed'})
        mod_df.modeled = transform(mod_df.modeled)
        the_eval = Evaluation(mod_df, obs_df, join_on=join_on)
        gof = the_eval.gof(group_by=group_by)
        assert_frame_close(round_trip_df_serial(gof), expected)

    elif engine == 'xr':
        if group_by_in is not None:
            pytest.skip("Currently not grouping using xarray.")
        mod_ds = mod.rename({variable: 'modeled'})['modeled']
        obs_ds = mod.rename({variable: 'observed'})['observed']
        new_data = np.array(transform(mod_ds.to_dataframe().modeled)).reshape(mod_ds.shape)
        mod_ds.values = new_data
        # mod_ds = xr.DataArray(new_data, dims=mod_ds.dims, coords=mod_ds.coords)
        the_eval = Evaluation(mod_ds, obs_ds, join_on=join_on)
        gof = the_eval.gof(group_by=group_by).to_dataframe()
        # assert repr(gof) == expected
        assert_frame_close(round_trip_df_serial(gof), expected)


@pytest.mark.parametrize('engine', engine)
@pytest.mark.parametrize('the_stat', ['crps', 'brier'])
def test_crps_brier_basic(
    the_stat,
    engine
):

    # The input data for the test
    ens0 = np.linspace(-5, 5, num=1000)
    ens1 = np.linspace(-500, 500, num=1000)
    obs = 0.0000

    # WOw i must be a dunce, this is way too much work.
    t0 = datetime.datetime(2000, 1, 1)
    t1 = datetime.datetime(2000, 1, 2)
    modeled = pd.DataFrame(
        np.array([ens0, ens1]).transpose(),
        columns=[t0, t1]
    )
    modeled.index.name = 'member'
    modeled = modeled.reset_index()
    modeled = modeled.melt(
        id_vars=['member'],
        var_name='time',
        value_name='modeled'
    ).set_index(['time', 'member'])
    observed = modeled.rename(columns={'modeled': 'observed'}) * obs

    if engine == 'xr':
        pytest.skip("Currently using xarray for brier and crps.")
        modeled = modeled.to_xarray()['modeled']
        observed = observed.to_xarray()['observed']

    the_eval = Evaluation(modeled, observed)

    if the_stat == 'crps':
        # Generate the answer
        # import properscoring as ps
        # answer = np.array([ps.crps_ensemble(obs, mod) for mod in [ens0, ens1]])
        answer = pd.DataFrame(
            {'time': [t0, t1],
             'crps': np.array([0.83416917, 83.41691692])}
        ).set_index('time')
        crps = the_eval.crps()
        assert_frame_close(crps, answer)

    elif the_stat == 'brier':
        threshold = 1
        # Generate the answer
        # import properscoring as ps
        # answer = np.array([ps.threshold_brier_score(obs, mod, threshold=threshold)
        #                   for mod in [ens0, ens1]])
        # answer = pd.DataFrame(
        #     {'time': [t0, t1],
        #      'crps': np.array([ 0.83416917, 83.41691692])}
        # ).set_index('time')
        answer = np.array([0.16, 0.249001])
        brier = the_eval.brier(threshold)
        assert np.isclose(brier, answer).all()


# Inputs for contingency and event stat calculations.
# Answers are in data/evaluation_answer_reprs.py
base_dum_time = datetime.datetime(2000, 1, 1)
dumtime = [base_dum_time + datetime.timedelta(hours=dd) for dd in range(4)]

# Easy to read and interpret inputs and grouped output.
contingency_known_data_input = pd.DataFrame({
    #       hits         #mix           # misses         # false pos      # corr_neg
    'mod': [1, 1, 1, 1,   1, -1,  1, -1,  -1, -1, -1, -1,   1,  1,  1,  1,  -1, -1, -1, -1],
    'obs': [1, 1, 1, 1,   1,  1, -1, -1,   1,  1,  1,  1,  -1, -1, -1, -1,  -1, -1, -1, -1],
    'tsh': [0, 0, 0, 0,   0,  0,  0,  0,   0,  0,  0,  0,   0,  0,  0,  0,   0,  0,  0,  0],
    'loc': (['hits']*4)+ (['mix']*4)+     (['miss']*4)+  (['false_pos']*4)+  (['corr_neg']*4),
    'time': dumtime +     dumtime +        dumtime +      dumtime +           dumtime,
    }).set_index(['loc', 'time'])

# A threshold that varies across the group on which the calcuation is made.
contingency_known_data_input_2 = pd.DataFrame({
    #       hits             #mix             # misses          # false pos      # corr_neg
    'mod': [1, 11, 111, 1,   1, 1,  111,  1,   0, 10, 110, -1,   1, 11, 111, 2,  0, 2, 10, 17],
    'obs': [1, 11, 111, 1,   1, 11,   1,  1,   2, 11, 111,  1,   0, 10, 110, 1,  0, 2, 10, 13],
    'tsh': [0, 10, 110, 0,   0, 10, 110, 10,   1, 10, 110,  0,   0, 10, 110, 1,  1, 3, 11, 20],
    'loc': (['hits']*4)+    (['mix']*4)+      (['miss']*4)+   (['false_pos']*4)+ (['corr_neg']*4),
    'time': dumtime +     dumtime +        dumtime +      dumtime +           dumtime,
    }).set_index(['loc', 'time'])

# TODO: test NaNs in the data


# @pytest.mark.parametrize('engine', engine)
@pytest.mark.parametrize(
    'input_data',
    [contingency_known_data_input, contingency_known_data_input_2])
def test_contingency_known_data(input_data):
    known_data = input_data.to_xarray().set_coords("tsh")
    mod = known_data.mod.drop('tsh')
    obs = known_data.obs
    result = mod.eval.obs(obs).contingency(threshold='tsh', group_by='loc')
    result = round_trip_df_serial(result)
    expected = str_to_frame(contingency_known_data_answer)
    assert_frame_close(result, expected)


# @pytest.mark.parametrize('engine', engine)
@pytest.mark.parametrize(
    'input_data',
    [contingency_known_data_input, contingency_known_data_input_2])
def test_contingency_missing_columns(input_data):
    known_data = input_data.to_xarray().set_coords("tsh")
    mod = known_data.mod.drop('tsh')
    obs = known_data.obs
    result = mod.eval.obs(obs).contingency(threshold='tsh', group_by='loc')
    result = round_trip_df_serial(result)
    expected = str_to_frame(contingency_known_data_answer)
    assert_frame_close(result, expected)


@pytest.mark.parametrize(
    'input_data',
    [contingency_known_data_input, contingency_known_data_input_2])
def test_event_known_data(input_data):
    known_data = input_data.to_xarray().set_coords("tsh")
    mod = known_data.mod.drop('tsh')
    obs = known_data.obs
    result = mod.eval.obs(obs).event(threshold='tsh', group_by='loc')
    result = round_trip_df_serial(result)
    expected = str_to_frame(event_known_data_answer)
    assert_frame_close(result, expected)
