from typing import Union
import itertools
import numpy as np
import pandas as pd
import spotpy.objectivefunctions as spo
import xarray as xr


class Evaluation(object):
    """A dataset consisting of a modeled and observed dataframe.
    This class provides methods for calculating staistics."""

    def __init__(
        self,
        observed: Union[pd.DataFrame, xr.DataArray],
        modeled: Union[pd.DataFrame, xr.DataArray],
        join_on: Union[list, str] = None,
        join_how: str = 'inner'
    ):

        """
        Instantiate analysis class by joining modeled and observed datasets.
        Args:
            observed: Dataframe containing observed data
            modeled: Dataframe containing modelled data
            join_on: Optional, string or list of columns names to join datasets.
            Default is ['feature_id','time']
            join_how: Optional, how to perform teh dataframe join. Default is
            'inner'. Options
            are 'inner','left','right'.
        """
        if join_on is None:
            self.join_on = ['feature_id', 'time']
        else:
            self.join_on = join_on
            
        if not type(observed) == type(modeled):
            raise ValueError('Observed and modeled data are not of the same type.')
            
        if isinstance(observed, pd.DataFrame):            
            data = pd.merge(
                modeled,
                observed,
                on=self.join_on,
                how=join_how,
                suffixes=['_mod', '_obs']
            )

        elif isinstance(observed, xr.DataArray):
            data = xr.merge(
                [modeled, observed],
                join=join_how
            )

        self.data = data
        """pd.Dataframe: The dataframe to analyze"""

    @staticmethod
    def _group_calc_cont_stats(
        data,
        threshold: Union[float, str],
        label: str = None,
        time_window: str = None,
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        inf_as_na: bool = True,
        decimals: int = 2
    ):
        """Internal method to by applied to groups for contingency stats
        This private method provides an interface to calc_cont_stats() that
        handles groups processed by contingency(): This method constructs
        the contingency table based on the arguments provided.


        TODO: Why this? would be clearer if the range of options and what
              this is provding is clearer.

        TODO: can this be done in the public method? the arguments described
              there explain what is going on here...
        TODO: threhold: float, str or a zero length object
        """

        # TODO: Comment this opacity
        # TODO: Check if there are data above the above the threshold
        # TODO: how is threshold being used and then reset if it's a string?
        # TODO: Can one even index with a float?
        if len(data[threshold].dropna()) > 0:
            if type(threshold) == str:
                threshold = data[threshold].iloc[0]

            if label is None:
                label = threshold

            if time_window is None:
                observed = data[obs_col] > threshold
                modeled = data[mod_col] > threshold
            else:
                data.set_index('time', inplace=True)
                rolling_df = data.rolling(window=time_window)
                observed = rolling_df[obs_col].max() > threshold
                modeled = rolling_df[mod_col].max() > threshold
                data.reset_index('time', inplace=True)

            cont_table = calc_cont_table(observed, modeled)

            cont_stats = calc_cont_stats(
                cont_table,
                inf_as_na=inf_as_na,
                decimals=decimals
            )

            cont_stats['threshold'] = label
            cont_stats['value'] = cont_stats['value'].round(decimals=decimals)

            return cont_stats

        else:
            if label is None:
                label = threshold
            return pd.DataFrame(
                columns={
                    'statistic': np.nan,
                    'value': np.nan,
                    'threshold': label})

    @staticmethod
    def _calc_gof_stats(
        data,
        obs_col,
        mod_col,
        inf_as_na,
        decimals
    ):
        # TODO: This is just an interface... i guess, i think this can be removed
        return calc_gof_stats(
            data[obs_col],
            data[mod_col],
            inf_as_na=inf_as_na,
            decimals=decimals)

    @staticmethod
    # TODO: What are "flow metrics" and how do they differ form GOF stats?
    def _group_calc_event_stats(
        data,
        threshold: Union[float, str],
        label: str = None,
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        decimals: int = 2
    ):

        if len(data[threshold].dropna()) > 0:
            if type(threshold) == str:
                threshold = data[threshold].iloc[0]

            observed = data[obs_col] > threshold
            modeled = data[mod_col] > threshold

            event_stats = calc_event_stats(
                observed,
                modeled,
                decimals=decimals
            )

            if label is None:
                label = threshold
            event_stats['threshold'] = label
            event_stats['value'] = event_stats['value'].round(
                decimals=decimals)
            return event_stats

        else:
            if label is None:
                label = threshold
            return pd.DataFrame(columns={'statistic': np.nan,
                                         'value': np.nan,
                                         'threshold': label})

    def contingency(
        self,
        threshold: Union[float, str],
        label: str = None,
        time_window: str = None,
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        group_by: Union[list, str] = None,
        inf_as_na: bool = True,
        decimals: int = 2
    ):
        """
        Calculate contingency statistics

        Accuracy (acc, fraction correct) = (hits + correct_negatives)/total
        Bias score (bias, frequency bias) = (hits + false_alarms)/(hits +
        misses)
        Probability of detection (pod, hit rate) = hits/(hits + misses)
        False alarm ratio (far) = false_alarms/(hits + false_alarms)
        Probability of false detection (pofd, false alarm rate) =
        false_alarms/(false_alarms + correct_negatives)
        Success ratio (sr) = hits/(hits + false_alarms)
        Critical success index (csi, threat score) = hits/(hits + misses +
        false_alarms)
        Gilbert skill score (gss, Equitable threat score) =
        (hits - hits_random)/(hits + misses +
        false_alarms - hits_random) where hits_random = ((hits + misses)*(hits +
        false_alarms))/total
        Hanssen and Kuipers discriminant (hk, Peirce's skill score) = (hits/(
        hits+misses)) - (false_alarms/(false_alarms + correct_negatives))
        Odds ratio (or) = (POD/(1-POD))/(POFD/(1-POFD))
        Odds ratio skill score (orss, Yule's Q) = ((hits * correct_negatives) -
        (misses * false_alarms))/((hits * correct_negatives) +
        (misses * false_alarms)

        Args:
            threshold: The threshold value for contingency stats or column name
            in self.data containing threshold value. The first value of the
            column will be used as the threshold value.
            TODO JLM: I Do NOT love an entire column where a single value is 
                      used. I guess this allows different thresholds for 
                      different groups within the data.frame.

            label: Label for the threshold, value used if None.
            time_window: Calculate contingency statistics over a moving
            time window of specified width in seconds ('s'), hours ('h'),
            or days('d').
            mod_col: Column name of modelled data
            obs_col: Column name of observed data
            group_by: Column names to group by prior to calculating statistics
            inf_as_na: convert inf values to na?
            decimals: round stats to specified decimal places
        Returns:
            Pandas dataframe containing contingency statistics
        """
        if group_by:
            cont_stats = self.data.set_index(group_by).groupby(group_by). \
                apply(
                    self._group_calc_cont_stats,
                    threshold=threshold,
                    label=label,
                    time_window=time_window,
                    mod_col=mod_col,
                    obs_col=obs_col,
                    inf_as_na=inf_as_na,
                    decimals=decimals)

        else:
            cont_stats = self._group_calc_cont_stats(
                data=self.data,
                threshold=threshold,
                label=label,
                time_window=time_window,
                mod_col=mod_col,
                obs_col=obs_col,
                inf_as_na=inf_as_na,
                decimals=decimals)

        return cont_stats

    def gof(
        self,
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        group_by: Union[list, str] = None,
        inf_as_na: bool = True,
        decimals: int = 2
    ):
        """
        Calculate goodness of fit statistics using the spotpy package.
        See :py:fun:`calculate_all_functions() <calculate_all_functions>`
        in :py:mod:`spotpy`.
        Args:
            mod_col: Column name of modelled data
            obs_col: Column name of observed data
            group_by: Column names to group by prior to calculating statistics
            inf_as_na: convert inf values to na?
            decimals: round stats to specified decimal places
        Returns:
            Pandas dataframe containing contingency table
        """

        if isinstance(self.data, pd.DataFrame):
            
            if group_by is None:
                gof_stats = self._calc_gof_stats(
                    data=self.data,
                    obs_col=obs_col,
                    mod_col=mod_col,
                    inf_as_na=inf_as_na,
                    decimals=decimals)

            else:
                gof_stats = self.data.set_index(group_by). \
                    groupby(group_by). \
                    apply(
                        self._calc_gof_stats,
                        obs_col=obs_col,
                        mod_col=mod_col,
                        inf_as_na=inf_as_na,
                        decimals=decimals)
                gof_stats = gof_stats.rename(columns={'level_2': 'statistic'})

        elif isinstance(self.data, xr.Dataset):
            gof_stats = xr.apply_ufunc(
                spo_all_xr,
                self.data.observed,
                self.data.modeled,
                kwargs = {'inf_as_na': inf_as_na,
                          'decimals': decimals},
                input_core_dims=[self.join_on, self.join_on]#,
                #exclude_dims=set(self.join_on)
            ).values.tolist()[0].to_dataframe()

            gof_stats = gof_stats.reset_index().drop(columns='index')
            #gof_stats = gof_stats.rename(columns={0:'statistic', 1: 'value'})
            #gof_stats['value'] = gof_stats['value'].round(decimals=decimals)

            # fafaf
            # obs_grp = self.data.observed.groupby('feature_id')
            # mod_grp = self.data.modeled.groupby('feature_id')
            # gof_stats_xr = xr.apply_ufunc(spo_all_xr, obs_grp, mod_grp, input_core_dims = [core_dims, core_dims], exclude_dims = set(core_dims))

        return gof_stats

    def event(
        self,
        threshold: Union[float, str],
        label: str = None,
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        group_by: Union[list, str] = None,
        decimals: int = 2
    ):
        """
        TODO: HUH? this is the same description as gof but returns a contingency table?
        Calculate goodness of fit statistics using the spotpy package.
        See :py:fun:`calculate_all_functions() <calculate_all_functions>`
        in :py:mod:`spotpy`.
        Args:
            mod_col: Column name of modelled data
            obs_col: Column name of observed data
            group_by: Column names to group by prior to calculating statistics
            decimals: round stats to specified decimal places
            threshold: Threshold value for high flow event or
            column name containing threshold value.
            label: Label for threshold to use instead of valeu in returned
        dataframe
        Returns:
            Pandas dataframe containing contingency table
        """

        if group_by is None:
            event_stats = self._group_calc_event_stats(
                data=self.data,
                threshold=threshold,
                label=label,
                obs_col=obs_col,
                mod_col=mod_col,
                decimals=decimals)
        else:
            event_stats = self.data.set_index(group_by). \
                groupby(group_by). \
                apply(
                    self._group_calc_event_stats,
                    threshold=threshold,
                    label=label,
                    obs_col=obs_col,
                    mod_col=mod_col,
                    decimals=decimals)
            event_stats = event_stats.rename(
                columns={'level_2': 'statistic'})

        return event_stats


def calc_cont_table(actual: np.array, predicted: np.array) -> pd.DataFrame:
    """
    Calculate a contingency table from two arrays of hits/misses.
    In each input array, a hit is True and a miss is a False

                      observed
                 | True |  False  |
                 ------------------
            True |hits  |false    |
    forecast     |      |alarms   |
                 ------------------
           False |misses|correct  |
                 |      |negatives|
                 ------------------

        Args:
            actual: Array of actual hits/misses
            predicted: Array of predicted hits/misses
        Returns:
            Pandas dataframe containing contingency table
    """

    # Negating bools because pd.crosstab treats 0 as a hit and 1 as a miss
    cont_tbl = pd.crosstab(~predicted, ~actual)

    # Have to manually construct table because pandas will drop columns
    # on return table if all values are 0
    try:
        hits = cont_tbl.at[False, False]
    except:
        hits = 0
    try:
        false_alarms = cont_tbl.at[False, True]
    except:
        false_alarms = 0
    try:
        misses = cont_tbl.at[True, False]
    except:
        misses = 0
    try:
        correct_negatives = cont_tbl.at[True, True]
    except:
        correct_negatives = 0

    cont_tbl = pd.DataFrame({True: [hits, misses],
                             False: [false_alarms, correct_negatives]},
                            index=[True, False])

    return cont_tbl


def calc_cont_stats(
    cont_table: pd.DataFrame,
    inf_as_na: bool = True,
    decimals: int = 2
):
    """
    Calculate contigency statistics from a contingency table

                      observed
                 | True |  False  |
                 ------------------
            True |hits  |false    |
    forecast     |      |alarms   |
                 ------------------
           False |misses|correct  |
                 |      |negatives|
                 ------------------

    Accuracy (acc, fraction correct) = (hits + correct_negatives)/total
    Bias score (bias, frequency bias) = (hits + false_alarms)/(hits + misses)
    Probability of detection (pod, hit rate) = hits/(hits + misses)
    False alarm ratio (far) = false_alarms/(hits + false_alarms)
    Probability of false detection (pofd, false alarm rate) =
    false_alarms/(false_alarms + correct_negatives)
    Success ratio (sr) = hits/(hits + false_alarms)
    Critical success index (csi, threat score) =
    hits/(hits + misses + false_alarms)
    Gilbert skill score (gss, Equitable threat score) =
    (hits - hits_random)/(hits + misses +
    false_alarms - hits_random) where hits_random =
    ((hits + misses)*(hits + false_alarms))/total
    Hanssen and Kuipers discriminant (hk, Peirce's skill score) = (hits/(
    hits+misses)) - (false_alarms/(false_alarms + correct_negatives))
    Odds ratio (or) = (POD/(1-POD))/(POFD/(1-POFD))
    Odds ratio skill score (orss, Yule's Q) = ((hits * correct_negatives) -
    (misses * false_alarms))/((hits * correct_negatives) +
    (misses * false_alarms)


    Args:
        cont_table: A contingency table
        inf_as_na: convert inf values to na?
        decimals: round stats to specified decimal places
    Returns:
        A list of tuples of contingency statistics

    """

    total = cont_table.sum().sum()
    hits = cont_table.loc[True, True]
    misses = cont_table.loc[False, True]
    false_alarms = cont_table.loc[True, False]
    correct_neg = cont_table.loc[False, False]
    hits_random = ((hits + misses) * (hits + false_alarms)) / total
    pod = hits / (hits + misses)
    pofd = false_alarms / (false_alarms + correct_neg)

    cont_stats = [
        ('acc', (hits + correct_neg) / total),
        ('bias', (hits + false_alarms) / (hits + misses)),
        ('pod', pod),
        ('far', false_alarms / (hits + false_alarms)),
        ('pofd', pofd),
        ('sr', hits / (hits + false_alarms)),
        ('csi', hits / (hits + misses + false_alarms)),
        ('gss',
         (hits - hits_random) / (hits + misses + false_alarms - hits_random)),
        ('hk', (hits / (hits + misses)) - (
                false_alarms / (false_alarms + correct_neg))),
        ('or', (pod / (1 - pod)) / (pofd / (1 - pofd))),
        ('orss', ((hits * correct_neg) - (misses * false_alarms)) /
         ((hits * correct_neg) + (misses * false_alarms))),
        ('N', (hits + misses))
    ]

    cont_stats = pd.DataFrame(cont_stats).rename(
        columns={0: 'statistic', 1: 'value'})

    cont_stats['value'] = cont_stats['value'].round(decimals=decimals)
    if inf_as_na:
        cont_stats['value'].replace(np.inf, np.nan, inplace=True)
        cont_stats['value'].replace(-np.inf, np.nan, inplace=True)

    return cont_stats


# TODO: actual -> observed, predicted -> modeled ?
def calc_gof_stats(
    actual: np.array,
    predicted: np.array,
    inf_as_na: bool = True,
    decimals: int = 2
):
    """
    Calculate goodness of fit statistics using the spotpy package.
    See :py:fun:`calculate_all_functions() <calculate_all_functions>`
    in :py:mod:`spotpy`.
    Args:
        actual: Array of actual hits/misses
        predicted: Array of predicted hits/misses
        inf_as_na: convert inf values to na?
        decimals: round stats to specified decimal places
    Returns:
        Pandas dataframe containing GOF stats.
    """
    gof_stats = spo.calculate_all_functions(actual, predicted)
    gof_stats = pd.DataFrame(gof_stats).rename(
        columns={0: 'statistic', 1: 'value'})

    #if gof_stats['value'][7] != 1.000:
    #    fjfjfjf
    
    # Change the sign on bias since it is presented in spotpy as
    # obs-sim instead of typical sim-obs
    gof_stats.loc[gof_stats['statistic'] == 'bias', ['value']] = \
        gof_stats.loc[gof_stats['statistic'] == 'bias', ['value']] * -1

    # Screen out very large numbers so that dont have to move to
    # double precision
    gof_stats.loc[gof_stats['value'] > 1e10, 'value'] = 1e10
    gof_stats.loc[gof_stats['value'] < -1e10, 'value'] = -1e10

    if inf_as_na:
        gof_stats['value'].replace(np.inf, np.nan, inplace=True)
        gof_stats['value'].replace(-np.inf, np.nan, inplace=True)

    # Summarize actual
    # noinspection PyTypeChecker
    summary = pd.DataFrame(
        {'value': [actual.mean(), np.percentile(actual, 0.5), actual.std()],
         'statistic': ['mean_obs', 'median_obs', 'std_obs']})
    gof_stats = pd.concat([gof_stats, summary], ignore_index=True, sort=True)

    gof_stats['value'] = gof_stats['value'].round(decimals=decimals)

    return gof_stats


def spo_all_xr(observed, modeled, inf_as_na: bool = True, decimals: int = 2):
    #return (pd.DataFrame(spo.calculate_all_functions(observed, modeled)).to_xarray(),)
    return (calc_gof_stats(observed, modeled, inf_as_na, decimals).to_xarray(),)


# TODO: actual -> observed, predicted -> modeled ?
def calc_event_stats(
    actual: np.array,
    predicted: np.array,
    decimals: int = 2
):
    """
    TODO: describe these metrics better.
    Args:
        actual: Array of actual hits/misses TODO: does that make sense?
        predicted: Array of predicted hits/misses
        decimals: round stats to specified decimal places
    Returns:
        pd.DataFrame(
            {'statistic': ['event_freq_bias', 'event_dur_bias', 'N_obs_events'],
             'value': [event_freq_bias, event_dur_bias, num_act_events]}
        )
    """

    def run_length(arr):
        for event, group in itertools.groupby(arr):
            if event:
                yield len(list(group))

    pred_events = np.array(list(run_length(predicted)))
    act_events = np.array(list(run_length(actual)))

    num_pred_events = len(pred_events)
    num_act_events = len(act_events)

    event_freq_bias = np.nan
    event_dur_bias = np.nan
    if num_act_events > 0:
        event_freq_bias = num_pred_events / num_act_events

        avg_dur_pred = pred_events.sum() / num_pred_events
        avg_dur_act = act_events.sum() / num_act_events
        event_dur_bias = avg_dur_pred / avg_dur_act

    if decimals is not None:
        event_dur_bias = np.round(event_dur_bias, decimals=decimals)

    df = pd.DataFrame(
        {'statistic': ['event_freq_bias', 'event_dur_bias', 'N_obs_events'],
         'value': [event_freq_bias, event_dur_bias, num_act_events]})

    return df
