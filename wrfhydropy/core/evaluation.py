from typing import Union
import itertools
import numpy as np
import pandas as pd
import properscoring as ps
import spotpy.objectivefunctions as spo
import xarray as xr

# TODO: i think pd.DataFrame should be pd.Series everywhere.

# For collected data.
@xr.register_dataarray_accessor("eval")
# Better way to subclass?
class Evaluate:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        # Something to check that this is actually collected data
        # Something to classify the dimensions of the collected data:
        # simulation, ensemble, forecast, ensembleforecast

    def obs(
        self,
        observed: Union[pd.DataFrame, xr.DataArray],
        join_on: Union[list, str] = None,
        join_how: str = 'inner'
    ):
        modeled = self._obj.rename('modeled')
        observed = observed.rename('observed')
        return Evaluation(
            modeled=modeled.to_dataframe(),  # should remove, make an arg to_dataframe=True
            observed=observed.to_dataframe(),
            join_on=join_on,
            join_how=join_how
        )


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
            if isinstance(observed, pd.DataFrame):
                if observed.index.names is None:
                    if observed.index.nlevels > 1:
                        raise ValueError('You should add dataframe index level names')
                    observed.index.name = 'ind0'
                self.join_on = observed.index.names

            elif isinstance(observed, xr.DataArray):
                self.join_on = list(observed.dims)
            else:
                raise ValueError(
                    'Observed data neither pandas dataframe nor xarray.DataArray')
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
            ).reset_index()   # should we be resetting the index?

        elif isinstance(observed, xr.DataArray):
            data = xr.merge(
                [modeled, observed],
                join=join_how
            )

        else:
            raise ValueError('Observed data neither pandas dataframe nor xarray.DataArray')

        self.data = data
        """pd.Dataframe: The dataframe to analyze"""

    @staticmethod
    def _group_calc_cont_stats(
        data,
        threshold: Union[float, str],
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

        if isinstance(threshold, str):
            thresh_col = threshold
        else:
            thresh_col = '__thresh_col__'
            data[thresh_col] = threshold

        # I dont like that this is in a different place than for gof
        observed = data[obs_col]
        modeled = data[mod_col]
        thresh = data[thresh_col]
        nan_mask = np.isnan(observed) | np.isnan(modeled) | np.isnan(thresh)
        obs_masked = observed[~nan_mask]
        mod_masked = modeled[~nan_mask]
        thresh_masked = thresh[~nan_mask]
        if len(obs_masked) == 0:
            return pd.DataFrame()

        if time_window is None:
            obs_is_event = obs_masked > thresh_masked
            mod_is_event = mod_masked > thresh_masked
        else:
            raise ValueError('This is some highly experimental code, but Im leaving it there')
            data.set_index('time', inplace=True)
            rolling_df = data.rolling(window=time_window)
            obs_is_event = rolling_df[obs_col].max() > data[thresh_col]
            mod_is_event = rolling_df[mod_col].max() > data[thresh_col]
            data.reset_index('time', inplace=True)

        cont_table = calc_cont_table(obs_is_event, mod_is_event)
        cont_stats = calc_cont_stats(
            cont_table,
            inf_as_na=inf_as_na,
            decimals=decimals
        )

        cont_stats['value'] = cont_stats['value'].round(decimals=decimals)
        return cont_stats

    @staticmethod
    def _calc_gof_stats(
        data,
        obs_col,
        mod_col,
        inf_as_na,
        decimals
    ):
        # TODO: does this facilitate grouping?
        # This can be removed by changing the call to this function for the call made by this
        # function.
        return calc_gof_stats(
            data[obs_col],
            data[mod_col],
            inf_as_na=inf_as_na,
            decimals=decimals)

    @staticmethod
    def _group_calc_event_stats(
        data,
        threshold: Union[float, str],
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        decimals: int = 2
    ):

        if isinstance(threshold, str):
            thresh_col = threshold
        else:
            thresh_col = '__thresh_col__'
            data[thresh_col] = threshold

        obs_is_event = data[obs_col] > data[thresh_col]
        mod_is_event = data[mod_col] > data[thresh_col]

        event_stats = calc_event_stats(obs_is_event, mod_is_event, decimals=decimals)

        event_stats['value'] = event_stats['value'].round(decimals=decimals)
        return event_stats

    def contingency(
        self,
        threshold: Union[float, str],
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
            cont_stats = self.data.groupby(group_by). \
                apply(
                    self._group_calc_cont_stats,
                    threshold=threshold,
                    time_window=time_window,
                    mod_col=mod_col,
                    obs_col=obs_col,
                    inf_as_na=inf_as_na,
                    decimals=decimals)

        else:
            cont_stats = self._group_calc_cont_stats(
                data=self.data,
                threshold=threshold,
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
            if group_by is None:
                gof_stats = xr.apply_ufunc(
                    spo_all_xr,
                    self.data.observed.data,
                    self.data.modeled.data,
                    kwargs={
                        'inf_as_na': inf_as_na,
                        'decimals': decimals},
                    input_core_dims=[self.join_on, self.join_on]
                )
                gof_stats = gof_stats.value

            else:

                # how does this play out with more than 1 grouper?
                # Xarray
                obs_grp = self.data.observed.groupby(group_by)
                mod_grp = self.data.modeled.groupby(group_by)
                gof_stats = xr.apply_ufunc(
                    spo_all_xr,
                    obs_grp,
                    mod_grp,
                    kwargs={
                        'inf_as_na': inf_as_na,
                        'decimals': decimals},
                    input_core_dims=[self.join_on, self.join_on]
                )
                stats = [mv()._mapping for mv in gof_stats.values.tolist()]
                grp_ids = gof_stats.feature_id.values
                for grp_id, stat in zip(grp_ids, stats):
                    stat[group_by] = grp_id
                stats2 = [stat.expand_dims(group_by).set_coords(group_by) for stat in stats]
                stats3 = xr.merge(stats2)
                stats4 = stats3.to_dataframe().reset_index().set_index(['feature_id', 'index'])
                stats4.index.names = ['feature_id', '']
                gof_stats = stats4

        return gof_stats

    def crps(
        self,
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        member_col: str = 'member',
        valid_time_col: str = 'valid_time',
        lead_time_col: str = 'lead_time',
        gage_col: str = 'gage',
        weights=None
    ):
        """
        Calculate CRPS (continuous ranked probability score) using the properscoring package.
        See :py:fun:`crps_ensemble() <crps_ensemble>`
        in :py:mod:`properscoring`.

        Grouping is not necessary because CRPS returns a value per forecast.
        Grouping would happen when computing CRPSS.

        The Eval object generally wants one observation per modeled data point,
        that is overkill for this function (since the ensemble takes one observations)
        but we handle it in a consistent manner with the rest of Evaluation.

        This function is setup to identify the ensemble dimension in the following way:
            1. if "member_col" is present in the columns, then this is the ensemble dimension,
               which is a standard ensemble forecast way
            2. else, the "valid_time" dimension is used. This is the time-lagged ensembles way.
            3. NOT DONE: one could consider time-lagged ensembles of ensemble forecasts.

        Args:
            mod_col: str = 'modeled': Column name of modelled data
            obs_col: str = 'observed': Column name of observed data.
            member_col: str = 'member': Column name giving the members. If the column is present,
                evaluation is performed across the member dimension for each combination of
                other columns. If member is not present the valid_time lead_time and gage cols
                are used to calculate CRPS across lead-time for each valid_time, gage combination.
                This later option is the "timelagged" ensemble verification.
            valid_time_col: str = 'valid_time': I
            lead_time_col: str = 'lead_time',
            gage_col: str = 'gage',

        Returns:
            CRPS for each ensemble forecast against the observations.
        """
        # Grouping is not necessary because CRPS
        if isinstance(self.data, pd.DataFrame):
            # This is a bit hackish to get the indices columns
            indices = list(set(self.data.columns.tolist()) - set([mod_col, obs_col]))
            data = self.data.set_index(indices)
            modeled = data[mod_col]
            observed = data[obs_col]

            if valid_time_col in indices and member_col in indices:
                # Time-lagged ensemble WITH members
                mm = modeled.reset_index()
                mm = mm.set_index([valid_time_col, gage_col, member_col])
                mm = mm.pivot(columns=lead_time_col)
                mm = mm.unstack(level='member')
                mm = mm.sort_index()
                oo = observed.reset_index().set_index([valid_time_col, gage_col, member_col])
                inds_avg = list(set(indices) - set([lead_time_col, member_col]))
                oo = observed.mean(axis=0, level=inds_avg).to_frame()
                oo = oo.reset_index().set_index([valid_time_col, gage_col]).sort_index()
                assert mm.index.equals(oo.index)
                modeled = mm
                observed = oo[obs_col]

            elif valid_time_col in indices:
                # Time-lagged ensemble WITHOUT members
                # This may be a bit too in the business of the modeled data.
                mm = modeled.reset_index()
                drop_inds = list(set(indices) - set([valid_time_col, gage_col, lead_time_col]))
                mm = mm.drop(columns=drop_inds)
                mm = mm.set_index([valid_time_col, gage_col])
                # Expand lead times across the columns, across which the CRPS is calculated
                # for each valid time, gage
                mm = mm.pivot(columns=lead_time_col).sort_index()
                oo = observed.mean(axis=0, level=[valid_time_col, gage_col])
                oo = oo.reset_index().set_index([valid_time_col, gage_col]).sort_index()
                assert mm.index.equals(oo.index)
                modeled = mm
                observed = oo[obs_col]

            elif member_col in indices:
                # A "regular" member-only ensemble.
                inds_m_member = list(set(indices) - set([member_col]))
                # Expand the members across the columns - across which the CRPS is calculated
                # for each reference_time, lead_time, gage
                mm = modeled.unstack(level='member')
                mm = mm.reset_index().set_index(inds_m_member).sort_index()
                # Remove the member dimension from the obs. Could check the mean
                # matches the values.
                if isinstance(observed, pd.Series):
                    oo = observed.groupby(inds_m_member).mean()
                elif isinstance(observed, pd.DataFrame):
                    oo = observed.mean(axis=0, level=inds_m_member)
                else:
                    raise ValueError('observed not panda Series or DataFrame')
                oo = oo.reset_index().set_index(inds_m_member).sort_index()
                assert mm.index.equals(oo.index)
                modeled = mm
                observed = oo[obs_col]

            result_np = ps.crps_ensemble(
                observed.to_numpy(),
                modeled.to_numpy(),
                weights=weights)

            result_pd = pd.DataFrame(
                result_np,
                columns=['crps'],
                index=observed.index)

            return result_pd

        else:
            raise ValueError('Xarray not currently implemented for CRPS')

    def brier(
        self,
        threshold: float,
        mod_col: str = 'modeled',
        obs_col: str = 'observed',
        time_col: str = 'time',
        weights=None
    ):
        """
        Calculate Brier score using the properscoring package.
        See :py:fun:`threshold_brier_score() <threshold_brier_score>`
        in :py:mod:`properscoring`.
        Grouping is not necessary because BRIER returns a value per forecast.
        Grouping would happen when computing BRIERS.
        The Eval object generally wants one observation per modeled data point,
        that is overkill for this function but we handle it in a consistent manner
        with the rest of Evaluation.
        Args:
            mod_col: Column name of modelled data
            obs_col: Column name of observed data.
        Returns:
            BRIER for each ensemble forecast against the observations.
        """
        # Grouping is not necessary because BRIER
        if isinstance(self.data, pd.DataFrame):
            # This is a bit hackish to get the indices columns
            indices = list(set(self.data.columns.tolist()) - set([mod_col, obs_col]))
            data = self.data.set_index(indices)
            modeled = data[mod_col]
            observed = data[obs_col]

            modeled = modeled.unstack(level='time').to_numpy().transpose()
            if isinstance(observed, pd.Series):
                observed = observed.groupby('time').mean().to_numpy()
            elif isinstance(observed, pd.DataFrame):
                observed = observed.mean(axis=0, level='time').to_numpy()
            else:
                raise ValueError('observed not panda Series or DataFrame')
            result = ps.threshold_brier_score(observed, modeled, threshold=threshold)
            return result

        else:
            raise ValueError('Xarray not currently implemented for Brier score.')

    def event(
        self,
        threshold: Union[float, str],
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
        dataframe
        Returns:
            Pandas dataframe containing contingency table
        """

        if group_by is None:
            event_stats = self._group_calc_event_stats(
                data=self.data,
                threshold=threshold,
                obs_col=obs_col,
                mod_col=mod_col,
                decimals=decimals)
        else:
            event_stats = self.data.set_index(group_by). \
                groupby(group_by). \
                apply(
                    self._group_calc_event_stats,
                    threshold=threshold,
                    obs_col=obs_col,
                    mod_col=mod_col,
                    decimals=decimals)
            event_stats = event_stats.rename(
                columns={'level_2': 'statistic'})

        return event_stats


def calc_cont_table(observed: np.array, modeled: np.array) -> pd.DataFrame:
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
            observed: Array of observed hits/misses
            modeled: Array of modeled hits/misses
        Returns:
            Pandas dataframe containing contingency table
    """

    # Negating bools because pd.crosstab treats 0 as a hit and 1 as a miss
    cont_tbl = pd.crosstab(~modeled, ~observed)

    # Have to manually construct table because pandas will drop columns
    # on return table if all values are 0
    try:
        hits = cont_tbl.at[False, False]
    except KeyError:
        hits = 0

    try:
        false_alarms = cont_tbl.at[False, True]
    except KeyError:
        false_alarms = 0

    try:
        misses = cont_tbl.at[True, False]
    except KeyError:
        misses = 0

    try:
        correct_negatives = cont_tbl.at[True, True]
    except KeyError:
        correct_negatives = 0

    cont_tbl = pd.DataFrame(
        {True: [hits, misses],
         False: [false_alarms, correct_negatives]},
        index=[True, False])

    return cont_tbl


cont_stats_ideal = {
    'hits': np.nan,
    'misses': 0,
    'false_alarms': 0,
    'correct_neg': np.nan,
    'hits_random': np.nan,  # ?
    'acc': 1,
    'bias': 1,
    'pod': 1,
    'far': 0,
    'pofd': 0,
    'sr': 1,
    'csi': 1,
    'gss': 1,
    'hk': 1,
    'or': np.nan,
    'orss': 1,
    'sample_size': np.nan,
}


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
    modeled      |      |alarms   |
                 ------------------
           False |misses|correct  |
                 |      |negatives|
                 ------------------

    See https://www.cawcr.gov.au/projects/verification/ for descriptions.

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
    # Used in various stats...
    hits_random = ((hits + misses) * (hits + false_alarms)) / total
    pod = hits / (hits + misses)
    pofd = false_alarms / (false_alarms + correct_neg)

    cont_stats = [
        ('hits', hits),
        ('misses', misses),
        ('false_alarms', false_alarms),
        ('correct_neg', correct_neg),
        ('hits_random', hits_random),
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
        ('orss',
            ((hits * correct_neg) - (misses * false_alarms)) / (
                (hits * correct_neg) + (misses * false_alarms))),
        ('sample_size', total)
    ]

    cont_stats = pd.DataFrame(cont_stats).rename(
        columns={0: 'statistic', 1: 'value'})
    cont_stats = cont_stats.set_index('statistic')
    cont_stats['value'] = cont_stats['value'].round(decimals=decimals)
    if inf_as_na:
        cont_stats['value'].replace(np.inf, np.nan, inplace=True)
        cont_stats['value'].replace(-np.inf, np.nan, inplace=True)

    return cont_stats


gof_stats_ideal = {
    "agreementindex": 1,
    "bias": 0,
    "correlationcoefficient": 1,
    "covariance": np.nan,
    "decomposed_mse": 0,
    "kge": 1,
    "log_p": np.nan,
    "lognashsutcliffe": np.nan,
    "mae": 0,
    "mean_obs": np.nan,
    "median_obs": np.nan,
    "mse": np.nan,
    "nashsutcliffe": 1,
    "pbias": 0,
    "rmse": 0,
    "rrmse": 0,
    "rsquared": 1,
    "rsr": 0,
    "sample_size": np.nan,
    "std_obs": np.nan,
    "volume_error": 0
}


def calc_gof_stats(
    observed: np.array,
    modeled: np.array,
    inf_as_na: bool = True,
    decimals: int = 2
):
    """
    Calculate goodness of fit statistics using the spotpy package.
    See :py:fun:`calculate_all_functions() <calculate_all_functions>`
    in :py:mod:`spotpy`.
    Args:
        observed: Array of observed hits/misses
        modeled: Array of modeled hits/misses
        inf_as_na: convert inf values to na?
        decimals: round stats to specified decimal places
    Returns:
        Pandas dataframe containing GOF stats.
    """

    nan_mask = np.isnan(observed) | np.isnan(modeled)
    obs_masked = observed[~nan_mask]
    mod_masked = modeled[~nan_mask]
    if len(obs_masked) == 0:
        return pd.DataFrame()

    gof_stats = spo.calculate_all_functions(obs_masked, mod_masked)
    gof_stats = pd.DataFrame(gof_stats).rename(
        columns={0: 'statistic', 1: 'value'})

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

    # Summarize observed
    # noinspection PyTypeChecker
    summary = pd.DataFrame(
        {
            'value': [
                obs_masked.mean(),
                np.percentile(obs_masked, 0.5),
                obs_masked.std(),
                len(obs_masked)
            ],
            'statistic': [
                'mean_obs',
                'median_obs',
                'std_obs',
                'sample_size'
            ]
        }
    )
    gof_stats = pd.concat([gof_stats, summary], ignore_index=True, sort=True)
    gof_stats['value'] = gof_stats['value'].round(decimals=decimals)
    gof_stats = gof_stats.set_index('statistic')

    return gof_stats


def spo_all_xr(
    observed: np.array,
    modeled: np.array,
    inf_as_na: bool = True,
    decimals: int = 2
):
    result = calc_gof_stats(observed.ravel(), modeled.ravel(), inf_as_na, decimals).to_xarray()
    return result


def calc_event_stats(
    observed: np.array,
    modeled: np.array,
    decimals: int = 2
):
    """
    TODO: describe these metrics better.
    Args:
        observed: Array of observed hits/misses TODO: does that make sense?
        modeled: Array of modeled hits/misses
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

    pred_events = np.array(list(run_length(modeled)))
    act_events = np.array(list(run_length(observed)))

    num_pred_events = len(pred_events)
    num_act_events = len(act_events)

    event_freq_bias = np.nan
    event_dur_bias = np.nan
    avg_dur_act = np.nan
    avg_dur_pred = np.nan

    if num_act_events > 0:
        event_freq_bias = num_pred_events / num_act_events
        avg_dur_act = act_events.sum() / num_act_events

    if num_pred_events > 0:
        avg_dur_pred = pred_events.sum() / num_pred_events

    if not np.isnan(avg_dur_pred) and not np.isnan(avg_dur_act):
        event_dur_bias = avg_dur_pred / avg_dur_act

    if decimals is not None:
        event_dur_bias = np.round(event_dur_bias, decimals=decimals)

    df = pd.DataFrame(
        {'statistic': ['event_freq_bias', 'event_dur_bias', 'N_obs_events'],
         'value': [event_freq_bias, event_dur_bias, num_act_events]})
    df = df.set_index('statistic')

    return df
