import numpy as np
import pandas as pd
import pathlib
import wrfhydropy
import xarray as xr

wrf_hydro_py_dir = pathlib.Path(wrfhydropy.__file__).parent
thresh_file = wrf_hydro_py_dir / 'data/flood_thresholds.txt'

# -------------------------------------------------------
# Load the text file
thresh_df = pd.read_table(
    thresh_file,
    sep=' ',
    na_values='NA',
    dtype={'site_no': 'str'})

# Pretty up
# move "site_no" to "gage"
thresh_df = thresh_df.reset_index().rename(columns={'site_no': 'gage'}).drop(columns='index')


# -------------------------------------------------------
# QC
# 1. Duplicates feature_ids dropped.
dup_feats = thresh_df[thresh_df.duplicated(subset='feature_id')].feature_id.to_list()
# 3 duplicated features...
thresh_df[thresh_df['feature_id'].isin(dup_feats)].sort_values(by='feature_id')
# For now, just drop the duplicated. This serendipitiously selects the
# rows I would manually choose.
thresh_df = thresh_df.drop_duplicates(subset="feature_id")

# 2. No duplicated gages
dup_gages = thresh_df[thresh_df.duplicated(subset='gage')].gage.to_list()

# 3. There are 28 positive longitudes (31 before removing the 3 duplicated features)
(thresh_df[thresh_df['lon'] > 0])
thresh_df.loc[thresh_df['lon'] > 0, 'lon'] = -1 * abs(thresh_df.loc[thresh_df['lon'] > 0, 'lon'])
# Not removing... for now.

# 4. There are no negative latitudes
len(thresh_df[thresh_df['lat'] < 0])

# 5. Check the consistency of the various levels.
# Have basically found that "record" is a wild card... or at least I dont understand it.
ge_dict = {
    'minor': {'action'},
    'moderate': {'minor', 'action'},
    'major': {'moderate', 'minor', 'action'},
    # 'record': {'major', 'moderate', 'minor', 'action'}
}


def check_thresh_orders(row):
    errors = []
    for var in ['stage', 'flow']:
        for thresh, thresh_below in ge_dict.items():
            var_thresh = thresh + '_' + var
            for below in thresh_below:
                var_thresh_below = below + '_' + var
                if np.isnan(row[var_thresh_below]) or np.isnan(row[var_thresh]):
                    continue
                if row[var_thresh_below] > row[var_thresh]:
                    errors += [var_thresh_below + ' > ' + var_thresh]
    if errors == []:
        return(None)
    else:
        return(errors)


results = {}
for gage, row in thresh_df.iterrows():
    results[gage] = check_thresh_orders(row)

# remove the good=none results
results2 = {gage: result for gage, result in results.items() if result is not None}

# Only two gages with this contradiction
funky_gages = list(results2.keys())
funky_ones = thresh_df[thresh_df.index.isin(funky_gages)].sort_values(by='feature_id')

with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(funky_ones)

# Just set the conflicting thresholds to none by hand!
thresh_df.loc[thresh_df.gage == '07159750', 'action_stage'] = np.NaN
thresh_df.loc[thresh_df.gage == '11156500', 'action_flow'] = np.NaN

funky_ones = thresh_df[thresh_df.index.isin(funky_gages)].sort_values(by='feature_id')
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(funky_ones)

# QC Done

# -------------------------------------------------------
# Write it out

thresh_ds_write = thresh_df.set_index('gage').to_xarray()

# Convert cfs to cms
cfs_to_cms = 0.0280
thresh_flows = ['action_flow', 'minor_flow', 'moderate_flow', 'major_flow', 'record_flow']
for col in thresh_flows:
    thresh_ds_write[col] = thresh_ds_write[col] * cfs_to_cms
    thresh_ds_write[col].attrs['units'] = 'm^3/s'
    thresh_ds_write[col].encoding = {'dtype': 'float32'}

# Convert to m
ft_to_m = 0.3048
thresh_stages = ['action_stage', 'minor_stage', 'moderate_stage', 'major_stage', 'record_stage']
for col in thresh_stages:
    thresh_ds_write[col] = thresh_ds_write[col] * ft_to_m
    thresh_ds_write[col].attrs['units'] = 'meters'
    thresh_ds_write[col].encoding = {'dtype': 'float32'}

# Save this to a netcdf file.
thresh_nc_file = wrf_hydro_py_dir / 'data/flood_thresholds_metric_units.nc'
thresh_ds_write.to_netcdf(thresh_nc_file)
