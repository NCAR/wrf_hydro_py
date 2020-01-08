# Create the data necessary for testing if netcdf files contain nans
# as distinct from the fill value.
# This creates the following files
#   nan_na_data/fill_value.nc
#   nan_na_data/nan_fill.nc
#   nan_na_data/nan_value.nc
#   nan_na_data/value_value.nc
# and runs the basic test to show that these are the right files for the job.
# These files could be created on the fly by the tests, but it's kinda "6-of-one".

import xarray as xr
import numpy as np

the_nan = float('nan')
the_fill = -9999.0
the_value = 0.0

all_combos = {
    'value_value': [the_value, the_value],
    'nan_value': [the_nan, the_value],
    'fill_value': [the_fill, the_value],
    'nan_fill': [the_nan, the_fill]
}

for name, value in all_combos.items():
    ds = xr.Dataset()
    da = xr.DataArray(
        np.array(value),
        coords=[np.array([0, 1])],
        dims='dim'
    )
    ds['some_var'] = da  # np.array(value)
    ds.encoding = {'_FillValue': the_fill}
    ds.reset_coords('some_var')
    the_file = 'nan_na_data/' + name + '.nc'
    ds.to_netcdf(the_file)
    # This is just an xarray based check.
    ds_in = xr.open_dataset(the_file, mask_and_scale=False)
    print('')
    print(name)
    print(ds_in)
