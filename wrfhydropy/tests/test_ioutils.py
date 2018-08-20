import pytest
import numpy as np
import pandas as pd
import xarray as xr
import pathlib
from wrfhydropy.core.ioutils import *

@pytest.fixture
def ds_timeseries(tmpdir):
    ts_dir = pathlib.Path(tmpdir).joinpath('timeseries_data')
    ts_dir.mkdir(parents=True)
    # Create a dummy dataset
    vals_ts = np.array([1.0,2.0,3.0],dtype='int')
    reference_times = pd.to_datetime(['1984-10-14 00:00:00',
                                      '1984-10-14 01:00:00',
                                      '1984-10-14 02:00:00'])
    times = pd.to_datetime(['1984-10-14 01:00:00',
                            '1984-10-14 02:00:00',
                            '1984-10-14 03:00:00'])
    location = ['loc1', 'loc2', 'loc3']

    for idx in enumerate(times):
        idx = idx[0]
        time_array = [times[idx]]
        ref_time_array = [reference_times[idx]]
        ds_ts = xr.Dataset({'var1': ('location', vals_ts)},
                           {'time': time_array,
                            'reference_time': ref_time_array,
                            'location': location})
        filename = 'timeseries_' + str(idx) + '.nc'
        ds_ts.to_netcdf(ts_dir.joinpath(filename))
    return ts_dir

def test_open_nwmdataset_no_forecast(ds_timeseries):
    ds_paths = list(ds_timeseries.rglob('*.nc'))
    the_ds = open_nwmdataset(paths = ds_paths,
                             chunks = None,
                             forecast = False)

    assert the_ds['reference_time'].values == np.array(['1970-01-01T00:00:00.000000000'],
                                                             dtype='datetime64[ns]')

    the_ds['time'].values.sort()
    assert np.all(the_ds['time'].values == np.array(['1984-10-14T01:00:00.000000000',
                                              '1984-10-14T02:00:00.000000000',
                                              '1984-10-14T03:00:00.000000000'],
                                                             dtype='datetime64[ns]'))

    assert np.all(the_ds['var1'].values == np.array([1.0,2.0,3.0], dtype='int'))

def test_open_nwmdataset_forecast(ds_timeseries):
    ds_paths = list(ds_timeseries.rglob('*.nc'))
    the_ds = open_nwmdataset(paths = ds_paths,
                             chunks = None,
                             forecast = True)

    the_ds['reference_time'].values.sort()
    assert np.all(the_ds['reference_time'].values == np.array(['1984-10-14T00:00:00.000000000',
                                              '1984-10-14T01:00:00.000000000',
                                              '1984-10-14T02:00:00.000000000'],
                                                             dtype='datetime64[ns]'))

    the_ds['time'].values.sort()
    assert np.all(the_ds['time'].values == np.array(['1984-10-14T01:00:00.000000000',
                                              '1984-10-14T02:00:00.000000000',
                                              '1984-10-14T03:00:00.000000000'],
                                                             dtype='datetime64[ns]'))
    # print(the_ds)
    # print(the_ds['var1'].values)
    # assert np.all(the_ds['var1'].values == np.array([[[1.0,2.0,3.0]]], dtype='int'))

def test_wrfhydrots(ds_timeseries):
    ts_obj = WrfHydroTs(list(ds_timeseries.rglob('*.nc')))

    ts_obj_open = ts_obj.open()

    assert type(ts_obj_open) == xr.core.dataset.Dataset

    # TODO add test for checkNAs
    #assert ts_obj.check_nas == None

def test_wrfhydrostatic(ds_timeseries):

    st_obj = WrfHydroStatic(list(ds_timeseries.rglob('*.nc'))[0])

    st_obj_open = st_obj.open()

    assert type(st_obj_open) == xr.core.dataset.Dataset

    # TODO add test for checkNAs
    #assert ts_obj.check_nas == None

