import json

import pytest

from wrfhydropy.core.ioutils import *
from wrfhydropy.core.namelist import JSONNamelist

@pytest.fixture
def ds_timeseries(tmpdir):
    ts_dir = pathlib.Path(tmpdir).joinpath('timeseries_data')
    ts_dir.mkdir(parents=True)

    # Create a dummy dataset
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        vals_ts = np.array([np.log(-1.0),2.0,3.0],dtype='float')

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
    assert type(ts_obj.check_nas()) == pd.DataFrame

def test_wrfhydrostatic(ds_timeseries):

    static_obj = WrfHydroStatic(list(ds_timeseries.rglob('*.nc'))[0])

    static_obj_open = static_obj.open()

    assert type(static_obj_open) == xr.core.dataset.Dataset
    assert type(static_obj.check_nas()) == pd.DataFrame

def test_check_input_files(domain_dir):
    hrldas_namelist = JSONNamelist(domain_dir.joinpath('hrldas_namelist_patches.json'))
    hrldas_namelist = hrldas_namelist.get_config('nwm_ana')
    hydro_namelist = JSONNamelist(domain_dir.joinpath('hydro_namelist_patches.json'))
    hydro_namelist = hydro_namelist.get_config('nwm_ana')

    input_file_check = check_input_files(hrldas_namelist=hrldas_namelist,
                                         hydro_namelist=hydro_namelist,
                                         sim_dir=domain_dir)
    assert input_file_check is None

    # Alter one file to cause a false in check_input_files
    hydro_namelist['hydro_nlist']['geo_static_flnm'] = 'no_such_file'

    with pytest.raises(ValueError) as excinfo:
        check_input_files(hrldas_namelist=hrldas_namelist,
                          hydro_namelist=hydro_namelist,
                          sim_dir=domain_dir)

    assert str(excinfo.value) == 'The namelist file geo_static_flnm = no_such_file does not exist'

