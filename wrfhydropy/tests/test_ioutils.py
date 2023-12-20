from bs4 import BeautifulSoup
import datetime
import numpy as np
import pandas as pd
import pathlib
import pytest
import re
import requests
import warnings
import xarray as xr

from wrfhydropy.core.ioutils import \
    open_wh_dataset, WrfHydroTs, WrfHydroStatic, check_input_files, nwm_forcing_to_ldasin

from wrfhydropy.core.namelist import JSONNamelist


@pytest.fixture(scope='function')
def ds_timeseries(tmpdir):
    ts_dir = pathlib.Path(tmpdir).joinpath('timeseries_data')
    ts_dir.mkdir(parents=True)

    # Create a dummy dataset
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        vals_ts = np.array([np.log(-1.0), 2.0, 3.0], dtype='float')

    reference_times = pd.to_datetime([
        '1984-10-14 00:00:00',
        '1984-10-14 01:00:00',
        '1984-10-14 02:00:00'
    ])
    times = pd.to_datetime([
        '1984-10-14 01:00:00',
        '1984-10-14 02:00:00',
        '1984-10-14 03:00:00'
    ])
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


def test_open_wh_dataset_no_forecast(ds_timeseries):
    ds_paths = sorted(ds_timeseries.rglob('*.nc'))
    the_ds = open_wh_dataset(
        paths=ds_paths,
        chunks=None,
        forecast=False
    )

    the_ref_times = np.array(
        ['1970-01-01T00:00:00.000000000'], dtype='datetime64[ns]')
    assert (the_ds['reference_time'].values == the_ref_times).all()

    the_ds['time'].values.sort()
    assert np.all(the_ds['time'].values == np.array(['1984-10-14T01:00:00.000000000',
                                                     '1984-10-14T02:00:00.000000000',
                                                     '1984-10-14T03:00:00.000000000'],
                                                    dtype='datetime64[ns]'))


def test_open_wh_dataset_forecast(ds_timeseries):
    ds_paths = list(ds_timeseries.rglob('*.nc'))
    the_ds = open_wh_dataset(
        paths=ds_paths,
        chunks=None,
        forecast=True
    )

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
    assert type(ts_obj.check_nans()) == dict


def test_wrfhydrostatic(ds_timeseries):

    static_obj = WrfHydroStatic(list(ds_timeseries.rglob('*.nc'))[0])

    static_obj_open = static_obj.open()

    assert type(static_obj_open) == xr.core.dataset.Dataset
    assert type(static_obj.check_nans()) == dict


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


def test_nwm_forcing_to_ldasin(tmpdir):
    tmpdir = pathlib.Path(tmpdir)

    def url_index_anchor_regex(url, regex=''):
        page = requests.get(url).text
        soup = BeautifulSoup(page, 'html.parser')
        anchors = [url + '/' + node.get('href') for
                   node in soup.find_all('a') if re.search(regex, node.get('href'))]
        return anchors

    nwm_yesterday = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1))
    nwm_yesterday = nwm_yesterday.strftime("nwm.%Y%m%d")
    prod_url = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/' + nwm_yesterday
    para_url = 'http://para.nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/para/' + nwm_yesterday
    version_dict = {
        # 'para': para_url,
        'prod': prod_url}

    for version_name, model_version in version_dict.items():

        forcing_dirs = url_index_anchor_regex(model_version, r'^forcing_analysis_assim/$')
        for forcing_range in forcing_dirs:

            forcing_files = url_index_anchor_regex(forcing_range, r'\.nc$')
            for file in forcing_files:
                the_split = file.split('/')
                the_base = '/'.join(file.split('/')[(the_split.index(version_name)+1):])
                the_file = tmpdir.joinpath(version_name).joinpath(the_base)
                the_file.parent.mkdir(exist_ok=True, parents=True)
                the_file.touch()

            # The argument to nwm_forcing_dir is a list of "nwm.YYYYMMDD" dirs.
            ldasin_dir_list = tmpdir.joinpath(
                'ldasin_' + version_name + '_from_list/' + pathlib.Path(forcing_range).name
            )
            ldasin_dir_list.mkdir(parents=True)
            nwm_forcing_to_ldasin(
                nwm_forcing_dir=[tmpdir.joinpath(version_name).joinpath(nwm_yesterday)],
                ldasin_dir=ldasin_dir_list,
                range=pathlib.Path(forcing_range).name
            )
            ldasin_list_files = sorted(ldasin_dir_list.glob('*/*'))
            assert len(ldasin_list_files) == len(forcing_files)

            # The argument to nwm_forcing_dir is a path which contains "nwm.YYYYMMDD" dirs.
            ldasin_dir = tmpdir.joinpath(
                'ldasin_' + version_name + '/' + pathlib.Path(forcing_range).name
            )
            ldasin_dir.mkdir(parents=True)
            nwm_forcing_to_ldasin(
                nwm_forcing_dir=tmpdir.joinpath(version_name),
                ldasin_dir=ldasin_dir,
                range=pathlib.Path(forcing_range).name
            )
            ldasin_files = sorted(ldasin_dir.glob('*/*'))
            assert len(ldasin_files) == len(forcing_files)
