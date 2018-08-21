import json
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr


@pytest.fixture()
def ds_1d():
    # Create a dummy dataset
    vals_1d = np.random.randn(3)
    time = pd.to_datetime('1984-10-14')
    location = ['loc1', 'loc2', 'loc3']

    ds_1d = xr.Dataset({'var1': (('location'), vals_1d)},
                    {'Time': time, 'location': location})

    return ds_1d


@pytest.fixture()
def ds_2d():
    x = [10,11,12]
    y = [101,102,103]
    vals_2d = np.random.randn(3,3)
    time = pd.to_datetime('1984-10-14')

    ds_2d = xr.Dataset({'var1': (('x','y'), vals_2d)},
                    {'Time': time, 'x': x,'y':y})

    return ds_2d


@pytest.fixture()
def ds_timeseries():
    # Create a dummy dataset
    vals_ts = np.random.randn(3,3)
    time = pd.to_datetime(['1984-10-14 00:00:00','1984-10-14 01:00:00','1984-10-14 02:00:00'])
    location = ['loc1', 'loc2', 'loc3']

    ds_ts = xr.Dataset({'var1': (('location','Time'), vals_ts)},
                    {'Time': time,
                     'location': location})

    return ds_ts

@pytest.fixture()
def domain_dir(tmpdir, ds_1d):
    domain_top_dir_path = pathlib.Path(tmpdir).joinpath('example_case')
    domain_dir_path = domain_top_dir_path.joinpath('NWM/DOMAIN')
    restart_dir_path = domain_top_dir_path.joinpath('NWM/RESTART')
    forcing_dir_path = domain_top_dir_path.joinpath('FORCING')

    domain_top_dir_path.mkdir(parents=True)
    domain_dir_path.mkdir(parents=True)
    restart_dir_path.mkdir(parents=True)
    forcing_dir_path.mkdir(parents=True)

    # Make a list of DOMAIN filenames to create
    domain_file_names = ['Fulldom_hires.nc',
                         'Route_Link.nc',
                         'soil_properties.nc',
                         'GEOGRID_LDASOUT_Spatial_Metadata.nc',
                         'geo_em.d01.nc',
                         'spatialweights.nc',
                         'GWBUCKPARM.nc',
                         'hydro2dtbl.nc',
                         'wrfinput_d01.nc',
                         'LAKEPARM.nc',
                         'nudgingParams.nc']

    for file in domain_file_names:
        file_path = domain_dir_path.joinpath(file)
        ds_1d.to_netcdf(str(file_path))

    # Make restart files
    restart_file_names = ['HYDRO_RST.2011-08-26_00:00_DOMAIN1',
                          'nudgingLastObs.2011-08-26_00:00:00.nc',
                          'RESTART.2011082600_DOMAIN1']

    for file in restart_file_names:
        file_path = restart_dir_path.joinpath(file)
        ds_1d.to_netcdf(str(file_path))

    # Make forcing files
    forcing_file_names = ['2011082600.LDASIN_DOMAIN1',
                          '2011082601.LDASIN_DOMAIN1',
                          '2011082602.LDASIN_DOMAIN1']

    for file in forcing_file_names:
        file_path = forcing_dir_path.joinpath(file)
        ds_1d.to_netcdf(str(file_path))

    # Make namelist patch files
    hrldas_namelist = {
        "base": {
            "noahlsm_offline": {
                "hrldas_setup_file": "./NWM/DOMAIN/wrfinput_d01.nc",
                "restart_filename_requested": "./NWM/RESTART/RESTART.2011082600_DOMAIN1",
                "indir": "./FORCING",
            },
            "wrf_hydro_offline": {
                "forc_typ": 1
            }
        },
        "nwm_ana": {
            "noahlsm_offline": {},
            "wrf_hydro_offline": {
                "forc_typ": 1
            }
        }
    }

    hydro_namelist = {
        "base": {
            "hydro_nlist": {
                "geo_static_flnm": "./NWM/DOMAIN/geo_em.d01.nc",
                "restart_file": "./NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1",
                "aggfactrt": 4,
                "udmp_opt": 1,
            },
            "nudging_nlist": {
                "nudginglastobsfile": "./NWM/RESTART/nudgingLastObs.2011-08-26_00:00:00.nc"
            }
        },

        "nwm_ana": {
            "hydro_nlist": {},
            "nudging_nlist": {}
        }
    }

    json.dump(hrldas_namelist,domain_top_dir_path.joinpath('hrldas_namelist_patches.json').open('w'))
    json.dump(hydro_namelist,domain_top_dir_path.joinpath('hydro_namelist_patches.json').open('w'))

    return domain_top_dir_path