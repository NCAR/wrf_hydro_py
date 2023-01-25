import json
import pathlib
import subprocess
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from wrfhydropy.core.domain import Domain
from wrfhydropy.core.job import Job
from wrfhydropy.core.model import Model
from wrfhydropy.core.schedulers import PBSCheyenne


@pytest.fixture(scope='function')
def ds_1d():
    # Create a dummy dataset
    vals_1d = np.random.randn(3)
    time = pd.to_datetime('1984-10-14')
    location = ['loc1', 'loc2', 'loc3']

    ds_1d = xr.Dataset({'var1': (('location'), vals_1d)},
                       {'Time': time, 'location': location})
    ds_1d.var1.encoding['_FillValue'] = False

    return ds_1d


@pytest.fixture(scope='function')
def ds_1d_has_nans():
    # Create a dummy dataset
    vals_1d = np.random.randn(3)
    time = pd.to_datetime('1984-10-14')
    location = ['loc1', 'loc2', 'loc3']

    ds_1d = xr.Dataset({'var1': (('location'), vals_1d)},
                       {'Time': time, 'location': location})

    return ds_1d


@pytest.fixture(scope='function')
def ds_2d():
    x = [10, 11, 12]
    y = [101, 102, 103]
    vals_2d = np.random.randn(3, 3)

    time = pd.to_datetime('1984-10-14')

    ds_2d = xr.Dataset(
        {'var1': (('x', 'y'), vals_2d)},
        {'Time': time, 'x': x, 'y': y})
    ds_2d.var1.encoding['_FillValue'] = False

    return ds_2d


@pytest.fixture(scope='function')
def ds_timeseries():
    # Create a dummy dataset
    vals_ts = np.random.randn(3, 3)
    time = pd.to_datetime(['1984-10-14 00:00:00', '1984-10-14 01:00:00', '1984-10-14 02:00:00'])
    location = ['loc1', 'loc2', 'loc3']

    ds_ts = xr.Dataset({'var1': (('location', 'Time'), vals_ts)},
                       {'Time': time,
                        'location': location})

    return ds_ts


@pytest.fixture(scope='function')
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

    json.dump(
        hrldas_namelist,
        domain_top_dir_path.joinpath('hrldas_namelist_patches.json').open('w')
    )

    json.dump(
        hydro_namelist,
        domain_top_dir_path.joinpath('hydro_namelist_patches.json').open('w')
    )

    return domain_top_dir_path


@pytest.fixture(scope='function')
def model_dir(tmpdir):

    model_dir_path = pathlib.Path(tmpdir).joinpath('wrf_hydro_nwm_public/src')
    model_dir_path.mkdir(parents=True)

    # Make namelist files
    hrldas_namelist = {
        "base": {
            "noahlsm_offline": {
                "btr_option": 1,
                "canopy_stomatal_resistance_option": 1,
                'restart_frequency_hours': 24,
                'output_timestep': 86400
            },
            "wrf_hydro_offline": {
                "forc_typ": "NULL_specified_in_domain.json"
            }
        },
        "nwm_ana": {
            "noahlsm_offline": {},
            "wrf_hydro_offline": {}
        }
    }

    hydro_namelist = {
        "base": {
            "hydro_nlist": {
                "channel_option": 2,
                "chanobs_domain": 0,
                "chanrtswcrt": 1,
                "chrtout_domain": 1,
                'rst_dt': 1440,
                'out_dt': 1440
            },
            "nudging_nlist": {
                "maxagepairsbiaspersist": 3,
                "minnumpairsbiaspersist": 1,
            }
        },

        "nwm_ana": {
            "hydro_nlist": {},
            "nudging_nlist": {}
        }
    }

    json.dump(
        hrldas_namelist,
        model_dir_path.joinpath('hrldas_namelists.json').open('w')
    )

    json.dump(
        hydro_namelist,
        model_dir_path.joinpath('hydro_namelists.json').open('w')
    )

    compile_options = {
        "nwm": {
            "WRF_HYDRO": 1,
            "HYDRO_D": 0,
            "SPATIAL_SOIL": 1,
            "WRF_HYDRO_RAPID": 0,
            "WRFIO_NCD_LARGE_FILE_SUPPORT": 1,
            "NCEP_WCOSS": 0,
            "WRF_HYDRO_NUDGING": 1
        }
    }

    json.dump(
        compile_options,
        model_dir_path.joinpath('compile_options.json').open('w')
    )

    with model_dir_path.joinpath('.version').open('w') as f:
        f.write('v5.1.0')

    with model_dir_path.joinpath('configure').open('w') as f:
        f.write('# dummy configure \n')

    # Arugments passed to wrf_hydro.exe are echoed to diag_hydro.00000.
    dummy_compile = (
        "#!/bin/bash \n"
        "# dummy compile \n"
        "mkdir Run \n"
        "echo '#!/bin/bash \n"
        "echo $@ > diag_hydro.00000\n"
        "echo \'The model finished successfully.......\' >>  diag_hydro.00000\n"
        "exit 0' > Run/wrf_hydro.exe\n"
        "touch Run/DUMMY.TBL \n"
    )
    with model_dir_path.joinpath('./compile_offline_NoahMP.sh').open('w') as f:
        f.write(dummy_compile)

    subprocess.run(['chmod', '-R', '755', str(model_dir_path)])

    return model_dir_path


@pytest.fixture(scope='function')
def compile_dir(tmpdir):
    compile_dir = pathlib.Path(tmpdir).joinpath('compile_dir')
    compile_dir.mkdir(parents=True)

    # Set table files and exe file attributes
    table_files = [compile_dir.joinpath('file1.tbl'), compile_dir.joinpath('file2.tbl')]
    wrf_hydro_exe = compile_dir.joinpath('wrf_hydro.exe')

    # Make fake run directory with files that would have been produced at compile
    with wrf_hydro_exe.open('w') as f:
        f.write('#dummy exe file')

    for file in table_files:
        with file.open('w') as f:
            f.write('#dummy table file')

    return compile_dir


@pytest.fixture(scope='function')
def model(model_dir):
    model = Model(
        source_dir=model_dir,
        model_config='nwm_ana'
    )
    return model


@pytest.fixture(scope='function')
def domain(domain_dir):
    domain = Domain(
        domain_top_dir=domain_dir,
        domain_config='nwm_ana',
        compatible_version='v5.1.0'
    )
    return domain


@pytest.fixture(scope='function')
# TODO: this should be changed to job_cold_start
def job():
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=False,
        exe_cmd='./wrf_hydro.exe',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )
    return job


@pytest.fixture(scope='function')
def job_restart():
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=True,
        restart_file_time='2013-10-13',
        exe_cmd='./wrf_hydro.exe',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )
    return job


@pytest.fixture(scope='function')
def scheduler():
    scheduler = PBSCheyenne(account='fake_acct',
                            email_who='elmo',
                            email_when='abe',
                            nproc=216,
                            nnodes=6,
                            ppn=None,
                            queue='regular',
                            walltime="12:00:00")
    return scheduler


@pytest.fixture(scope='function')
def sim_output(tmpdir, ds_1d, ds_1d_has_nans, ds_2d):

    tmpdir = pathlib.Path(tmpdir)
    sim_out_dir = tmpdir.joinpath('sim_out')

    sim_out_dir.mkdir(parents=True)

    # Make a list of DOMAIN filenames to create
    file_names = [
        'CHRTOUT_DOMAIN1_TEST',
        'CHRTOUT_GRID1_TEST'
        'CHANOBS_TEST',
        'LAKEOUT_TEST',
        'HYDRO_RST_TEST',
        'RESTART_TEST',
        'nudgingLastObs_TEST',
        '.RTOUT_',
        'LDASOUT'
    ]

    for counter in range(3):
        for file in file_names:
            filename = file + '_' + str(counter)
            file_path = sim_out_dir.joinpath(filename)
            ds_2d.to_netcdf(str(file_path))

    for counter in range(3):
        filename = 'GWOUT_' + str(counter)
        file_path = sim_out_dir.joinpath(filename)
        ds_1d_has_nans.to_netcdf(str(file_path))

    return sim_out_dir
