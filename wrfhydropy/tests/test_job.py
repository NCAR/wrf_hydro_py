import os
import pathlib

from pandas import Timestamp

from wrfhydropy.core.job import Job
from wrfhydropy.core.namelist import Namelist


def test_job_init():
    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=False,
              exe_cmd='bogus exe cmd',
              entry_cmd='bogus entry cmd',
              exit_cmd='bogus exit cmd')

    assert job.model_start_time == Timestamp('1984-10-14 00:00:00')
    assert job.model_end_time == Timestamp('2017-01-04 00:00:00')

    assert job.hydro_times == {'hydro_nlist': {'restart_file': None},
                               'nudging_nlist': {'nudginglastobsfile': None}}
    assert job.hrldas_times == {'noahlsm_offline': {'kday': 11770,
                                                    'khour': None,
                                                    'start_year': 1984,
                                                    'start_month': 10,
                                                    'start_day': 14,
                                                    'start_hour': 0,
                                                    'start_min': 0,
                                                    'restart_filename_requested': None}}

def test_job_hydro_namelist():
    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=False,
              exe_cmd='bogus exe cmd',
              entry_cmd='bogus entry cmd',
              exit_cmd='bogus exit cmd')

    hydro_namelist =  Namelist({'hydro_nlist': {
        "restart_file": None,
        "channel_option": 2
    },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })

    job._add_hydro_namelist(hydro_namelist)
    assert job.hydro_namelist == {'hydro_nlist': {'restart_file': None, 'channel_option': 2},
                                  'nudging_nlist': {'nudginglastobsfile': None}}


def test_job_hrldas_namelist():
    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=False,
              exe_cmd='bogus exe cmd',
              entry_cmd='bogus entry cmd',
              exit_cmd='bogus exit cmd')

    hrldas_namelist = Namelist({'noahlsm_offline':
        {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None
        }
    })

    job._add_hrldas_namelist(hrldas_namelist)
    assert job.hrldas_namelist == {'noahlsm_offline': {'btr_option': 1,
                                                       'kday': 11770,
                                                       'khour': None,
                                                       'start_year': 1984,
                                                       'start_month': 10,
                                                       'start_day': 14,
                                                       'start_hour': 0,
                                                       'start_min': 0,
                                                       'restart_filename_requested': None}}

def test_job_restart_file_times():
    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=True,
              exe_cmd='bogus exe cmd',
              entry_cmd='bogus entry cmd',
              exit_cmd='bogus exit cmd')

    hydro_namelist =  Namelist({'hydro_nlist': {
        "restart_file": None,
    },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })

    job._add_hydro_namelist(hydro_namelist)

    hrldas_namelist = Namelist({'noahlsm_offline':
        {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None
        }
    })

    job._add_hrldas_namelist(hrldas_namelist)

    assert job.hydro_namelist == {
        'hydro_nlist': {
            'restart_file': 'HYDRO_RST.1984-10-14_00:00_DOMAIN1'
        },
        'nudging_nlist': {
            'nudginglastobsfile': 'nudgingLastObs.1984-10-14_00:00:00.nc'
        }
    }

    assert job.hrldas_namelist == {'noahlsm_offline': {
        'btr_option': 1,
        'kday': 11770,
        'khour': None,
        'start_year': 1984,
        'start_month': 10,
        'start_day': 14,
        'start_hour': 0,
        'start_min': 0,
        'restart_filename_requested': './RESTART.1984101400_DOMAIN1'}
    }

def test_job_run_coldstart(tmpdir):

    os.chdir(tmpdir)

    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=False,
              exe_cmd='echo "bogus exe cmd"',
              entry_cmd='echo "bogus entry cmd"',
              exit_cmd='echo "bogus exit cmd"')

    hydro_namelist =  Namelist({'hydro_nlist': {
        "restart_file": None,
    },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })

    job._add_hydro_namelist(hydro_namelist)

    hrldas_namelist = Namelist({'noahlsm_offline':
        {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None
        }
    })

    job._add_hrldas_namelist(hrldas_namelist)

    job._make_job_dir()
    job._write_namelists()
    job._write_run_script()

    try:
        job._run()
    except:
        pass

    assert job.exit_status == 1
    assert job._proc_log.returncode == 0

    actual_files = list(job.job_dir.glob('*'))
    expected_files = [pathlib.Path('job_test_job_1/WrfHydroJob_prerun.pkl'),
                      pathlib.Path('job_test_job_1/WrfHydroJob_postrun.pkl'),
                      pathlib.Path('job_test_job_1/hydro.namelist'),
                      pathlib.Path('job_test_job_1/namelist.hrldas')]

    for file in actual_files:
        assert file in expected_files

