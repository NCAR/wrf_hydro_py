import os
import pathlib

from pandas import Timestamp

from wrfhydropy.core.job import Job
from wrfhydropy.core.namelist import Namelist


def test_job_init():
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    assert job.model_start_time == Timestamp('1984-10-14 00:00:00')
    assert job.model_end_time == Timestamp('2017-01-04 00:00:00')

    # The restart/output frequencies are none since no namelist file is supplied.
    assert job.hydro_times == {
        'hydro_nlist': {
            'restart_file': None,
            'rst_dt': None,
            'out_dt': None
        },
        'nudging_nlist': {
            'nudginglastobsfile': None
        },
    }

    assert job.hrldas_times == {
        'noahlsm_offline': {
            'khour': 282480,
            'restart_frequency_hours': None,
            'output_timestep': None,
            'start_year': 1984,
            'start_month': 10,
            'start_day': 14,
            'start_hour': 0,
            'start_min': 0,
            'restart_filename_requested': None
        }
    }


def test_job_hydro_namelist():
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    hydro_namelist = Namelist({
        'hydro_nlist': {
            "restart_file": None,
            "channel_option": 2,
            "out_dt": 1260,
            "rst_dt": 1260
        },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })

    job._add_hydro_namelist(hydro_namelist)
    assert job.hydro_namelist == {
        'hydro_nlist': {
            'restart_file': None,
            'channel_option': 2,
            'rst_dt': 1260,
            'out_dt': 1260
        },
        'nudging_nlist': {
            'nudginglastobsfile': None
        }
    }


def test_job_hrldas_namelist():
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    hrldas_namelist = Namelist({
        'noahlsm_offline': {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None,
            'output_timestep': 75600,
            'restart_frequency_hours': 21
        }
    })

    job._add_hrldas_namelist(hrldas_namelist)
    assert job.hrldas_namelist == {
        'noahlsm_offline': {
            'btr_option': 1,
            'khour': 282480,
            'start_year': 1984,
            'start_month': 10,
            'start_day': 14,
            'start_hour': 0,
            'start_min': 0,
            'restart_filename_requested': None,
            'output_timestep': 75600,
            'restart_frequency_hours': 21
        }
    }


def test_job_restart_file_times():

    # Test adding namelists to a job.
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=True,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    hydro_namelist = Namelist({
        'hydro_nlist': {
            "restart_file": None,
            "out_dt": 1260,
            "rst_dt": 1260
        },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })

    answer_hydro_namelist = {
        'hydro_nlist': {
            'restart_file': 'HYDRO_RST.1984-10-14_00:00_DOMAIN1',
            'rst_dt': 1260,
            'out_dt': 1260
        },
        'nudging_nlist': {
            'nudginglastobsfile': 'nudgingLastObs.1984-10-14_00:00:00.nc'
        }
    }

    hrldas_namelist = Namelist({
        'noahlsm_offline': {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None,
            'output_timestep': 86400,
            'restart_frequency_hours': 21
        }
    })

    answer_hrldas_namelist = {
        'noahlsm_offline': {
            'btr_option': 1,
            'khour': 282480,
            'restart_frequency_hours': 21,
            'output_timestep': 86400,
            'start_year': 1984,
            'start_month': 10,
            'start_day': 14,
            'start_hour': 0,
            'start_min': 0,
            'restart_filename_requested': 'RESTART.1984101400_DOMAIN1'
        }
    }

    # Add the namelists.
    job._add_hydro_namelist(hydro_namelist)
    job._add_hrldas_namelist(hrldas_namelist)

    assert job.hydro_namelist == answer_hydro_namelist
    assert job.hrldas_namelist == answer_hrldas_namelist

    # Now do it with separate and different restart_file times.
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=True,
        restart_file_time={'hydro': '1999-12-31', 'hrldas': '2000-01-01'},
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    answer_hydro_namelist['hydro_nlist']['restart_file'] = \
        'HYDRO_RST.1999-12-31_00:00_DOMAIN1'
    answer_hydro_namelist['nudging_nlist']['nudginglastobsfile'] = \
        'nudgingLastObs.1999-12-31_00:00:00.nc'

    answer_hrldas_namelist['noahlsm_offline']['restart_filename_requested'] = \
        'RESTART.2000010100_DOMAIN1'

    job._add_hydro_namelist(hydro_namelist)
    job._add_hrldas_namelist(hrldas_namelist)

    assert job.hydro_namelist == answer_hydro_namelist
    assert job.hrldas_namelist == answer_hrldas_namelist


def test_job_run_coldstart(tmpdir):

    os.chdir(tmpdir)

    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=False,
        exe_cmd='echo "bogus exe cmd"',
        entry_cmd='echo "bogus entry cmd"',
        exit_cmd='echo "bogus exit cmd"'
    )

    hydro_namelist = Namelist({
        'hydro_nlist': {
            "restart_file": None,
            "out_dt": 1260,
            "rst_dt": 1260
        },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })

    job._add_hydro_namelist(hydro_namelist)

    hrldas_namelist = Namelist({
        'noahlsm_offline': {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None,
            'output_timestep': 75600,
            'restart_frequency_hours': 21
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
    expected_files = [
        pathlib.Path('job_test_job_1/WrfHydroJob_prerun.pkl'),
        pathlib.Path('job_test_job_1/WrfHydroJob_postrun.pkl'),
        pathlib.Path('job_test_job_1/hydro.namelist'),
        pathlib.Path('job_test_job_1/namelist.hrldas')
    ]

    for file in actual_files:
        assert file in expected_files


def test_job_output_restart_freqs():
    # Test that setting output_freq_hr and restart_freq_hr changes the
    # value specified in the namelist.
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        output_freq_hr=1,
        restart_freq_hr=1,
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    # Set the namelists
    hydro_namelist = Namelist({
        'hydro_nlist': {
            "restart_file": None,
            "channel_option": 2,
            "out_dt": 1260,
            "rst_dt": 1260
        },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })
    hrldas_namelist = Namelist({
        'noahlsm_offline': {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None,
            'output_timestep': 75600,
            'restart_frequency_hours': 21
        }
    })

    # Apply the namelists to the job
    job._add_hydro_namelist(hydro_namelist)
    job._add_hrldas_namelist(hrldas_namelist)

    # Check the results (should be 1 hour for both).
    assert job.hydro_namelist == {
        'hydro_nlist': {
            'restart_file': None,
            'channel_option': 2,
            'rst_dt': 60,
            'out_dt': 60
        },
        'nudging_nlist': {
            'nudginglastobsfile': None
        }
    }

    assert job.hrldas_namelist == {
        'noahlsm_offline': {
            'btr_option': 1,
            'khour': 282480,
            'restart_frequency_hours': 1,
            'output_timestep': 3600,
            'start_year': 1984,
            'start_month': 10,
            'start_day': 14,
            'start_hour': 0,
            'start_min': 0,
            'restart_filename_requested': None
        }
    }

    # Now check that NOT specifying output_freq_hr and restart_freq_hr
    # does NOT change what is in the namelist.
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        #output_freq_hr=1,
        #restart_freq_hr=1,
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    # Use the same namelists as before/above.

    # Apply the namelists to the job
    job._add_hydro_namelist(hydro_namelist)
    job._add_hrldas_namelist(hrldas_namelist)

    # Check the results (should be what's in the namelist for both).
    assert job.hydro_namelist == {
        'hydro_nlist': {
            'restart_file': None,
            'channel_option': 2,
            'rst_dt': 1260,
            'out_dt': 1260
        },
        'nudging_nlist': {
            'nudginglastobsfile': None
        }
    }

    assert job.hrldas_namelist == {
        'noahlsm_offline': {
            'btr_option': 1,
            'khour': 282480,
            'restart_frequency_hours': 21,
            'output_timestep': 75600,
            'start_year': 1984,
            'start_month': 10,
            'start_day': 14,
            'start_hour': 0,
            'start_min': 0,
            'restart_filename_requested': None
        }
    }

    # Note: not testing if neither the namelist nor the Job specify
    # the restart/output frequencies.

    # Test if the alternative keywords set the namelists.
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        output_freq_hr={'hydro': 2},
        restart_freq_hr={'hrldas': 4},
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    # Set the namelists
    hydro_namelist = Namelist({
        'hydro_nlist': {
            "restart_file": None,
            "channel_option": 2,
            "out_dt": 1260,
            "rst_dt": 1260
        },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })
    hrldas_namelist = Namelist({
        'noahlsm_offline': {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None,
            'output_timestep': 75600,
            'restart_frequency_hours': 21
        }
    })

    # Apply the namelists to the job
    job._add_hydro_namelist(hydro_namelist)
    job._add_hrldas_namelist(hrldas_namelist)

    # Check the results
    assert job.hydro_namelist == {
        'hydro_nlist': {
            'restart_file': None,
            'channel_option': 2,
            'rst_dt': 1260,
            'out_dt': 120  # 2 hours
        },
        'nudging_nlist': {
            'nudginglastobsfile': None
        }
    }

    assert job.hrldas_namelist == {
        'noahlsm_offline': {
            'btr_option': 1,
            'khour': 282480,
            'restart_frequency_hours': 4,  # 4 hours
            'output_timestep': 75600,
            'start_year': 1984,
            'start_month': 10,
            'start_day': 14,
            'start_hour': 0,
            'start_min': 0,
            'restart_filename_requested': None
        }
    }


def test_job_restart_negative():

    # Test that setting restart_freq_hr to negative sets the value
    # to -99999 for both hydro and hrldas
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        output_freq_hr=1,
        restart_freq_hr=-1,
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )

    # Set the namelists
    hydro_namelist = Namelist({
        'hydro_nlist': {
            "restart_file": None,
            "channel_option": 2,
            "out_dt": 1260,
            "rst_dt": 1260
        },
        "nudging_nlist": {
            "nudginglastobsfile": None
        }
    })
    hrldas_namelist = Namelist({
        'noahlsm_offline': {
            'btr_option': 1,
            'kday': 1,
            'khour': None,
            'start_year': 1900,
            'start_month': 1,
            'start_day': 1,
            'start_hour': 1,
            'start_min': 1,
            'restart_filename_requested': None,
            'output_timestep': 75600,
            'restart_frequency_hours': 21
        }
    })

    # Apply the namelists to the job
    job._add_hydro_namelist(hydro_namelist)
    job._add_hrldas_namelist(hrldas_namelist)

    # Check the results (should be 1 hour for both).
    assert job.hydro_namelist == {
        'hydro_nlist': {
            'restart_file': None,
            'channel_option': 2,
            'rst_dt': -99999,
            'out_dt': 60
        },
        'nudging_nlist': {
            'nudginglastobsfile': None
        }
    }

    assert job.hrldas_namelist == {
        'noahlsm_offline': {
            'btr_option': 1,
            'khour': 282480,
            'restart_frequency_hours': -99999,
            'output_timestep': 3600,
            'start_year': 1984,
            'start_month': 10,
            'start_day': 14,
            'start_hour': 0,
            'start_min': 0,
            'restart_filename_requested': None
        }
    }

