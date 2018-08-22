import pathlib
import pytest

from wrfhydropy.core.schedulers import PBSCheyenne
from wrfhydropy.core.job import Job

@pytest.fixture()
def scheduler_regular():
    scheduler = PBSCheyenne(account='fake_acct',
                            email_who='elmo',
                            email_when='abe',
                            nproc=216,
                            nnodes=6,
                            ppn=None,
                            queue='regular',
                            walltime="12:00:00")
    return scheduler


@pytest.fixture()
def scheduler_shared():
    scheduler = PBSCheyenne(account='fake_acct',
                            email_who='elmo',
                            email_when='abe',
                            nproc=216,
                            nnodes=6,
                            ppn=None,
                            queue='shared',
                            walltime="12:00:00")
    return scheduler


def test_schedulers_pbs_regular_init(scheduler_regular):
    assert scheduler_regular._exe_cmd == 'mpiexec_mpt ./wrf_hydro.exe'


def test_schedulers_pbs_shared_init(scheduler_shared):
    assert scheduler_shared._exe_cmd == 'mpirun -np 216 ./wrf_hydro.exe'

def test_schedulers_pbs_solve_nodes(scheduler_regular):

    assert scheduler_regular.ppn == 36

    scheduler_regular.nproc = None
    scheduler_regular.nnodes = 5
    assert scheduler_regular.ppn == 36
    assert scheduler_regular.nnodes == 5
    assert scheduler_regular.nproc == 180

def test_schedulers_pbs_writescript(scheduler_regular):
    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=False,
              exe_cmd='bogus exe cmd',
              entry_cmd='bogus entry cmd',
              exit_cmd='bogus exit cmd')

    scheduler_regular._write_job_pbs([job,job])

    script_path = job.job_dir.joinpath('job_' + job.job_id + '.pbs')
    with script_path.open(mode='r') as f:
        job_script = f.read()

    expected_script = '#!/bin/sh\n' \
                      '#PBS -N test_job_1\n' \
                      '#PBS -A fake_acct\n' \
                      '#PBS -q regular\n' \
                      '#PBS -M elmo\n' \
                      '#PBS -m abe\n' \
                      '\n' \
                      '#PBS -l walltime=12:00:00\n' \
                      '\n' \
                      '#PBS -l select=6:ncpus=36:mpiprocs=36\n' \
                      '\n' \
                      '# Not using PBS standard error and out files to capture model output\n' \
                      '# but these files might catch output and errors from the scheduler.\n' \
                      '#PBS -o job_test_job_1\n' \
                      '#PBS -e job_test_job_1\n' \
                      '\n' \
                      '# CISL suggests users set TMPDIR when running batch jobs on Cheyenne.\n' \
                      'export TMPDIR=/glade/scratch/$USER/temp\n' \
                      'mkdir -p $TMPDIR\n' \
                      '\n' \
                      '/Volumes/d1/jmills/miniconda3/bin/python run_job.py --job_id test_job_1\n' \
                      'exit $?\n'

    # Only comparing the first 400 lines because the last lines vary according to system
    assert job_script[0:400] == expected_script[0:400]

def test_schedulers_pbs_schedule(scheduler_regular,capfd):
    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=False,
              exe_cmd='bogus exe cmd',
              entry_cmd='bogus entry cmd',
              exit_cmd='bogus exit cmd')

    try:
        scheduler_regular.schedule([job,job])
        out, err = capfd.readouterr()
        print(out)
    except:
        out, err = capfd.readouterr()
        pass
    assert out == '/bin/bash -c "job_test_job_1=`qsub -h job_test_job_1/job_test_job_1.pbs`;' \
                  'job_test_job_1=`qsub -W depend=afterok:$job_test_job_1 ' \
                  'job_test_job_1/job_test_job_1.pbs`;qrls $job_test_job_1;"\n'