import os
import pathlib

import pytest

from wrfhydropy.core.domain import Domain
from wrfhydropy.core.job import Job
from wrfhydropy.core.model import Model
from wrfhydropy.core.schedulers import PBSCheyenne
from wrfhydropy.core.simulation import Simulation, SimulationOutput
from wrfhydropy.core.ioutils import WrfHydroTs

@pytest.fixture()
def model(model_dir):
    model = Model(source_dir=model_dir,
                  model_config='nwm_ana')
    return model


@pytest.fixture()
def domain(domain_dir):
    domain = Domain(domain_top_dir=domain_dir,
                    domain_config='nwm_ana',
                    compatible_version='v5.1.0')
    return domain

@pytest.fixture()
def job():
    job = Job(job_id='test_job_1',
              model_start_time='1984-10-14',
              model_end_time='2017-01-04',
              restart=False,
              exe_cmd='bogus exe cmd',
              entry_cmd='bogus entry cmd',
              exit_cmd='bogus exit cmd')
    return job

@pytest.fixture()
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

def test_simulation_add_model_domain(model,domain):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)

    assert sim.base_hydro_namelist == \
           {'hydro_nlist': {'channel_option': 2,
                            'chanobs_domain': 0,
                            'chanrtswcrt': 1,
                            'chrtout_domain': 1,
                            'geo_static_flnm': './NWM/DOMAIN/geo_em.d01.nc',
                            'restart_file': './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
                            'aggfactrt': 4,
                            'udmp_opt': 1},
            'nudging_nlist': {'maxagepairsbiaspersist': 3,
                              'minnumpairsbiaspersist': 1,
                              'nudginglastobsfile':
                                  './NWM/RESTART/nudgingLastObs.2011-08-26_00:00:00.nc'}}

    assert sim.base_hrldas_namelist == \
           {'noahlsm_offline': {'btr_option': 1,
                                'canopy_stomatal_resistance_option': 1,
                                'hrldas_setup_file': './NWM/DOMAIN/wrfinput_d01.nc',
                                'restart_filename_requested':
                                    './NWM/RESTART/RESTART.2011082600_DOMAIN1',
                                'indir': './FORCING'},
            'wrf_hydro_offline': {'forc_typ': 1}}

def test_simulation_add_job(model,domain,job):
    sim = Simulation()
    with pytest.raises(Exception) as e_info:
        sim.add(job)

    sim.add(model)
    sim.add(domain)
    sim.add(job)

def test_simulation_compose(model,domain,job,capfd,tmpdir,domain_dir):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job)

    compose_dir = pathlib.Path(tmpdir).joinpath('sim_compose')
    os.mkdir(str(compose_dir))

    os.chdir(str(compose_dir))

    try:
        sim.compose()
        out, err = capfd.readouterr()
        print(out)
    except:
        out, err = capfd.readouterr()
        pass

    actual_files = list()
    for file in list(compose_dir.rglob('*')):
        actual_files.append(file.name)

    domain_files = domain.domain_top_dir.rglob('*')
    expected_files = ['namelist.hrldas','hydro.namelist','job_test_job_1','.uid','NWM']

    for file in domain_files:
        expected_files.append(file.name)

    for file in actual_files:
        assert file in expected_files

    assert out[-19:] == 'Compiling model...\n'



def test_simulation_run_no_scheduler(model,domain,job,capfd):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    sim.add(job)

    try:
        sim.run()
        out, err = capfd.readouterr()
    except:
        out, err = capfd.readouterr()
        pass
    assert err == '/bin/bash: bogus: command not found\n/bin/bash: bogus: command not found\n'


def test_simulation_collect(sim_output):

    os.chdir(sim_output)

    sim=Simulation()
    sim.collect()

    assert sim.output is not None
    assert type(sim.output.channel_rt) is WrfHydroTs


def test_simulation_output_checknans(sim_output):
    output=SimulationOutput()
    output.collect_output(sim_dir=sim_output)
    public_atts = [att for att in dir(output) if not att.startswith('__')]
    for att in public_atts:
        assert getattr(output,att) is not None

    assert output.check_output_nas() is not None

