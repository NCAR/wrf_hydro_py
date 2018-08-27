import deepdiff
import os
import pathlib

import pytest

from wrfhydropy.core.domain import Domain
from wrfhydropy.core.job import Job
from wrfhydropy.core.model import Model
from wrfhydropy.core.schedulers import PBSCheyenne
from wrfhydropy.core.simulation import Simulation
from wrfhydropy.core.ensemble import EnsembleSimulation
from wrfhydropy.core.ioutils import WrfHydroTs


@pytest.fixture()
def model(model_dir):
    model = Model(
        source_dir=model_dir,
        model_config='nwm_ana'
    )
    return model


@pytest.fixture()
def domain(domain_dir):
    domain = Domain(
        domain_top_dir=domain_dir,
        domain_config='nwm_ana',
        compatible_version='v5.1.0'
    )
    return domain


@pytest.fixture
def simulation(model, domain):
    sim = Simulation()
    sim.add(model)
    sim.add(domain)
    return sim

@pytest.fixture()
def job():
    job = Job(
        job_id='test_job_1',
        model_start_time='1984-10-14',
        model_end_time='2017-01-04',
        restart=False,
        exe_cmd='bogus exe cmd',
        entry_cmd='bogus entry cmd',
        exit_cmd='bogus exit cmd'
    )
    return job


@pytest.fixture()
def scheduler():
    scheduler = PBSCheyenne(
        account='fake_acct',
        email_who='elmo',
        email_when='abe',
        nproc=216,
        nnodes=6,
        ppn=None,
        queue='regular',
        walltime="12:00:00"
    )
    return scheduler


def test_ensemble_init_replicate(simulation):
    sim=simulation
    ens1 = EnsembleSimulation([sim])
    ens2 = EnsembleSimulation(sim)
    assert deepdiff.DeepDiff(ens1, ens2) == {}

    ens1.replicate_member(4)
    ens3 = EnsembleSimulation([sim, sim, sim, sim])
    assert deepdiff.DeepDiff(ens1, ens3) == {}

    assert len(ens1) == 4
    assert ens1.N == 4
    # How to assert an error?
    # assert ens1.replicate_member(4) == "WTF mate?"


def test_get_diff_dicts(simulation):
    sim=simulation
    ens = EnsembleSimulation([sim, sim, sim, sim])
    answer = {
        'number': ['000', '001', '002', '003'],
        'run_dir': ['member_000', 'member_001', 'member_002', 'member_003']
    }
    assert ens.diffs_dict == answer


def test_set_diff_dicts(simulation):
    sim=simulation
    ens = EnsembleSimulation([sim, sim, sim, sim, sim])
    ens.set_diffs_dict(('base_hrldas_namelist', 'noahlsm_offline', 'indir'), 
                       ['./FOO' if mm == 2 else './FORCING' for mm in range(len(ens))])
    answer = {
        ('base_hrldas_namelist', 'noahlsm_offline', 'indir'):
        ['./FORCING', './FORCING', './FOO', './FORCING', './FORCING'],
        'number': ['000', '001', '002', '003', '004'],
        'run_dir': ['member_000', 'member_001', 'member_002', 'member_003', 'member_004']
    }
    assert ens.diffs_dict == answer
