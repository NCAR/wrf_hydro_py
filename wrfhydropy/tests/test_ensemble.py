import copy
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


def test_ensemble_init():
    ens = EnsembleSimulation()
    assert type(ens) is EnsembleSimulation
    # Not sure why this dosent vectorize well.
    atts = ['members', '_EnsembleSimulation__member_diffs', 'jobs', 'scheduler', 'ncores']
    for kk in ens.__dict__.keys():
        assert kk in atts


def test_ensemble_addsimulation(simulation, job, scheduler):
    sim = simulation
    ens1 = EnsembleSimulation()
    ens2 = EnsembleSimulation()
    ens1.add([sim])
    ens2.add(sim)
    assert deepdiff.DeepDiff(ens1, ens2) == {}

    # add a sim with job and make sure it is deleted.
    sim.add(job)
    sim.add(scheduler)
    ens1 = EnsembleSimulation()
    ens1.add(sim)
    assert all([len(mm.jobs) == 0 for mm in ens1.members])
    assert all([mm.scheduler is None for mm in ens1.members])


def test_ensemble_addjob(simulation, job):
    ens1 = EnsembleSimulation()
    ens1.add(job)
    assert deepdiff.DeepDiff(ens1.jobs[0], job) == {}

    job.job_id = 'a_different_id'
    ens1.add(job)
    assert deepdiff.DeepDiff(ens1.jobs[1], job) == {}


def test_ensemble_addscheduler(simulation, scheduler):
    ens1 = EnsembleSimulation()
    ens1.add(scheduler)
    assert deepdiff.DeepDiff(ens1.scheduler, scheduler) == {}

    sched2 = copy.deepcopy(scheduler)
    sched2.nnodes = 99
    ens1.add(sched2)
    assert deepdiff.DeepDiff(ens1.scheduler, sched2) == {}    


def test_ensemble_replicate(simulation):
    sim = simulation
    ens1 = EnsembleSimulation()
    ens2 = EnsembleSimulation()
    ens1.add(sim)
    ens1.replicate_member(4)
    ens2.add([sim, sim, sim, sim])
    assert deepdiff.DeepDiff(ens1, ens2) == {}


def test_ensemble_length(simulation):    
    sim = simulation
    ens1 = EnsembleSimulation()
    ens1.add(simulation)
    ens1.replicate_member(4)
    assert len(ens1) == 4
    assert ens1.N == 4
    # How to assert an error?
    # assert ens1.replicate_member(4) == "WTF mate?"


def test_get_diff_dicts(simulation):
    sim = simulation
    ens = EnsembleSimulation()
    ens.add([sim, sim, sim, sim])
    answer = {
        'number': ['000', '001', '002', '003'],
        'run_dir': ['member_000', 'member_001', 'member_002', 'member_003']
    }
    assert ens.member_diffs == answer


def test_set_diff_dicts(simulation):
    sim = simulation
    ens = EnsembleSimulation()
    ens.add([sim, sim, sim, sim, sim])
    ens.set_member_diffs(('base_hrldas_namelist', 'noahlsm_offline', 'indir'), 
                       ['./FOO' if mm == 2 else './FORCING' for mm in range(len(ens))])
    answer = {
        ('base_hrldas_namelist', 'noahlsm_offline', 'indir'):
        ['./FORCING', './FORCING', './FOO', './FORCING', './FORCING'],
        'number': ['000', '001', '002', '003', '004'],
        'run_dir': ['member_000', 'member_001', 'member_002', 'member_003', 'member_004']
    }
    assert ens.member_diffs == answer
