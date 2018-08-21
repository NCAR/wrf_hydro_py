from wrfhydropy.core.simulation import Simulation
from wrfhydropy.core.model import Model
from wrfhydropy.core.domain import Domain
import pathlib

import pytest

model_dir = pathlib.Path('wrf_hydro_nwm_public/trunk/NDHMS')
domain_dir = pathlib.Path('example_case')

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
