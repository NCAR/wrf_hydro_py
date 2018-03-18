import sys
sys.path.insert(0, '/home/docker/wrf_hydro_py/wrfhydro')

from wrf_hydro_model import *
from test_cases import *
from utilities import *

wrfModel = WrfHydroModel('/home/docker/wrf_hydro_nwm/trunk/NDHMS')
wrfDomain = WrfHydroDomain(domain_top_dir='/home/docker/domain/croton_NY',
                          domain_config='NWM',
                           model_version='v1.2.1')

wrfModel.compile('gfort')

wrfSim = WrfHydroSim(wrfModel,wrfDomain)

modelRun = wrfSim.run('/home/docker/testRun1',overwrite=True)