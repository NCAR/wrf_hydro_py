#Import modules
import sys
sys.path.insert(0, '/home/docker/wrf_hydro_py/wrfhydro')

from wrf_hydro_model import *
from test_cases import *
from utilities import *

#Setup a domain
domain = WrfHydroDomain(domain_top_dir='/home/docker/domain/croton_lite',
                           domain_config='NWM',
                           model_version='v1.2.1')
#Setup a candidate model
candidate_model = WrfHydroModel('/home/docker/wrf_hydro_nwm_public/trunk/NDHMS')

#Setup a reference model
reference_model = WrfHydroModel('/home/docker/wrf_hydro_nwm_public/trunk/NDHMS')

#Setup a candidate simulation
candidate_sim = WrfHydroSim(candidate_model,domain)

#Setup a reference simulation
reference_sim = WrfHydroSim(reference_model,domain)

#Create a test class
testCase = FundamentalTest(candidate_sim,reference_sim,'/home/docker/test',overwrite=True)

#Ask questions
testCase.test_compile_candidate('gfort',overwrite=True,compile_options={'WRF_HYDRO_NUDGING': 1})
testCase.test_run_candidate()
testCase.test_ncores_candidate()
testCase.test_perfrestart_candidate()
testCase.test_compile_reference('gfort',overwrite=True,compile_options={'WRF_HYDRO_NUDGING': 0})
testCase.test_run_reference()
testCase.test_regression()
