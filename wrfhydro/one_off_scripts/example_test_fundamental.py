#######################################################
# Before Docker
WRF_HYDRO_NWM_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_nwm_myFork
WRF_HYDRO_PY_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_py

docker create --name croton wrfhydro/domains:croton_NY
## The complement when youre done with it:
## docker rm -v sixmile_channel-only_test

docker run -it \
    -v ${WRF_HYDRO_NWM_PATH}:/wrf_hydro_nwm \
    -v ${WRF_HYDRO_PY_PATH}:/home/docker/wrf_hydro_py \
    --volumes-from croton \
    wrfhydro/dev:conda

#######################################################
# Inside docker (before python)
cp -r /wrf_hydro_nwm /home/docker/wrf_hydro_nwm
python


#######################################################
# Python inside docker

import sys
sys.path.insert(0, '/home/docker/wrf_hydro_py/wrfhydro')

from wrf_hydro_model import *
from test_cases import *
from utilities import *
from pprint import pprint

# Setup a domain
domain = WrfHydroDomain(domain_top_dir='/home/docker/domain/croton_NY',
                           domain_config='NWM',
                           model_version='v1.2.1')
# Setup a candidate model
candidate_model = WrfHydroModel('/home/docker/wrf_hydro_nwm/trunk/NDHMS')

# Setup a reference model
reference_model = WrfHydroModel('/home/docker/wrf_hydro_nwm/trunk/NDHMS')

# Setup a candidate simulation
candidate_sim = WrfHydroSim(candidate_model,domain)

# Setup a reference simulation
reference_sim = WrfHydroSim(reference_model,domain)

# Setup the test 
testCase = FundamentalTest(candidate_sim,reference_sim,'/home/docker/test',overwrite=True)

# Run the individual questions instead of just invoking run_test() 

testCase.test_compile_candidate('gfort', overwrite=True, compile_options={'WRF_HYDRO_NUDGING': 1})

testCase.test_run_candidate()

testCase.test_ncores_candidate()

testCase.test_perfrestart_candidate()
testCase.test_compile_reference('gfort',overwrite=True,compile_options={'WRF_HYDRO_NUDGING': 1})

testCase.test_run_reference()
testCase.test_regression()

print(testCase.results)

exit(testCase.exit_code)
