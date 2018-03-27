# WRF_HYDRO_NWM_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_nwm_myFork
# WRF_HYDRO_PY_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_py

# docker create --name croton wrfhydro/domains:croton_NY
# ## The complement when youre done with it:
# ## docker rm -v sixmile_channel-only_test

# docker run -it \
#     -v /Users/jamesmcc/Downloads:/Downloads \
#     -v ${WRF_HYDRO_NWM_PATH}:/wrf_hydro_nwm \
#     -v ${WRF_HYDRO_PY_PATH}:/home/docker/wrf_hydro_py \
#     --volumes-from croton \
#     wrfhydro/dev:conda

# #######################################################
# cp -r /wrf_hydro_nwm /home/docker/wrf_hydro_nwm
# python

#######################################################
wrf_hydro_py_path = '/home/docker/wrf_hydro_py/wrfhydro'
wrf_hydro_py_path = '/Users/jamesmcc/WRF_Hydro/wrf_hydro_py/wrfhydro'

import sys
from pprint import pprint
sys.path.insert(0, wrf_hydro_py_path)
from wrf_hydro_model import *
from utilities import *
from ensemble import *

# ######################################################
# Model Section
# There are implied options here
# What is the argument? Are there more arguments?

#domain_top_path = '/home/docker/domain/croton_NY'
#source_path = '/home/docker/wrf_hydro_nwm/trunk/NDHMS'
domain_top_path = '/Users/jamesmcc/Downloads/domain/croton_NY'
source_path = '/Users/jamesmcc/WRF_Hydro/wrf_hydro_nwm_myFork/trunk/NDHMS'

theDomain = WrfHydroDomain(domain_top_dir=domain_top_path,
                           model_version='v1.2.1',
                           domain_config='NWM')

theModel = WrfHydroModel(source_path)

theSim = WrfHydroSim(theModel, theDomain)


# Regular lists or numpy ndarrays
print('ensemble 0')
e0=WrfHydroEnsembleSim([])
print(len(e0))
e0.add_member(theSim)
print(len(e0))

print('ensemble 1')
e1=WrfHydroEnsembleSim([])
print(len(e1))
e1.add_member(theSim)
print(len(e1))
e1.replicate_member(4)
print(len(e1))
print(e1.N)

print(e1.get_ens_attributes('hydro_namelist', 'nlastobs'))
print(e1.get_ens_attributes('hydro_namelist', 'nlastobs'))


#theRun = theSim.run('/home/docker/testRun1', overwrite=True)
