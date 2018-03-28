# WRF_HYDRO_NWM_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_nwm_myFork
# WRF_HYDRO_PY_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_py

# docker create --name croton wrfhydro/domains:croton_NY
# ## The complement when youre done with it:
# ## docker rm -v croton_NY

# docker run -it \
#     -v /Users/${USER}/Downloads:/Downloads \
#     -v ${WRF_HYDRO_NWM_PATH}:/wrf_hydro_nwm \
#     -v ${WRF_HYDRO_PY_PATH}:/home/docker/wrf_hydro_py \
#     --volumes-from croton \
#     wrfhydro/dev:conda

# #######################################################
# cp -r /wrf_hydro_nwm /home/docker/wrf_hydro_nwm
# python

#######################################################
import os
wrf_hydro_py_path = '/home/docker/wrf_hydro_py/wrfhydro'
USER = os.path.expanduser('~/')
wrf_hydro_py_path = USER + '/WRF_Hydro/wrf_hydro_py/wrfhydro'
wrf_hydro_py_path = USER + '/WRF_Hydro/wrf_hydro_py/wrfhydro'

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
domain_top_path = USER + '/Downloads/domain/croton_NY'
source_path = USER + '/WRF_Hydro/wrf_hydro_nwm_myFork/trunk/NDHMS'

theDomain = WrfHydroDomain(domain_top_dir=domain_top_path,
                           model_version='v1.2.1',
                           domain_config='NWM')

theModel = WrfHydroModel(source_path)

theSim = WrfHydroSim(theModel, theDomain)


# Regular lists or numpy ndarrays
#print('ensemble 0')
#e0=WrfHydroEnsembleSim([])
#print('len(e0): ',len(e0))
#e0.add_members(theSim)
#print('len(e0): ',len(e0))

print('ensemble 1')
e1=WrfHydroEnsembleSim([theSim])
print('len(e1): ',len(e1))
e1.members[0].description='the primal member'
print(e1.members_dict)
e1.replicate_member(4)
print('len(e1): ',len(e1))

print('e1.N: ',e1.N)
print(e1.members_dict)

e1.members[1].description='the first member'
print(e1.members_dict)

e1.members[1].number=400
print(e1.members_dict)


# Can I come up with a good visitor...
from boltons.iterutils import remap

def visit(path, key, value):

    print(path, key)

    if (not path):
        print('not path')
        if key == 'model':
            print('key is model')
            remap(value.__dict__, visit=visit)

    if key == 'forcing_dir':
        r.append(value)

    return False

r=[]
n = remap(m1.__dict__, visit=visit)

#e1.members

#print(e1.get_ens_attributes('hydro_namelist', 'nlastobs'))
#print(e1.get_ens_attributes('hydro_namelist', 'nlastobs'))



# Edit the forcing

# Edit the namelists




#theRun = theSim.run('/home/docker/testRun1', overwrite=True)
