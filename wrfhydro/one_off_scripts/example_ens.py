# WRF_HYDRO_NWM_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_nwm_myFork
# WRF_HYDRO_PY_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_py

# docker create --name croton wrfhydro/domains:croton_NY
# ## The complement when youre done with it:
# ## docker rm -v croton

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
import collections
import os
import pathlib
from pprint import pprint
import sys
import wrfhydropy
from wrfhydropy.core.ensemble_tools import DeepDiffEq, dictify

# ######################################################
# Model Section
# There are implied options here
# What is the argument? Are there more arguments?

#domain_top_path = '/home/docker/domain/croton_NY'
#source_path = '/home/docker/wrf_hydro_nwm/trunk/NDHMS'
USER = os.path.expanduser('~/')
domain_top_path = pathlib.PosixPath(USER + '/Downloads/croton_NY')
#domain_top_path = USER + '/domain/croton_NY'
source_path = pathlib.PosixPath(USER + '/WRF_Hydro/wrf_hydro_nwm_public/trunk/NDHMS')
#source_path = '/wrf_hydro_nwm/trunk/NDHMS/'

theDomain = wrfhydropy.WrfHydroDomain(
    domain_top_dir=domain_top_path,
    model_version='v1.2.1',
    domain_config='NWM'
)

theModel = wrfhydropy.WrfHydroModel(
    source_path,
    model_config='NWM'
)

theSetup = wrfhydropy.WrfHydroSetup(
    theModel,
    theDomain
)


#print('ensemble 0')
#e0=WrfHydroEnsembleSim([])
#print('len(e0): ',len(e0))
#e0.add_members(theSim)
#print('len(e0): ',len(e0))

print('ensemble 1')
e1=wrfhydropy.WrfHydroEnsembleSetup([theSetup])
print('len(e1): ',len(e1))
e1.members[0].description='the primal member'
print(e1.members_dict)

print('len(e1): ',len(e1))
print('e1.N: ',e1.N)
print(e1.members_dict)

e1.replicate_member(4)
e1.members[1].description='the first member'
print(e1.members_dict)

e1.members[1].number=400
print(e1.members_dict)

m2=e1.members[2]
m3=e1.members[3]

m2.domain.forcing_dir = \
    pathlib.PosixPath('/Users/jamesmcc/Downloads/domain/croton_NY/FORCING_FOO')
m2.model.source_dir = pathlib.PosixPath('foo')

md = e1.members_dict

{kk:collections.Counter(vv) for (kk,vv) in md.items()}


