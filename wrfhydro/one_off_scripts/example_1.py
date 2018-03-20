WRF_HYDRO_NWM_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_nwm_myFork
WRF_HYDRO_PY_PATH-/Users/${USER}/WRF_Hydro/wrf_hydro_py

docker create --name croton wrfhydro/domains:croton_NY
docker run -it \
    -v ${WRF_HYDRO_NWM_PATH}:/wrf_hydro_nwm \
    -v ${WRF_HYDRO_PY_PATH}:/home/docker/wrf_hydro_py \
    --volumes-from croton \
    wrfhydro/dev:conda

#######################################################
cp -r /wrf_hydro_nwm /home/docker/wrf_hydro_nwm
python

#######################################################
import sys
from pprint import pprint

# What does this line do?
sys.path.insert(0, '/home/docker/wrf_hydro_py/wrfhydro')

from wrf_hydro_model import *
from test_cases import *
from utilities import *

# ######################################################
# Model Section
# There are implied options here
# What is the argument? Are there more arguments?
theModel = WrfHydroModel('/home/docker/wrf_hydro_nwm/trunk/NDHMS')

# --- these are our concerns, dude ---

# The attributes of the model object.
# Note: menus of both compile options and namelists (run-time options) are now in the
# repository. These menus of namelists come in with the creation of the model
# object. Note that while the compile time options can be used in the compile method
# on the model object, the namelists are used only in simulation objects (where
# the model is actually run on a domain).

# 1. Compile options are not yet version/configed in the json namelist?
#    Are these even being used currently?
#    CLEARLY NOT, NUDGING NEEDS SET TO 1 for NWM. CHRIST ALMIGHTY.
pprint(theModel.compile_options)

# 2. The NWM configuration should be NWM-AnA (we may eventually have all 4 configurations in there).
# The configuration required for perfect restarts is actually slightly
# different than NWM in the nudging options, we may want to differentiate that.
pprint(theModel.hrldas_namelists)

pprint(theModel.hydro_namelists)

pprint(theModel.source_dir)
pprint(theModel.version)

# The only method on the model (it is independent of domain).
# 3. Should be able to pass version/configuration to the compile. Currently not args.
# What are other arguments here? Might just show help.
theModel.compile('gfort')
# The compilation results in the following new attributes/slots
## {'__slotnames__', 'compile_dir', 'compiler', 'wrf_hydro_exe', 'table_files', 'configure_log', 'object_id', 'compile_log'}
pprint(theModel.compiler)
pprint(theModel.compile_dir)
# Resulting binary
pprint(theModel.wrf_hydro_exe)
# The parameter table files which result from compiling.
pprint(theModel.table_files)
# Logs of config and compile
pprint(theModel.configure_log)
# pretty print not working so hot on the CompletedProcess object... 
pprint(theModel.compile_log)
# An object that needs some description......
pprint(theModel.object_id)

# ######################################################
# Domain Section
theDomain = WrfHydroDomain(domain_top_dir='/home/docker/domain/croton_NY',
                           model_version='v1.2.1',
                           domain_config='NWM')

# Note: The domain has no methods!
# Examine the attributes, skip the attributes set in the call.

# Each domain has 2 kinds of files which are actually independent of
# version+configuration: Forcing and nudging files.
pprint(theDomain.forcing_dir)
pprint(theDomain.nudging_files)

# The choice of domain+version+configuration specifices certain "patches" to the
# base namelists that were in the model object. Note that none of these are physics
# options, they are only domain-specific files, domain-specific times, and restart
# output frequencies.
# 4. forcing_timestep should probably NOT be in the NWM patches.
#    output_timestep  should probably NOT be in the NWM patches.
# The patches are held in/with the individual domains. The patch files is
# specified here
pprint(theDomain.namelist_patch_file)
# The patches are contained here
pprint(theDomain.namelist_patches)

# The specific hydro and lsm files found in the patches are listed in the following fields. 
# 5. What is the necessity of these versus just keeping them in the patche objects
pprint(theDomain.hydro_files)
# 6. What are these, why wrapped in a class?
#[WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/geo_em.nc'),
# WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/Fulldom.nc'),
# WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/HYDRO_TBL_2D.nc'),
# WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/geospatial_data_template_land_GIS.nc'),
# PosixPath('/home/docker/domain/croton_NY/NWM/RESTART/HYDRO_RST.2011082600_DOMAIN1'),
# WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/RouteLink.nc'),
# WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/LAKEPARM.nc'),
# WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/GWBUCKPARM.nc'),
# WrfHydroStatic('/home/docker/domain/croton_NY/NWM/DOMAIN/spatialweights.nc')]
pprint(theDomain.lsm_files)



# ######################################################
# Simulation Section
# simulation object = model object + domain object
# Note that CHANGING THE MODEL OR DOMAIN OBJECTS WILL CHANGE THE SIMULATION
# OBJECT ONLY BEFORE IT IS RUN. 
theSim = WrfHydroSim(theModel, theDomain)

pprint(theSim.hydro_namelist)
# 7. A bit awkward with the namelist*s* convention, but might have no other choice
# since we are stuck with the actual poor convention? namelist_hrldas vs hrldas_namelists.
pprint(theSim.namelist_hrldas)

# Edit an object in theDom
id1=theSim.model.object_id
# '3451646a-2cae-4b1f-9c38-bd8725e1c55f'
 
# Compile again and see the object_id in theSim.model change
theModel.compile('gfort', compile_options={'WRF_HYDRO_NUDGING':1})
theModel.object_id
theSim.model.object_id
# WAIT!!! HUH???????? theSim.model.object_id did NOT change.

##
theRun = theSim.run('/home/docker/testRun1', overwrite=True)
theRun.chanobs.open()
