docker create --name croton wrfhydro/domains:croton_NY
docker run -it \
    -v /Users/james/WRF_Hydro/wrf_hydro_nwm_myFork:/wrf_hydro_nwm \
    -v /Users/james/WRF_Hydro/wrf_hydro_py/:/home/docker/wrf_hydro_py \
    --volumes-from croton \
    wrfhydro/dev:conda

#######################################################
cp -r /wrf_hydro_nwm /home/docker/wrf_hydro_nwm
python

#######################################################
import sys
sys.path.insert(0, '/home/docker/wrf_hydro_py/wrfhydro')

from wrf_hydro_model import *
from test_cases import *
from utilities import *

model = WrfHydroModel('/home/docker/wrf_hydro_nwm/trunk/NDHMS')

domain = WrfHydroDomain(domain_top_dir='/home/docker/domain/croton_NY',
                        domain_config='NWM',
                        model_version='v1.2.1')

wrfModel.compile('gfort')

wrfSim = WrfHydroSim(wrfModel,wrfDomain)

modelRun = wrfSim.run('/home/docker/testRun1',overwrite=True)
