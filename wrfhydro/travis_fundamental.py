#Import modules

from sys import argv
from wrf_hydro_model import *
from test_cases import *
from utilities import *

# Get domain, reference, and candidate from command line arguments
domain_dir = str(argv[1])
candidate_dir = str(argv[2])
reference_dir = str(argv[3])
output_dir = str(argv[4])

#Setup a candidate model
candidate_model = WrfHydroModel('/home/docker/wrf_hydro_nwm/trunk/NDHMS')

#Setup a reference model
reference_model = WrfHydroModel('/home/docker/wrf_hydro_nwm/trunk/NDHMS')

#Setup a domain
domain = WrfHydroDomain(domain_top_dir=domain_dir,
                           domain_config='NWM',
                           model_version=candidate_model.version)

#Setup a candidate simulation
candidate_sim = WrfHydroSim(candidate_model,domain)

#Setup a reference simulation
reference_sim = WrfHydroSim(reference_model,domain)

#Create a test class
testCase = FundamentalTest(candidate_sim,reference_sim,'/home/docker/test',overwrite=True)

#Run all tests
testCase.run_tests(output_file=output_dir)

# Exit with testCase exit code
exit(testCase.exit_code)