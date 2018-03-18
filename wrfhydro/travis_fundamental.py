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

domain_dir='/home/docker/domain/croton_lite'
candidate_dir='/home/docker/candidate'
reference_dir=candidate_dir
output_dir='test123'
#Setup a candidate model
candidate_model = WrfHydroModel(candidate_dir)

#Setup a reference model
reference_model = WrfHydroModel(reference_dir)

#Setup a domain
domain = WrfHydroDomain(domain_top_dir=domain_dir,
                           domain_config='NWM',
                           model_version=candidate_model.version)

#Setup a candidate simulation
candidate_sim = WrfHydroSim(candidate_model,domain)

#Setup a reference simulation
reference_sim = WrfHydroSim(reference_model,domain)

#Create a test class
testCase = FundamentalTest(candidate_sim,reference_sim,output_dir,overwrite=True)

#Run all tests
testCase.run_tests(output_file='test_out.pkl')

# Exit with testCase exit code
exit(testCase.exit_code)