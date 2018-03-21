# Import modules

import sys
from wrf_hydro_model import *
from wrfhydro_test_cases import *
from utilities import *

# Get domain, reference, and candidate from command line arguments
domain_dir = str(sys.argv[1])
candidate_dir = str(sys.argv[2])
reference_dir = str(sys.argv[3])
output_dir = str(sys.argv[4])

# Setup a candidate model
candidate_model = WrfHydroModel(candidate_dir)

# Setup a reference model
reference_model = WrfHydroModel(reference_dir)

# Setup a domain
domain = WrfHydroDomain(domain_top_dir=domain_dir,
                           domain_config='NWM',
                           model_version=candidate_model.version)

# Setup a candidate simulation
candidate_sim = WrfHydroSim(candidate_model,domain)

# Setup a reference simulation
reference_sim = WrfHydroSim(reference_model,domain)

# Create a test class
testCase = FundamentalTest(candidate_sim,reference_sim,output_dir,overwrite=True)

# Run all tests
testCase.run_tests(output_file='test_out.pkl')

# Exit with testCase sys.exit code
sys.exit(testCase.exit_code)