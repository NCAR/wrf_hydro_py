import copy
import os
from pprint import pprint
import sys
from wrfhydropy import *

home = os.path.expanduser("~/")
sys.path.insert(0, home + '/WRF_Hydro/wrf_hydro_tests/toolbox/')
from establish_specs import establish_spec
from establish_job import get_job_args_from_specs


# Establish the setup
the_model = WrfHydroModel(
    os.path.expanduser('~/WRF_Hydro/wrf_hydro_nwm_public/trunk/NDHMS')
)
the_model.compile("gfort")

the_domain = WrfHydroDomain(
    domain_top_dir='/glade/p/work/jamesmcc/DOMAINS/croton_NY',
    model_version='v1.2.1',
    domain_config='NWM'
)

the_setup = WrfHydroSetup(
    the_model,
    the_domain
)

# #######################################################
# Use these to build Jobs
machine_spec_file = home +'/WRF_Hydro/wrf_hydro_tests/machine_spec.yaml'
candidate_spec_file = home + '/WRF_Hydro/wrf_hydro_tests/template_candidate_spec.yaml'
user_spec_file = home + '/WRF_Hydro/wrf_hydro_tests/template_user_spec.yaml'


# ######################################################
# ######################################################
# ######################################################


# #######################################################
# Check setting of job.nproc vs that of job.scheduler.nproc



# #######################################################
# Add two scheduled runs on cheyenne
job_args = get_job_args_from_specs(
    job_name='test_job',
    nnodes=1,
    nproc=2,
    mode='w',
    machine_spec_file=machine_spec_file,
    user_spec_file=user_spec_file,
    candidate_spec_file=candidate_spec_file
)

job_sched = Job( **job_args )
job_sched.scheduler.walltime = '00:01:00'

run_sched_dir = "/glade/scratch/jamesmcc/test_sched"
run_sched = WrfHydroRun(
    the_setup,
    run_sched_dir,
    rm_existing_run_dir=True
)

run_sched.add_job(job_sched)

run_sched = None
import pickle
with open(run_sched_dir + '/WrfHydroRun.pkl', 'rb') as f:
    r = pickle.load(f)


sys.exit()




# #######################################################
# An interactive run on cheyenne
job_args = get_job_args_from_specs(
    job_name='test_job',
    nnodes=1,
    nproc=2,
    mode='w',
    scheduler_name = None, # Choice disables PBS
    machine_spec_file=machine_spec_file,
    user_spec_file=user_spec_file,
    candidate_spec_file=candidate_spec_file
)
job_interactive = Job( **job_args )

run_interactive = WrfHydroRun(
    the_setup,
    "/glade/scratch/jamesmcc/test_dir",
    rm_existing_run_dir = True
)

run_interactive.add_job(job_interactive)

## Verify that the run occurred.
assert len(run_interactive.chanobs) == 168




# ######################################################
# A default docker Job
job_default = build_default_job()


