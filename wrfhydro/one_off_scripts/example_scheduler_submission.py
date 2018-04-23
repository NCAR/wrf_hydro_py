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


# #######################################################
# Check setting of job.nproc vs that of job.scheduler.nproc


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
len(run_object.chanobs)


# #######################################################
# Add two scheduled runs on cheyenne
job_args = get_job_args_from_specs(
    job_name='test_job',
    nnodes=1,
    nproc=72,
    mode='w',
    machine_spec_file=machine_spec_file,
    user_spec_file=user_spec_file,
    candidate_spec_file=candidate_spec_file
)

#pprint(job_args)
job_sched_ch = Job( **job_args )
#pprint(the_job.__dict__)
#pprint(the_job.scheduler.__dict__)
the_job.scheduler.nproc = 2
the_job.scheduler.wait_for_complete=True
the_job.scheduler.monitor_freq_s = 10
#pprint(the_job.scheduler.__dict__)
run_sched = WrfHydroRun(
    the_setup,
    "/glade/scratch/jamesmcc/test_sched",
    rm_existing_run_dir = True
)
run_sched.add_job(job_sched_ch)


# ######################################################
# A default docker Job
job_default = build_default_job()


