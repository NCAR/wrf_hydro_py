# docker pull wrfhydro/domains:croton_NY
# docker pull wrfhydro/dev

# docker create --name croton wrfhydro/domains:croton_NY
# # When done with the container: docker rm -v croton

# docker run -it \
#     -e WRF_HYDRO_TESTS_USER_SPEC='/home/docker/WRF_Hydro/wrf_hydro_tests/template_user_spec.yaml' \
#     -v /Users/jamesmcc/WRF_Hydro:/home/docker/WRF_Hydro \
#     -v /Volumes/d1/chimayoSpace/git_repos/wrf_hydro_nwm_public:/home/docker/wrf_hydro_nwm_public \
#     --volumes-from croton \
#     wrfhydro/dev:conda

# Inside docker
cd ~/WRF_Hydro/wrf_hydro_py/
pip uninstall -y wrfhydropy
python setup.py develop
pip install boltons termcolor
python


import copy
import os
from pprint import pprint
import re
from socket import gethostname
import sys
from wrfhydropy import *

home = os.path.expanduser("~/")
sys.path.insert(0, home + '/WRF_Hydro/wrf_hydro_tests/toolbox/')
from establish_specs import establish_spec
from establish_job import get_job_args_from_specs

host = gethostname()

if re.match('cheyenne', host):
    model_path = '/glade/u/home/jamesmcc/WRF_Hydro/'
    domain_path = '/glade/p/work/jamesmcc/DOMAINS/croton_NY'
    run_dir = "/home/docker/test_dir"
else:
    model_path = '/home/docker'
    domain_path = '/home/docker/domain/croton_lite'
    run_dir = "/glade/scratch/jamesmcc/test_dir"
    


# Establish the setup
the_model = WrfHydroModel(
    os.path.expanduser(model_path + '/wrf_hydro_nwm_public/trunk/NDHMS')
)
the_model.compile("gfort")

the_domain = WrfHydroDomain(
    domain_top_dir=domain_path,
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


# ######################################################
# A default docker Job

#job_default = build_default_job()

job_args = get_job_args_from_specs(
    job_name='test_job',
    nproc=2,
    mode='w',
    machine_spec_file=machine_spec_file,
    user_spec_file=user_spec_file,
    candidate_spec_file=candidate_spec_file
)
job_interactive = Job( **job_args )

run_interactive = WrfHydroRun(
    the_setup,
    run_dir,
    rm_existing_run_dir = True
)

run_interactive.add_job(job_interactive)

## Verify that the run occurred.
len(run_interactive.chanobs) #== 168


sys.exit()
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




