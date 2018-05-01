# docker pull wrfhydro/domains:croton_NY
# docker pull wrfhydro/dev:conda

# docker create --name croton wrfhydro/domains:croton_NY
# # When done with the container: docker rm -v croton 

# export WRF_HYDRO_DIR=~/WRF_Hydro
# if [ $HOSTNAME == *"chimayo"* ]; then
#   export PUBLIC_DIR=/Volumes/d1/chimayoSpace/git_repos/wrf_hydro_nwm_public:/home/docker/wrf_hydro_nwm_public
# else
#   export PUBLIC_DIR=${WRF_HYDRO_DIR}/wrf_hydro_nwm_public:/wrf_hydro_nwm_public
# fi

# docker run -it \
#     -e WRF_HYDRO_TESTS_USER_SPEC='/home/docker/WRF_Hydro/wrf_hydro_tests/template_user_spec.yaml' \
#     -v ${WRF_HYDRO_DIR}:/home/docker/WRF_Hydro \
#     -v ${PUBLIC_DIR} \
#     --volumes-from croton \
#     wrfhydro/dev:conda

# # Inside docker
# if [ ! -e /home/docker/wrf_hydro_nwm_public ]; then
#   cp -r /wrf_hydro_nwm_public /home/docker/wrf_hydro_nwm_public
# fi
  
# cd ~/WRF_Hydro/wrf_hydro_py/
# pip uninstall -y wrfhydropy
# python setup.py develop
# pip install boltons termcolor
# python

from wrfhydropy import *

import copy
import datetime
import os
from pprint import pprint
import re
from socket import gethostname
import sys

home = os.path.expanduser("~/")
sys.path.insert(0, home + '/WRF_Hydro/wrf_hydro_tests/toolbox/')
from establish_specs import establish_spec
from establish_job import get_job_args_from_specs

machine = job_tools.get_machine()
if machine == 'cheyenne':
    model_path = '/glade/u/home/jamesmcc/WRF_Hydro/'
    domain_path = '/glade/p/work/jamesmcc/DOMAINS/croton_NY'
    run_dir = "/glade/scratch/jamesmcc/test_dir"
else:
    model_path = '/home/docker'
    domain_path = '/home/docker/domain/croton_NY'
    run_dir = "/home/docker/test_dir"


# Establish the setup
the_model = WrfHydroModel(
    os.path.expanduser(model_path + '/wrf_hydro_nwm_public/trunk/NDHMS'),
    'NWM'
)
# modules are an attribute of the model?!?!
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
# CHEYENNE
# Add two scheduled jobs on cheyenne

run_sched_dir = "/glade/scratch/jamesmcc/test_sched"
run_sched = WrfHydroRun(
    the_setup,
    run_sched_dir,
    rm_existing_run_dir=True
)

job_args = get_job_args_from_specs(
    job_name='test_job',
    nnodes=1,
    nproc=2,
    mode='w',
    machine_spec_file=machine_spec_file,
    user_spec_file=user_spec_file,
    candidate_spec_file=candidate_spec_file
)
job_template = Job( **job_args )
job_template.scheduler.walltime = '00:04:00'
job_template.scheduler.ppn = 1

time_0 = datetime.datetime(2011, 8, 26, 0, 0)
time_1 = time_0 + datetime.timedelta(days=2)
time_2 = time_1 + datetime.timedelta(days=5)

job_template.model_start_time = time_0
job_template.model_end_time = time_1
job_0 = copy.deepcopy(job_template)

job_template.model_start_time = time_1
job_template.model_end_time = time_2
job_1 = copy.deepcopy(job_template)

run_sched.add_jobs([job_0, job_1])

run_sched.run_jobs()

del run_sched
import pickle
with open(run_sched_dir + '/WrfHydroRun.pkl', 'rb') as f:
    r = pickle.load(f)
assert len(r.chanobs) == 168

sys.exit()


# #######################################################
# CHEYENNE: interactive run
run_interactive_dir = "/glade/scratch/jamesmcc/test_dir"
run_interactive = WrfHydroRun(
    the_setup,
    run_interactive_dir,
    rm_existing_run_dir = True
)

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
job_interactive_template = Job( **job_args )

time_0 = datetime.datetime(2011, 8, 26, 0, 0)
time_1 = time_0 + datetime.timedelta(days=2)
time_2 = time_1 + datetime.timedelta(days=5)

job_interactive_template.model_start_time = time_0
job_interactive_template.model_end_time = time_1
job_0 = copy.deepcopy(job_interactive_template)

job_interactive_template.model_start_time = time_1
job_interactive_template.model_end_time = time_2
job_1 = copy.deepcopy(job_interactive_template)

run_interactive.add_jobs([job_0, job_1])
run_interactive.run_jobs()

## Verify that the run occurred.
assert len(run_interactive.chanobs) == 168


# ######################################################
# DOCKER
# a default docker Job
# Try a default job

# run_interactive_2 = WrfHydroRun(
#     the_setup,
#     run_dir,
#     rm_existing_run_dir = True,
#     job=Job(nproc=2)
# )


# run_interactive_2.run_jobs()

# assert len(run_interactive_2.chanobs) == 168  

# ######

run_interactive_3 = WrfHydroRun(
    the_setup,
    run_dir,
    rm_existing_run_dir = True,
)


time_0 = datetime.datetime(2011, 8, 26, 0, 0)
time_1 = time_0 + datetime.timedelta(days=2)
time_2 = time_1 + datetime.timedelta(days=5)

j0 = Job(
    nproc=2,
    model_start_time=time_0,
    model_end_time=time_1
)

j1 = Job(
    nproc=2,
    model_start_time=time_1,
    model_end_time=time_2
)

run_interactive_3.add_job([j0,j1])
run_interactive_3.run_jobs()

assert len(run_interactive_3.chanobs) == 7*24

sys.exit()




# #######################################################
# Check setting of job.nproc vs that of job.scheduler.nproc



# #################################
# A bit less defaultish
run_interactive = WrfHydroRun(
    the_setup,
    run_dir,
    rm_existing_run_dir = True
)

job_args = get_job_args_from_specs(
    job_name='test_job',
    nproc=2,
    mode='w',
    machine_spec_file=machine_spec_file,
    user_spec_file=user_spec_file,
    candidate_spec_file=candidate_spec_file
)
job_interactive = Job( **job_args )
run_interactive.add_job(job_interactive)

assert len(run_interactive.chanobs) == 24  # croton_lite

sys.exit()






