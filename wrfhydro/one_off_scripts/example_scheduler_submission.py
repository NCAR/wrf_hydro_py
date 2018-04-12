from wrfhydropy import *
import os, re
import sys
from shutil import rmtree
from pprint import pprint
home = os.path.expanduser("~/")
sys.path.insert(0, home + '/WRF_Hydro/wrf_hydro_tests/toolbox/')
from establish_specs import establish_spec
from establish_sched import get_sched_args_from_specs

#sys.path.insert(0, home + '/WRF_Hydro/wrf_hydro_py/wrfhydropy/core/')
#from scheduler import Scheduler

# Establish the scheduler
machine_spec_file= home +'/WRF_Hydro/wrf_hydro_tests/machine_spec.yaml'
candidate_spec_file= home + '/WRF_Hydro/wrf_hydro_tests/template_candidate_spec.yaml'
user_spec_file= home + '/WRF_Hydro/wrf_hydro_tests/template_user_spec.yaml'

sched_args = get_sched_args_from_specs(name='test_job',
                                       nnodes=1,
                                       run_dir='$PWD',
                                       machine_spec_file=machine_spec_file,
                                       user_spec_file=user_spec_file,
                                       candidate_spec_file=candidate_spec_file)

pprint(sched_args)
sched = Scheduler( **sched_args )
sched.run_dir = '/glade/scratch/jamesmcc/ex_sched_sub/'
sched.sched_name
sched.sched_version

# Establish the simulation
the_model = WrfHydroModel(os.path.expanduser('~/WRF_Hydro/wrf_hydro_nwm_public/trunk/NDHMS'))
the_model.compile("gfort")

the_domain = WrfHydroDomain(domain_top_dir='/glade/p/work/jamesmcc/DOMAINS/croton_NY',
                           model_version='v1.2.1',
                           domain_config='NWM')

the_sim = WrfHydroSim(the_model, the_domain)

# Put the scheduler in the run object.
rmtree(sched.run_dir)
run_object = the_sim.schedule_run( scheduler=sched,
                                   wait_for_complete=True,
                                   monitor_freq_s = 10 )

