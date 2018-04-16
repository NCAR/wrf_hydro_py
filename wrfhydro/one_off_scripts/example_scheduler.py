from wrfhydropy import Scheduler
import os, re
import sys
from pprint import pprint
home = os.path.expanduser("~/")
sys.path.insert(0, home + '/WRF_Hydro/wrf_hydro_tests/toolbox/')
from establish_specs import establish_spec
from establish_sched import get_sched_args_from_specs

machine_spec_file= home +'/WRF_Hydro/wrf_hydro_tests/machine_spec.yaml'
candidate_spec_file= home + '/WRF_Hydro/wrf_hydro_tests/template_candidate_spec.yaml'
user_spec_file= home + '/WRF_Hydro/wrf_hydro_tests/template_user_spec.yaml'

sched_args = get_sched_args_from_specs(name='test_job',
                                       nnodes=1,
                                       nproc=72,
                                       run_dir='$PWD',
                                       machine_spec_file=machine_spec_file,
                                       user_spec_file=user_spec_file,
                                       candidate_spec_file=candidate_spec_file)


pprint(sched_args)

# Test: set nproc
sched_args['nnodes']=2
sched_args['nproc']=None
sched_args['ppn']=36
sched = Scheduler( **sched_args )
assert sched.nnodes == 2
assert sched.nproc  == 2*36
assert sched.ppn    == 36
assert sched.nproc_last_node == 0
assert re.findall('select=2:ncpus=36:mpiprocs=36', sched.string())

# Test: set nnodes
sched_args['nnodes']=None
sched_args['nproc']=72
sched_args['ppn']=36
sched = Scheduler( **sched_args )
assert sched.nnodes == 2
assert sched.nproc  == 72
assert sched.ppn    == 36
assert sched.nproc_last_node == 0
assert re.findall('select=2:ncpus=36:mpiprocs=36',sched.string())

# Test: set nnodes with remainder on last node
sched_args['nnodes']=None
sched_args['nproc']=71
sched_args['ppn']=36
sched = Scheduler( **sched_args )
assert sched.nnodes == 2
assert sched.nproc  == 71
assert sched.ppn    == 36
assert sched.nproc_last_node == 35
assert re.findall('select=1:ncpus=36:mpiprocs=36\+1:ncpus=35:mpiprocs=35',sched.string())

# Test: set nnodes evenly distributed ppn < ppn_max
sched_args['nnodes']=None
sched_args['nproc']=48
sched_args['ppn']=24
sched = Scheduler( **sched_args )
assert sched.nnodes == 2
assert sched.nproc  == 48
assert sched.ppn    == 24
assert sched.nproc_last_node == 0
assert re.findall('select=2:ncpus=24:mpiprocs=24',sched.string())

# Test: set nnodes distributed ppn < ppn_max and remainder on last node
sched_args['nnodes']=None
sched_args['nproc']=47
sched_args['ppn']=24
sched = Scheduler( **sched_args )
assert sched.nnodes == 2
assert sched.nproc  == 47
assert sched.ppn    == 24
assert sched.nproc_last_node == 23
assert re.findall('select=1:ncpus=24:mpiprocs=24\+1:ncpus=23:mpiprocs=23',sched.string())

# Test: set ppn
sched_args['nnodes']=2
sched_args['nproc']=72
sched_args['ppn']=None
sched = Scheduler( **sched_args )
assert sched.nnodes == 2
assert sched.nproc  == 72
assert sched.ppn    == 36
assert sched.nproc_last_node == 0
assert re.findall('select=2:ncpus=36:mpiprocs=36',sched.string())

# Test: set ppn with remainder on last node.
sched_args['nnodes']=2
sched_args['nproc']=71
sched_args['ppn']=None
sched = Scheduler( **sched_args )
assert sched.nnodes == 2
assert sched.nproc  == 71
assert sched.ppn    == 36
assert sched.nproc_last_node == 35
assert re.findall('select=1:ncpus=36:mpiprocs=36\+1:ncpus=35:mpiprocs=35',sched.string())

sched.script()
print(sched.string())

sched.stdout_exe
sched.submit()
sched.stdout_exe
