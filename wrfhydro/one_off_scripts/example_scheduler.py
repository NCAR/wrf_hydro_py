from wrfhydropy import Job, Scheduler
import os, re
import sys
from pprint import pprint
home = os.path.expanduser("~/")
sys.path.insert(0, home + '/WRF_Hydro/wrf_hydro_tests/toolbox/')
from establish_specs import establish_spec
from establish_job import get_job_args_from_specs

# Establish a job
# A job has a scheduler. Create a scheduler first.

machine_spec_file = home + '/WRF_Hydro/wrf_hydro_tests/machine_spec.yaml'
candidate_spec_file = home + '/WRF_Hydro/wrf_hydro_tests/template_candidate_spec.yaml'
user_spec_file = home + '/WRF_Hydro/wrf_hydro_tests/template_user_spec.yaml'


job_args = get_job_args_from_specs(
    job_name='test_job',
    nnodes=1,
    nproc=72,
    mode='w',
    machine_spec_file=machine_spec_file,
    user_spec_file=user_spec_file,
    candidate_spec_file=candidate_spec_file
)
# pprint(job_args)

# Test: set nproc
job_args['scheduler']['nnodes']=2
job_args['scheduler']['nproc']=None
job_args['scheduler']['ppn']=36
#sched = Scheduler( **job_args['scheduler'] )
job = Job( **job_args )

pprint(job)
print('-------------------------------------------------------')
pprint(job.__dict__)
print('-------------------------------------------------------')
pprint(job.scheduler.__dict__)
sys.exit()

print(job.nproc)


assert job.scheduler.nnodes == 2
assert job.nproc  == 2*36
assert job.ppn    == 36
assert job.nproc_last_node == 0
assert re.findall('select=2:ncpus=36:mpiprocs=36', job.string())


sys.exit()


# Test: set nnodes
job_args['nnodes']=None
job_args['nproc']=72
job_args['ppn']=36
job = Jobuler( **job_args )
assert job.nnodes == 2
assert job.nproc  == 72
assert job.ppn    == 36
assert job.nproc_last_node == 0
assert re.findall('select=2:ncpus=36:mpiprocs=36',job.string())

# Test: set nnodes with remainder on last node
job_args['nnodes']=None
job_args['nproc']=71
job_args['ppn']=36
job = Jobuler( **job_args )
assert job.nnodes == 2
assert job.nproc  == 71
assert job.ppn    == 36
assert job.nproc_last_node == 35
assert re.findall('select=1:ncpus=36:mpiprocs=36\+1:ncpus=35:mpiprocs=35',job.string())

# Test: set nnodes evenly distributed ppn < ppn_max
job_args['nnodes']=None
job_args['nproc']=48
job_args['ppn']=24
job = Jobuler( **job_args )
assert job.nnodes == 2
assert job.nproc  == 48
assert job.ppn    == 24
assert job.nproc_last_node == 0
assert re.findall('select=2:ncpus=24:mpiprocs=24',job.string())

# Test: set nnodes distributed ppn < ppn_max and remainder on last node
job_args['nnodes']=None
job_args['nproc']=47
job_args['ppn']=24
job = Jobuler( **job_args )
assert job.nnodes == 2
assert job.nproc  == 47
assert job.ppn    == 24
assert job.nproc_last_node == 23
assert re.findall('select=1:ncpus=24:mpiprocs=24\+1:ncpus=23:mpiprocs=23',job.string())

# Test: set ppn
job_args['nnodes']=2
job_args['nproc']=72
job_args['ppn']=None
job = Jobuler( **job_args )
assert job.nnodes == 2
assert job.nproc  == 72
assert job.ppn    == 36
assert job.nproc_last_node == 0
assert re.findall('select=2:ncpus=36:mpiprocs=36',job.string())

# Test: set ppn with remainder on last node.
job_args['nnodes']=2
job_args['nproc']=71
job_args['ppn']=None
job = Jobuler( **job_args )
assert job.nnodes == 2
assert job.nproc  == 71
assert job.ppn    == 36
assert job.nproc_last_node == 35
assert re.findall('select=1:ncpus=36:mpiprocs=36\+1:ncpus=35:mpiprocs=35',job.string())

job.script()
print(job.string())

job.stdout_exe
job.submit()
job.stdout_exe
