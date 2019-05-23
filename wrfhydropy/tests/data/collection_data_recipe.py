#!/usr/bin/env python
# coding: utf-8

# # Ensemble Cycle For Testing wrfhydropy Collects

import datetime
import os
import pathlib
import pickle
import pywrfhydro
import sys
import wrfhydropy
import xarray as xa


# ## Configuration

scratch_dir = pathlib.Path('/glade/scratch/jamesmcc/')
work_dir = pathlib.Path('/glade/work/jamesmcc/')
home_dir = pathlib.Path('/glade/u/home/jamesmcc/')

experiment_dir = scratch_dir / 'ens_cycle_example'

domain_dir = experiment_dir / 'croton_NY'
model_dir = home_dir / 'WRF_Hydro/wrf_hydro_nwm_public'
compile_dir = experiment_dir / 'compile'

configuration = 'nwm_ana'

ens_routelink_dir = experiment_dir / 'routelink_ens'
sim_dir = experiment_dir / 'sim'
ens_dir = experiment_dir / "ens_sim"
ens_ana_dir = experiment_dir / "ens_ana"


# ## Data

# Set up the experiment directory and pull the croton domain:

if not experiment_dir.exists():
    os.mkdir(experiment_dir)

# This will hang/fail on a cheyenne compute node... 
if not domain_dir.exists():
    file_id = "1xFYB--zm9f8bFHESzgP5X5i7sZryQzJe"
    download_script = model_dir / 'tests/local/utils/gdrive_download.py'
    function_name = "download_file_from_google_drive"
    sys.path.insert(0, str(download_script.parent))
    download_file_from_google_drive = getattr(
        __import__(str(download_script.stem), fromlist=[function_name]), 
        function_name
    )
    download_file_from_google_drive(file_id, str(experiment_dir / 'croton_NY.tar.gz'))

    get_ipython().run_cell_magic('bash', '', 'cd /glade/scratch/jamesmcc/ens_cycle_example/ ;\ntar xzf croton_NY.tar.gz ;\nmv example_case croton_NY')


# ## Building Blocks
# ### Domain

domain = wrfhydropy.Domain(
    domain_top_dir=domain_dir,
    domain_config=configuration
)


# ### Model

model = wrfhydropy.Model(
    source_dir=model_dir / 'trunk/NDHMS', 
    model_config=configuration,
    #hydro_namelist_config_file=domain_dir / 'hydro_namelists.json',
    #hrldas_namelist_config_file=domain_dir / 'hrldas_namelists.json',
    #compile_options_config_file=domain_dir / 'compile_options.json',
    compiler='ifort'
)


model_pkl = compile_dir / 'WrfHydroModel.pkl'
if not model_pkl.exists():
    model.compile(compile_dir)
else:
    model = pickle.load(model_pkl.open('rb'))


# ### Job

model_start_time = datetime.datetime(2018, 8, 1, 0)
model_end_time = model_start_time + datetime.timedelta(hours=2)
job = wrfhydropy.Job(
    job_id = 'flo_sim', 
    model_start_time=model_start_time,
    model_end_time=model_end_time,
    output_freq_hr=1,
    restart_freq_hr=1,
    exe_cmd = 'mpirun -np 1 ./wrf_hydro.exe'
)


# ### Simulation

sim = wrfhydropy.Simulation()
sim.add(domain)
sim.add(model)
sim.add(job)

# ### Ensemble

ens = wrfhydropy.EnsembleSimulation()
ens.add(sim)
ens.add(job) # if the job is not present, member diffs are messed up!
ens.replicate_member(3)


# #### Routelink ensemble

rl_file = domain_dir / 'NWM/DOMAIN/Route_Link.nc'
routelink = xa.open_dataset(rl_file)
mannings_n = routelink['n']

if not ens_routelink_dir.exists():
    ens_routelink_dir.mkdir(parents=True)
deltas = [ .3, 1.0, 1.7]
for delta in deltas:
    out_file = ens_routelink_dir / ('Route_Link_edit_' + str(delta) + '.nc')
    values_dict = { 'n' : mannings_n + delta }
    result = pywrfhydro.routelink_edit(values_df=values_dict, in_file=rl_file, out_file=out_file)
    print(result)
routelink_files = [str(ff) for ff in sorted(ens_routelink_dir.glob("Route_Link*.nc"))]
print(routelink_files)
ens.set_member_diffs(
    att_tuple=('base_hydro_namelist', 'hydro_nlist', 'route_link_f'),
    values = routelink_files
)


ens.member_diffs


# ## Ensemble Cycle

init_times = [
    datetime.datetime(2011, 8, 26, 0), 
    datetime.datetime(2011, 8, 26, 1),
    datetime.datetime(2011, 8, 26, 2),
    datetime.datetime(2011, 8, 26, 3)
]
n_members = len(ens)
# Look back units are in hours, not casts.
restart_dirs = [['.'] * n_members, [-1] * n_members, ['-1'] * n_members, ['-1'] * n_members]

ens_ana = wrfhydropy.CycleSimulation(
    init_times=init_times,
    restart_dirs=restart_dirs, 
    ncores=1
)

ens_ana.add(ens)
ens_ana.add(job)

if not ens_ana_dir.exists():
    os.mkdir(ens_ana_dir)
    os.chdir(ens_ana_dir)
    ens_ana.compose()
    return_code = ens_ana.run(n_concurrent=1)

print(return_code)


# ## Wrap up
# Clean up unnecessary items in the experiment directory. Then package it up.

get_ipython().run_cell_magic('bash', '', 'cd /glade/scratch/jamesmcc/ens_cycle_example/\nrm croton_NY.tar.gz\n\nrm compile/wrf_hydro.exe\n\ncd croton_NY\nrm -rf Gridded Gridded_no_lakes/ Reach/ supplemental/\nrm USGS_obs.csv  Readme.txt  hydro_namelist_patches.json hrldas_namelist_patches.json study_map.PNG\nrm example_case  hydro_namelist_patches.json~ hrldas_namelist_patches.json~ \n\ncd NWM\nrm -rf DOMAIN_LR/ RESTART_LR/ referenceSim/\nrm hydro.namelist namelist.hrldas \n\ncd nudgingTimeSliceObs\nrm 2011-09*.usgsTimeSlice.ncdf 2011-08-3*.usgsTimeSlice.ncdf 2011-08-2[7-9]*.usgsTimeSlice.ncdf \nrm 2011-08-26_[1-2]*.usgsTimeSlice.ncdf 2011-08-26_0[6-9]*.usgsTimeSlice.ncdf 2011-08-25*.usgsTimeSlice.ncdf\n\ncd ../../FORCING/\nrm 201109*LDASIN_DOMAIN1 2011083*.LDASIN_DOMAIN1 2011082[7-9]*.LDASIN_DOMAIN1\nrm 20110826[1-2]*.LDASIN_DOMAIN1 201108260[6-9]*.LDASIN_DOMAIN1\nrm 2011082600.LDASIN_DOMAIN1')

get_ipython().run_cell_magic('bash', '', 'cd /glade/scratch/jamesmcc/\nmv ens_cycle_example wrfhydropy_io_collect_data\ntar czf wrfhydropy_io_collect_data.tar.gz wrfhydropy_io_collect_data')

