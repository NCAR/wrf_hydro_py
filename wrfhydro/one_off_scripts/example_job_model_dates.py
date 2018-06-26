zip()# add_job
## solve  model_start_time, model_end_time, model_restart variables.
### handle nones
## copy namelist from setup object to job
## apply solved times/restart to job's namelists
### namelist.hrldas: start, khour, kday, RESTART
### hydro.namelist: HYDRO_RST
## if restart: check that the restart is available
## diff the setup namelists with the new one to ensure only certain fields are changed.


import datetime
import os
import re
from wrfhydropy import *

home = os.path.expanduser("~/")
model_path = home + '/WRF_Hydro/'
the_model = WrfHydroModel(
    os.path.expanduser(model_path + '/wrf_hydro_nwm_public/trunk/NDHMS'),
    'NWM'
)

domain_path = '/Users/james/Downloads/croton_NY_domain/domain/croton_NY/'
the_domain = WrfHydroDomain(
    domain_top_dir=domain_path,
    model_version='v1.2.1',
    domain_config='NWM'
)

the_setup = WrfHydroSetup(
    the_model,
    the_domain
)

solve_model_start_end_times = job_tools.solve_model_start_end_times

# #################################

# All are 1 day and 2 hours.
def assert_start_end_soln(s,e):
    assert s == datetime.datetime(2011, 8, 26, 0, 0)
    assert e == datetime.datetime(2011, 9,  2, 0, 0)


s, e = solve_model_start_end_times(None, None, the_setup)
assert_start_end_soln(s, e)

model_start_time = '2011-08-26 00'
model_end_time = '2011-09-02 00'
s, e = solve_model_start_end_times(model_start_time, model_end_time, the_setup)
assert_start_end_soln(s, e)

model_start_time = '2011-08-26 00:00'
model_end_time = '2011-09-02 00'
s, e = solve_model_start_end_times(model_start_time, model_end_time, the_setup)
assert_start_end_soln(s, e)

model_start_time = '2011-08-26 00:00'
model_end_time = datetime.timedelta(days=7)
s, e = solve_model_start_end_times(model_start_time, model_end_time, the_setup)
assert_start_end_soln(s, e)

model_start_time = '2011-08-26 00:00'
model_end_time = {'hours': 24*7}
s, e = solve_model_start_end_times(model_start_time, model_end_time, the_setup)
assert_start_end_soln(s, e)

model_start_time = '2011-08-26 00:00'
model_end_time = {'days': 6, 'hours': 24}
s, e = solve_model_start_end_times(model_start_time, model_end_time, the_setup)
assert_start_end_soln(s, e)
