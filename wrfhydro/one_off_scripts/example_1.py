import wrf_hydro_model
repo_path = '/Users/jamesmcc/WRF_Hydro/wrf_hydro_nwm_myFork'
model = wrf_hydro_model(repo_path)

model.compile('gfort')

domain = wrf_hydro_domain('/home/docker/domain/croton_NY',
                          domain_config='NWM',
                          domain_dir='NWM/DOMAIN',
                          restart_dir='NWM/RESTART')

run_sim = wrf_hydro_simulation(model, domain).run()
ncores_sim = wrf_hydro_simulation(model, domain).run()
restart_sim = wrf_hydro_simulation(model, domain)

#Additional scripting to edit the namelist
#reference_run_sim = wrf_hydro_simulation(wrfModel, wrfDomain).run()

#wrfSim.make_run_dir('/home/docker/test/run2')
#candidate_run_sim.run()

#candidate_wrfModel = wrf_hydro_model('/home/docker/wrf_hydro_nwm/trunk/NDHMS',
#                                     '/home/docker/test/compile')
#candidate_wrfModel.compile('gfort')
