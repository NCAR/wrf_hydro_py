import json
import f90nml

#############################
###Write master namelist
hydroNamelistPath='/Volumes/d1/jmills/NCAR-docker/wrf_hydro_docker/domains/croton_NY/domain/NWM/hydro.namelist'
hrldasNamelistPath='/Volumes/d1/jmills/NCAR-docker/wrf_hydro_docker/domains/croton_NY/domain/NWM/namelist.hrldas'

hydro_namelist=dict(f90nml.read(hydroNamelistPath))
hydro_namelist_no_files = dict(f90nml.read(hydroNamelistPath))

hydro_keys_to_delete = ['geo_static_flnm',
                  'geo_finegrid_flnm',
                  'hydrotbl_f',
                  'land_spatial_meta_flnm',
                  'restart_file',
                  'route_link_f',
                  'route_lake_f',
                  'gwbuckparm_file',
                  'udmap_file']

nudging_keys_to_delete = ['timeslicepath',
                          'nudgingparamfile',
                          'nudginglastobsfile']

for key in hydro_keys_to_delete:
    #del hydro_namelist_no_files['hydro_nlist'][key]
    hydro_namelist_no_files['hydro_nlist'][key] = 'NULL_specified_in_domain.json'

for key in nudging_keys_to_delete:
    #del hydro_namelist_no_files['nudging_nlist'][key]
    hydro_namelist_no_files['nudging_nlist'][key] = 'NULL_specified_in_domain.json'

#Label with version and config
hydro_master_namelist = {}
hydro_master_namelist['v1.2.1'] = {'NWM':hydro_namelist_no_files,'Gridded':{},'Reach':{}}


# Write dict to json
output_file='hydro_namelists.json'
with open(output_file, mode='w') as f:
    json.dump(hydro_master_namelist, f, indent=4)
    f.close()

###Hrldas namelist
hrldas_namelist=dict(f90nml.read(hrldasNamelistPath))
hrldas_namelist_no_files = dict(f90nml.read(hrldasNamelistPath))

hrldas_keys_to_delete = ['hrldas_setup_file',
                         'indir',
                         'spatial_filename',
                         'restart_filename_requested',
                         "start_year",
                         "start_month",
                         "start_day",
                         "start_hour",
                         "start_min",
                         "forcing_timestep",
                         "noah_timestep",
                         "output_timestep"]

for key in hrldas_keys_to_delete:
    #del hrldas_namelist_no_files['noahlsm_offline'][key]
    hrldas_namelist_no_files['noahlsm_offline'][key] = 'NULL_specified_in_domain.json'

#del hrldas_namelist_no_files['wrf_hydro_offline']['forc_typ']
hrldas_namelist_no_files['wrf_hydro_offline']['forc_typ'] = 'NULL_specified_in_domain.json'

#Label with version and config
hrldas_master_namelist = {}
hrldas_master_namelist['v1.2.1'] = {'NWM':hrldas_namelist_no_files,'Gridded':{},'Reach':{}}

# Write dict to json
output_file='hrldas_namelists.json'
with open(output_file, mode='w') as f:
    json.dump(hrldas_master_namelist, f, indent=4)
    f.close()

#############################
###Write domain.json template
###Use the deleted keys to start a domain.json skeleton
domain_namelist = {}
domain_namelist['hydro_namelist'] = {}
domain_namelist['hydro_namelist']['hydro_nlist'] = {}
domain_namelist['hydro_namelist']['nudging_nlist'] = {}
domain_namelist['hydro_namelist']['nudging_nlist'] = {}

for key in hydro_keys_to_delete:
    domain_namelist['hydro_namelist']['hydro_nlist'][key] = hydro_namelist['hydro_nlist'][key]

for key in nudging_keys_to_delete:
    domain_namelist['hydro_namelist']['nudging_nlist'][key] = hydro_namelist['nudging_nlist'][key]


domain_namelist['namelist_hrldas'] = {}
domain_namelist['namelist_hrldas']['noahlsm_offline'] = {}

for key in hrldas_keys_to_delete:
    domain_namelist['namelist_hrldas']['noahlsm_offline'][key] = hrldas_namelist['noahlsm_offline'][key]

domain_namelist['namelist_hrldas']['wrf_hydro_offline']= {'forc_typ' : '1'}

domain_master_namelist = {}
domain_master_namelist['v1.2.1'] = {'NWM':domain_namelist,'Gridded':{},'Reach':{}}

# Write dict to json
output_file='domain_namelists.json'
with open(output_file, mode='w') as f:
    json.dump(domain_master_namelist, f, indent=4)
    f.close()