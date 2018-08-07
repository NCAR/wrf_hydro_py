#Overview This test case includes prepared geospatial data and input files for a
sample domain (region of interest) and prepared forcing data. The forcing data
prepared for this test case is North American Land Data Assimilation System
(NLDAS) hourly data. This domain is a small region encompassing the West Branch
of the Croton River, NY, USA (USGS stream gage 0137462010) during  hurricane
Irene, 2011-08-26 to 2011-09-02.  The simulation begins with a restart from a
spinup period from 2010-10-01 to 2011-08-26.  There are 3 basic routing
configurations included in the test case, National Water Model (NWM), Gridded,
and NCAR Reach. See the WRF-Hydro V5 Technical Description located at
https://ral.ucar.edu/projects/wrf_hydro for a more detailed description of model
physics options, configurations, and input files. However, some non-standard
files will be described below.

For instructions on how to set up and run this test case see the Test Case User
Guide available from https://ral.ucar.edu/projects/wrf_hydro/testcases

#Directory contents Croton_NY: directory containing all input files for the
Croton, NY example test case     
	|     
	-FORCING: Directory containing all NLDAS hrldas formatted hourly 
	forcing data for the simulation period.
	-Gridded: Directory containing all files required for the gridded routing
	configuration        
		|
		-DOMAIN: Directory containing all geospatial data and input files for
		the Gridded routing option with lakes included
		-lake_shapes: directory containing shape files that define lakes
		-DOMAIN_NO_LAKES: Directory containing all geospatial data and input
		files for the Gridded routing option without lakes. Note that Gridded
		routing is the only configuration that requires separate geospatial data
		for running with or without lakes.         
		-RESTART: Directory containing model restart files for the Gridded 
		routing option with lakes included.
		-RESTART_NO_LAKES: Directory containing model restart files for the 
		Gridded routing option without lakes.         
		-referenceSim: Directory containing restart files and a hydrograph from
		a successful run for reference
		-hydro.namelist: Fortran namelist file for the hydro model with lakes 
		turned on.        
		-hydro.namelist_NO_LAKES: Fortran namelist file for the hydro model with
		lakes turned off. Note this will need to be renamed to hydro.namelist 
		to be used with the model.         
		-namelist.hrldas: Fortran namelist file for the Noah-MP land surface 
		model with lakes turned on.
		-namelist.hrldas: Fortran namelist file for the Noah-MP land surface 
		model with lakes turned off. Note that the only difference for lakes off
		is using a different RESTART file. Also, note this will need to be 
		renamed to namelist.hrldas to be used with the model.
		-croton_frxst_pts_csv.csv: .CSV formatted file of gage locations in 
		latitude/longitude coordinates (WGS84)     
	-NWM: Directory containing all files required for the National Water Model
	(NWM) routing configuration
		|
		-DOMAIN: Directory containing all geospatial data and input files for 
		the NWM routing option.         
			|          
			-RouteLink_nudgeEdit.nc: An edited route link file with one gage 
			removed from nudging   
		-RESTART: Directory containing model restart files for the NWM routing
		option.
		-referenceSim: Directory containing restart files and hydrograph from a
		successful run for reference         
		-nudgingTimeSliceObs: Directory containing nudging "time slice" 
		observation files.        
		-hydro.namelist: Fortran namelist file for the hydro model.         
		-namelist.hrldas: Fortran namelist file for the Noah-MP land surface 
		model.     
	-Reach: Directory containing all files required for NCAR reach routing      configuration       
		|
		-DOMAIN: Directory containing all geospatial data and input files for
		the NCAR reach routing option.
		-stream_network: directory containing files that define the stream
		network.        
		-RESTART: Directory containing model restart files for the NCAR reach
		routing option.
		-referenceSim: Directory containing restart files and hydrograph from a
		successful run for reference         
		-hydro.namelist: Fortran namelist file for the hydro model.
		-namelist.hrldas: Fortran namelist file for the Noah-MP land surface
		model.    
	-USGS-Obs.csv: csv files containing USGS 15 minute streamflow data for
	gages in the domain. 
	-study_map.PNG: Study area map
	-namelist_patches.json: json file used by wrfhydropy python package for
	namelist parsing
	-supplimental_forcing.tar.gz: Tar ball containing additional forcing 
	data for spinup

