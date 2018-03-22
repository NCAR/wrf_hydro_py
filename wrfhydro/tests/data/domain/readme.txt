#Overview
This docker container is for training purposes and includes a sample domain, the
latest public model code, and a simple demo of running the model. This domain 
is a small domain encompassing the West Branch of the Croton River, NY, USA (USGS stream gage 0137462010) during  hurricane Irene, 2011-08-26 to 2011-09-02. 
The simulation starts with a restart from a spinup period from 2010-10-01 to 2011-08-26. There are 3 basic 
routing configurations included in the domain, National Water Model, Gridded, 
and NCAR Reach. See the Technical Description and User Guides located at 
https://ral.ucar.edu/projects/wrf_hydro for a more detailed description of model
configurations and input files. However, some non-standard files will be described below.

#Directory contents
Croton_NY_0137462010: directory containing all domain-related files for the 
Croton, NY sample domain
	|
	-FORCING: Directory containing all hrldas hourly forcing data for the 
	simulation period.
	-Gridded: Directory containing all files required for the gridded routing 
	configuration
	   |
	    -DOMAIN: Directory containing all geospatial data for the Gridded 
	    routing domain with lakes included
	    -DOMAIN_NO_LAKES: Directory containing all geospatial data for the 
	    Gridded routing domain without lakes. Note that Gridded routing is the 
	    only configuration that requires separate geospatial data for running 
	    with or without lakes.
	    -RESTART: Directory containing model restart files for the Gridded 
	    routing domain with lakes included.
	    -RESTART_NO_LAKES: Directory containing model restart files for the 
	    Gridded routing domain without lakes.
	    -hydro.namelist: Fortran namelist file for the hydro model with lakes 
	     turned on.
	    -hydro.namelist_NO_LAKES: Fortran namelist file for the hydro model with
	     lakes turned off. Note this will need to be renamed to hydro.namelist 
	     to be used with the model.
	    -namelist.hrldas: Fortran namelist file for the land surface model with 
	     lakes turned on.
	    -namelist.hrldas: Fortran namelist file for the land surface model with 
	     lakes turned off. Note that the only difference for lakes off is using a
	     different RESTART file. Also, note this will need to be renamed to 
	     namelist.hrldas to be used with the model.
	-NWM: Directory containing all files required for the National Water Model 
	(NWM) routing configuration
	   |
	    -DOMAIN: Directory containing all geospatial data for the NWM routing 
	    domain.
		|
		 -RouteLink_nudgeEdit.nc: An edited route link file with one gage removed from nudging
	    -RESTART: Directory containing model restart files for the NWM routing 
	    domain.
	    -hydro.namelist: Fortran namelist file for the hydro model.
	    -namelist.hrldas: Fortran namelist file for the land surface model.
	-Reach: Directory containing all files required for NCAR reach routing 
	configuration
	   |
	    -DOMAIN: Directory containing all geospatial data for the NCAR reach 
	    routing domain.
	    -RESTART: Directory containing model restart files for the NCAR reach 
	    routing domain.
	    -hydro.namelist: Fortran namelist file for the hydro model.
	    -namelist.hrldas: Fortran namelist file for the land surface model.
	-Croton_USGS_obs.csv: csv file containing USGS hourly streamflow data at gage 0137462010

