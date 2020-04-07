Overview: What and why wrfhydropy?
==================================

What is wrfhydropy?
-------------------

**wrfhydropy** provides an end-to-end python interface to support 
reproducible research and construction of workflows involving the 
WRF-Hydro model. 

**wrfhydropy**:  
    * Is a Python API for the WRF-Hydro modelling system. 
    * Provides tools for working with WRF-Hydro input (preparation)
      and output (analysis), largely based on xarray_.  
    * Is tested_ and coverage_ is calculated.  

The package provides fine-grained control over the model and its
inputs and outputs. Generally, high-level workflows are not found here
**but should be and can easily be built from wrfhydropy**.

**wrfhydropy** facilitates all aspects of working with WRF-Hydro including:  
    * compiling
    * setting up experiments (manipulating input files and namelists)
    * running and scheduling jobs
    * collecting output
    * analysis (input and output)
    * sharing and reproducing results (jupyter notebooks)

The wrfhydropy package is **user supported and community contributed**. That
means you can help add to and improve it!

            
Why wrfhydropy?
---------------
The WRF-Hydro model was not originally built with many applications or workflows
in mind. Without significant investment in rewriting the code, a scripting
language is needed to adapt the FORTRAN model API to something suited to other
purposes. Python is a good choice for this secondary API language for a vareity of
reasons (widely adopted, multi-platform, great packages for scientific analysis,
etc ...). Python therefore provides a mechanism for developing a better (for many
purposes) model interface that is afforded by the underlying model.  For this reason, 
a few conceptualizations in wrfhydropy are formalized differently than in FORTRAN. 
These are summarized in `Key concepts`_. The model API as developed in python may begin 
to make its way back to the underlying FORTRAN code with time.

wrfhydropy was initally developed to handle the WRF-Hydro model testing
(`wrf_hydro_nwm_public/tests <https://github.com/NCAR/wrf_hydro_nwm_public/tree/master/tests>`_) 
and, in particularly, the need to be able to
easily swap domains while holding model options constant. Another early
application was the construction and execuation of ensembles and ensemble
forecasts. The examples_ included in this documentation will grow to show other 
applications of the package.


Limitations of wrfhydropy
-------------------------

The wrfhydropy package does many things but also has limitations
which are worth acknowledging up-front. The development of wrfhydropy has
mostly emerged to support testing and other applications of the NWM. While
wrfhydropy supports other modes of running WRF-Hydro, the further away from
the NWM you get the less likely wrfhydropy will support your needs. This
guidance is highly dependent on the differences from the NWM. If the differences 
are containted in the namelists only, you are likely not going to have issues. But
attempting to use the Noah model instead of NoahMP, for example, will
simply not work. wrfhydropy is open to changes/enhancements to support your needs,
but may require you to implement *and test* them to get them into the master branch.

wrfhydropy does not provide an in-memory connection between WRF-Hydro and Python. 
The API is implemented through system calls (Python's subprocess) and all information
between Python and the model passes through disk. There is no magic in wrfhydropy, 
just convenience: you still need a system and environment in which WRF-Hydro can be
compiled and run. (Such as our `development docker container`_.)


Key concepts
------------

Here we summarize a few concepts in wrfhydropy which differ from how WRF-Hydro is generally
used. Links are provided to examples.


Object Oriented API
###################
THe wrfhydropy model API follows an object oriented approach. Composition
of objects is a theme of the design. That is: core building blocks are put
together to form more complicated objects. The separation of concerns of these
objects is important (and sometimes challenging), but often rewarding.

Upper case means a class (and will link to the class definition).
Lower case means an instance of a class (not linked).
The left arrow means object composition, also known as a "has a" relationship.

Core objects:
  * Domain
  * Model
  * Job
  * Scheduler

Higher-level objects: 
  * Simulation <- domain, model, job [, scheduler]
  * Ensemble <- simulation, job [, scheduler]
  * Cycle <- simulation|ensemble, job [, scheduler]

The first example in the documentation, 
`End-to-end overview of wrfhydropy: Simulation evaluation`_
details the core objects, their initialization and their composition into
a Simulation object.

    
Namelists: Model and domain sides
#################################
Namelists are treated by wrfhydropy in a completely different way
than WRF-Hydro model users experience them. The input namelists to the model, 
namelist.hrldas and hydro.namelist are each split in to two pieces, the model-side 
and domain-side options. The new namelist files collect many different potential 
namelists using named configurations. The motivation for this and the details are 
explained in depth in `namelist section`_ of the first example of the documentation.


Jobs: 
#####
The notion of a Job is formalized by wrfhydropy and can be a bit surprising to 
WRF-Hydro users. Jobs are essential model time and frequency interventions into the 
model namelists. Each job has a different call to the executable and a subdirectory
of the run directory dedicated to its provenance and its artifacts. Details are
provided in the `Job section`_ of the first example of the documentation. 


.. _xarray: http://xarray.pydata.org/en/stable/
.. _tested: https://github.com/NCAR/wrf_hydro_py/tree/master/wrfhydropy/tests
.. _coverage: https://coveralls.io/github/NCAR/wrf_hydro_py
.. _examples: https://wrfhydropy.readthedocs.io/en/latest/examples.html
.. _`development docker container`: https://hub.docker.com/r/wrfhydro/dev
.. _`End-to-end overview of wrfhydropy: Simulation evaluation`: https://wrfhydropy.readthedocs.io/en/latest/examples/ex_01_end_to_end.html
.. _`namelist section`: https://wrfhydropy.readthedocs.io/en/latest/examples/ex_01_end_to_end.html#2.-Namelists-and-configurations-in-wrfhydropy
.. _`Job section`: https://wrfhydropy.readthedocs.io/en/latest/examples/ex_01_end_to_end.html#7.-Job-object