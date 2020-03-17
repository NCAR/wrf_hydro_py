Overview: What and why wrfhydropy?
==================================

What is wrfhydropy?
-------------------

`wrfhydropy` provides an end-to-end user python interface to support 
reproducible research and construction of workflows involving the 
WRF-Hydro model. 

`wrfhydropy`:
1. Is A Python API for the WRF-Hydro modelling system. 
2. Provides tools for working with model input (preparation) 
   and output (analysis), largely based on 
   (xarray)[http://xarray.pydata.org/en/stable/].
3. Is tested.

The package provides fine-grained control over the model and its 
inputs and outputs. Generally, high-level workflows are not found here
*but should be and can easily be built from `wrfhydropy`.*

`wrfhydropy` facilitates all aspects of working with WRF-Hydro including:
    * compiling
    * setting up experiments (manipulating input files and namelists)
    * running and scheduling jobs
    * collecting output
    * analysis (input and output)
    * sharing and reproducing results (jupyter notebooks)

The `wrfhydropy` package is **user supported and community contributed**. That
means you can help add to and improve it.



`wrfhydropy` is tested.

            
Why wrfhydropy?
---------------
The WRF-Hydro model was not originally built with many applications or workflows
in mind. Without significant investment in rewriting the code, a scripting
language is needed to adapt the FORTRAN model API to something suited to other
purposes. Python is a good choice for this secondary API language for a vareity of
reasons (widely adopted, multi-platform, great packages for scientific analysis,
etc ...). Python therefore provides a mechanism for developing a better (for many
purposes) model interface that is afforded by the underlying model. The
model API as developed in python may begin to make its way back to the underlying
FORTRAN code with time. For this reason, a few conceptualizations in `wrfhydropy`
are formalized differently than in FORTRAN. These are summarized in `key_concepts`.

`wrfhydropy` was initally developed to handle the WRF-Hydro model testing
(`wrf_hydro_nwm_public/tests`) and, in particularly, the need to be able to
easily swap domains while holding model options constant. Another early
application was the construction and execuation of ensembles and ensemble
forecasts. 


Limitations of wrfhydropy
-------------------------

The `wrfhydropy` package does many things but also has some strong limitations
which are worth acknowledging up-front. The development of `wrfhydropy` has
mostly emerged to support testing and other applications of the NWM. While
`wrfhydropy` supports other modes of running WRF-Hydro, the further away from
the NWM you get, the less likely `wrfhydropy` will support your needs. This
guidance is highly dependent on those differences. If those differences are
containted in the namelists only, you are likely not going to have issues. But
attempting to use the `Noah` model instead of `NoahMP`, for example, will
simply not work. We are open to changes/enhancements to support your needs,
but we may require you to perform them and make a pull request to get them
into the upstream master branch.


Key concepts
------------
A few concepts in wrfhydropy differ from how WRF-Hydro is generally
interfaced. These are explained and summarized her.

Object Oriented API
###################
THe `wrfhydropy` model API follows an object oriented approach. Composition
of objects is a theme of the design. That is core building blocks are put
together to form more complicated object. The separation of concerns of these
objects is important (and sometimes challenging).

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
  * Forecast <- simulation, job [, scheduler]
  * Ensemble_Forecast <- ensemble, job [, scheduler]

    
Namelists: model and domain sides
#################################


