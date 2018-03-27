from wrf_hydro_model import *
import copy
from boltons.iterutils import remap

#########################
# Classes for constructing and running a wrf_hydro simulation
class WrfHydroEnsembleSim(object):
    """Class for a WRF-Hydro model, which consitutes the model source code and compiled binary.
    """
    def __init__(self, source_list: list):
        """Instantiate a WrfHydroEnsembleSim object.
        Args:
            source_dir: Directory containing the source code, e.g.
               'wrf_hydro_nwm/trunk/NDHMS'.
            new_compile_dir: Optional, new directory to to hold results
               of code compilation.
        Returns:
            A WrfHydroModel object.
        """

        # Accessing a object in a list is a pain.
        # Perhaps define member as a method and ensemble_sims
        # Let e.member(1).attribute be the access or to the 
        
        # The members live in a list.
        self.ensemble_sims = source_list
        """list: of WrfHydroSim objects."""

        self.N = len(source_list)
        #"""int: The number of ensemble members."""


    def __len__(self):
        return( len(self.ensemble_sims) )

    N = __len__()



    # Would be nice if the chaining had completion for the return objects.
    # Hard to figure that out... 
    def member(self, index: int):
        if self.N == 0:
            return(None)
        return(self.ensemble_sims[index])

        
        # TODO JLM: do we want to stash metadata with each ensemble member, do we need a
        #           super class of WrfHydroSim that has metadata fields like "ens member",
        #           "description"?

    def add_member(self,
                   new_member: WrfHydroSim, 
                   copy_new_member: bool=True):
        if copy_new_member:
            self.ensemble_sims.append(copy.deepcopy(new_member))
        else:
            self.ensemble_sims.append(new_member)

        
    def replicate_member(self,
                         N: int,
                         copy_new_member: bool=True):
        # N is the final ensemble size.
        # TODO JLM: only run this if e.N==1
        for nn in range(1,N):
            self.add_member(self.ensemble_sims[0])

        
    def get_ens_attributes(self, attribute, the_key):

        # Parse up the attribute
        return_list = []
        
        def visit_path_key(path, key, value):
            if key == the_key:
                return_list.append(value) #print(path, key, value)
                return key, value
            return key, value

        def remap_path_key(ll):
            return(remap(ll, visit_path_key))
        
        att_list = [remap_path_key(getattr(i, attribute)) for i in self.ensemble_sims ]
        #att_list = [ i.hydro_namelist['nudging_nlist']['nlastobs'] for i in self.ensemble_sims ]
        return(return_list)
        

#Ens:
#Run method checks run dir name differences
#Run dir names
#Print differences across all fields, incl namelists
#Job array submission
#Operations on data.
#Bulk edit of name lists: Run start and end times, etc.
#Forcing source and run dirs (preprocess the run forcings for the run period)

