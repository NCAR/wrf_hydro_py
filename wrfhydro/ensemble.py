from wrf_hydro_model import *
from deepdiff import DeepDiff
from boltons.iterutils import remap
import copy


# ########################
class DeepDiffEq(DeepDiff):

    def __init__(self,
                 t1,
                 t2,
                 eq_types,
                 ignore_order=False,
                 report_repetition=False,
                 significant_digits=None,
                 exclude_paths=set(),
                 exclude_regex_paths=set(),
                 exclude_types=set(),
                 include_string_type_changes=False,
                 verbose_level=1,
                 view='text',
                 **kwargs):

        # Must set this first for some reason.
        self.eq_types = set(eq_types)
        
        super().__init__(t1,
                         t2,
                         ignore_order=False,
                         report_repetition=False,
                         significant_digits=None,
                         exclude_paths=set(),
                         exclude_regex_paths=set(),
                         exclude_types=set(),
                         include_string_type_changes=False,
                         verbose_level=1,
                         view='text',
                         **kwargs)

    # Have to force override __diff_obj.
    def _DeepDiff__diff_obj(self, level, parents_ids=frozenset({}),
                            is_namedtuple=False):
        """Difference of 2 objects using their __eq__ if requested"""

        if type(level.t1) in self.eq_types:
            if level.t1 == level.t2:
                return
            else:
                self._DeepDiff__report_result('values_changed', level)
                return

        super(DeepDiffEq, self).__diff_obj(level, parents_ids=frozenset({}),
                                           is_namedtuple=False)





def copy_member(member,
                do_copy: bool):
    if do_copy:
        return(copy.deepcopy(member))
    else:
        return(member)
    
# ########################
# Classes for constructing and running a wrf_hydro simulation
class WrfHydroEnsembleSim(object):
    """Class for a WRF-Hydro model, which consitutes the model source code and compiled binary.
    """
    def __init__(self,
                 members: list,
                 ensemble_dir: str='' ):
        """Instantiate a WrfHydroEnsembleSim object.
        Args:
            members: 
            ensemble_dir: Optional, 
        Returns:
            A WrfHydroEnsembleSim object.
        """
        self.__members = []
        self.members = members
        self.__members_dict = {}
        """list: of WrfHydroSim objects."""

        # Several simulation properties are not specified
        # until run time. Place them here
        self.ens_dir = ''


    # Data to store in the ensemble object
    # 1) list of simulations = the ensemble
    # 2) N = __len__(), @property
    # 3) ensemble dir, the directory containing the ensemble member_dir run dirs


    def __len__(self):
        return( len(self.members) )

    
    # The "canonical" name for len
    @property
    def N(self):
        return(self.__len__())


    # Data to store with the "member" simulations, conceptually this
    # data belongs to the members:
    # 1) member number
    # 2) description
    # 3) member_dir
    # 4) forcing_source_dir
    #
    # Ensemblize the individual members.
    # Except for changing the Class definition, why
    # would I define a child class instead of just adding attributes?


    @property
    def members(self):
        return(self.__members)

    @members.setter
    def members(self,
                     new_members: list, 
                     copy_members: bool=True):

        if( type(new_members) is not list ):
            new_members = [ new_members ]

        for nn in new_members:
            self.__members.append(copy_member(nn, copy_members))
            # If copying an existing ensemble member, nuke the metadata
            # number is the detector for all ensemble metadata.
            if hasattr(nn, 'number'):
                delattr(self.__members[len(self.__members)-1], 'number')

        # Put refs to these properties in the ensemble objects
        for mm in range(len(self.__members)):
            if not hasattr(self.__members[mm], 'number'):
                self.__members[mm].number = -1
                self.__members[mm].description = ''
                self.__members[mm].run_dir = ''
                self.__members[mm].forcing_source_dir = ''


    # A quick way to setup a basic ensemble from a single sim.
    def replicate_member(self,
                         N: int,
                         copy_members: bool=True):
        if self.N > 1:
            print('WTF mate?')
        else:
            self.members = [ self.members[0] for nn in range(N-1) ]

            
    @property        
    def members_dict(self):
        m_dict = self.__members_dict
        for mm in range(len(self.members)):
            self.members[mm].number = mm
        m_dict['number'] = [ mm.number for mm in self.members ]
        m_dict['description'] = [ mm.description for mm in self.members ]
        m_dict['run_dir'] = [ mm.run_dir for mm in self.members ]
        m_dict['forcing_source_dir'] = [ mm.forcing_source_dir for mm in self.members ]
        return(m_dict)

    @members_dict.setter
    def members_dict(self,
                     att_path_key: str,
                     values: list): 
        m_dict = self.__members_dict

        m_dict[att_path_key] =[]
        
        att_path_key_tuple =  tuple(map(str, att_path_key.split('/')))
        att_key = att_path_key_tuple[len(key_path_tuple)-1]
        att_path = key_path_tuple[0:(len(key_path_tuple)-1)]

        def visit(path, key, value):
            if path == att_path:
                if key == 'att_key':
                    m_dict[att_path_key] = m_dict[att_path_key].append(value)

        for mm in self.members:            
            remap(mm.__dict__, visit=visit)


        
    # Would want a method for detecting differences between ensemble members
    # instead of just specifying them... 
                                         


    # def get_ens_attributes(self, attribute, the_key):

    #     # Parse up the attribute
    #     return_list = []
        
    #     def visit_path_key(path, key, value):
    #         if key == the_key:
    #             return_list.append(value) #print(path, key, value)
    #             return key, value
    #         return key, value

    #     def remap_path_key(ll):
    #         return(remap(ll, visit_path_key))
        
    #     att_list = [remap_path_key(getattr(i, attribute)) for i in self.members ]
    #     #att_list = [ i.hydro_namelist['nudging_nlist']['nlastobs'] for i in self.members ]
    #     return(return_list)
        

#Ens:
#Run method checks run dir name differences
#Run dir names
#Print differences across all fields, incl namelists
#Job array submission
#Operations on data.
#Bulk edit of name lists: Run start and end times, etc.
#Forcing source and run dirs (preprocess the run forcings for the run period)

