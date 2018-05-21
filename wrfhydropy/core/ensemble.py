import ast
from boltons.iterutils import remap, get_path
import copy
import pathlib

from .wrfhydroclasses import WrfHydroRun
from .ensemble_tools import DeepDiffEq, dictify, get_sub_objs


def copy_member(
    member,
    do_copy: bool
):
    if do_copy:
        return(copy.deepcopy(member))
    else:
        return(member)


# ########################
# Classes for constructing and running a wrf_hydro simulation
class WrfHydroEnsembleSetup(object):
    """Class for a WRF-Hydro model, which consitutes the model source code and compiled binary.
    """
    def __init__(
        self,
        members: list,
        ensemble_dir: str=''
    ):
        """Instantiate a WrfHydroEnsembleSim object.
        Args:
            members: 
            ensemble_dir: Optional, 
        Returns:
            A WrfHydroEnsembleSim object.
        """
        self.__members = []
        self.members = members
        self.__diffs_dict = {}
        """list: of WrfHydroSim objects."""

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

    @property
    def members(self):
        return(self.__members)

    @members.setter
    def members(
        self,
        new_members: list, 
        copy_members: bool=True
    ):

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
                self.__members[mm].number = "%03d" % (mm,)
                self.__members[mm].description = ''
                self.__members[mm].run_dir = 'member_' + self.__members[mm].number
                self.__members[mm].forcing_source_dir = ''


    # A quick way to setup a basic ensemble from a single sim.
    def replicate_member(self,
                         N: int,
                         copy_members: bool=True):
        if self.N > 1:
            print('WTF mate?')
        else:
            self.members = [ self.members[0] for nn in range(N-1) ]


    # The diffs_dict attribute has getter (@property) and setter methods.
    # The get method summarizes all differences across all the attributes of the
    #   members list attribute and (should) only report member attributes when there
    #   is at least one difference between members.
    # The setter method is meant as a convenient way to specify the differences in
    #   member attributes across the ensemble.


    @property        
    def diffs_dict(self):

        if len(self) == 1:
            print('Ensemble is of lenght 1, no differences.')
            return {}

        mem_0_ref_dict = dictify(self.members[0])

        all_diff_keys=set({})
        for ii in range(1,len(self)):
            mem_ii_ref_dict = dictify(self.members[ii])
            diff = DeepDiffEq(mem_0_ref_dict, mem_ii_ref_dict, eq_types={pathlib.PosixPath})

            unexpected_diffs = set(diff.keys()) - set(['values_changed'])
            if len(unexpected_diffs):
                unexpected_diffs1 = { uu: diff0[uu] for uu in list(unexpected_diffs) }
                raise ValueError(
                    'Unexpected attribute differences between ensemble members:',
                    unexpected_diffs1
                )

            diff_keys = list(diff['values_changed'].keys())
            all_diff_keys = all_diff_keys | set([ ss.replace('root','') for ss in diff_keys ])

        diff_tuples = [ss.replace('][',',') for ss in list(all_diff_keys)]
        diff_tuples = [ss.replace('[','(') for ss in list(diff_tuples)]
        diff_tuples = [ss.replace(']',')') for ss in list(diff_tuples)]
        diff_tuples = [ast.literal_eval(ss) for ss in list(diff_tuples)]

        self.__diffs_dict = {}
        for dd in diff_tuples:
            self.__diffs_dict[dd] = [ get_path(dictify(mm), dd) for mm in self.members ]

        return(self.__diffs_dict)


    def set_diffs_dict(
        self,
        att_tuple: tuple,
        values: list
    ):

        if type(values) is not list:
            values = [values]

        if len(values) == 1:
            the_value = values[0]
            values = [the_value for ii in range(len(self))]

        if len(values) != len(self):
            raise ValueError("The number of values supplied does not equal the number of members.")

        def update_obj_dict(obj, att_tuple):

            def visit(path, key, value):
                superpath = path + (key,)
                #print(superpath)
                if superpath != att_tuple[0:len(superpath)]:
                    return True
                if len(superpath) == len(att_tuple):
                    return key, new_value
                return True

            the_remap = remap(obj.__dict__, visit)
            obj.__dict__.update(the_remap)
            for ss in get_sub_objs(obj.__dict__):
                att_tuple_0 = att_tuple
                att_tuple = att_tuple[1:]
                if len(att_tuple) > 0:
                    update_obj_dict(obj.__dict__[ss], att_tuple)
                att_tuple = att_tuple_0

        for ii in range(len(self)):
            new_value = values[ii]
            #print(new_value)
            update_obj_dict(self.members[ii], att_tuple)


class WrfHydroEnsembleRun(object):
    def __init__(
        self,
        ens_setup: WrfHydroEnsembleSetup,
        run_dir: str,
        rm_existing_run_dir = False,
        jobs: list=None
    ):

        self.ens_setup = copy.deepcopy(ens_setup)
        """WrfHydroSetup: The WrfHydroSetup object used for the run"""

        # TODO(JLM): check all the setup members have to have rundirs with same path as run_dir
        self.run_dir = pathlib.PosixPath(run_dir)
        """pathlib.PosixPath: The location of where the jobs will be executed."""

        self.jobs_completed = []
        """Job: A list of previously executed jobs for this run."""
        self.jobs_pending = []
        """Job: A list of jobs *scheduled* to be executed for this run 
            with prior job dependence."""
        self.job_active = None
        """Job: The job currently executing."""        

        self.object_id = None
        """str: A unique id to join object to run directory."""

        self.members = []

        # Create the members list of run objects.
        for mm in self.ens_setup.members:
            self.members.append(WrfHydroRun(mm, run_dir = mm.run_dir, deepcopy_setup=False))

        # Make run_dir directory if it does not exist.
        # if self.run_dir.is_dir() and not rm_existing_run_dir:
        #     raise ValueError("Run directory already exists and rm_existing_run_dir is False.")

        # if self.run_dir.exists():
        #     shutil.rmtree(str(self.run_dir))
        # self.run_dir.mkdir(parents=True)

        ## TODO(JLM): I would prefer if the member runs dont make their parent dirs.
    
        
        
#Ens:
#Job array submission
#Operations on data.

