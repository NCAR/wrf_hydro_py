# WRF_HYDRO_NWM_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_nwm_myFork
# WRF_HYDRO_PY_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_py

# docker create --name croton wrfhydro/domains:croton_NY
# ## The complement when youre done with it:
# ## docker rm -v croton

# docker run -it \
#     -v /Users/${USER}/Downloads:/Downloads \
#     -v ${WRF_HYDRO_NWM_PATH}:/wrf_hydro_nwm \
#     -v ${WRF_HYDRO_PY_PATH}:/home/docker/wrf_hydro_py \
#     --volumes-from croton \
#     wrfhydro/dev:conda

# #######################################################
# cp -r /wrf_hydro_nwm /home/docker/wrf_hydro_nwm
# python

#######################################################
from boltons.iterutils import remap
import os
import pathlib
from pprint import pprint
import sys
import wrfhydropy

# ######################################################
# Model Section
# There are implied options here
# What is the argument? Are there more arguments?

#domain_top_path = '/home/docker/domain/croton_NY'
#source_path = '/home/docker/wrf_hydro_nwm/trunk/NDHMS'
USER = os.path.expanduser('~/')
domain_top_path = pathlib.PosixPath(USER + '/Downloads/croton_NY')
#domain_top_path = USER + '/domain/croton_NY'
source_path = pathlib.PosixPath(USER + '/WRF_Hydro/wrf_hydro_nwm_public/trunk/NDHMS')
#source_path = '/wrf_hydro_nwm/trunk/NDHMS/'

theDomain = wrfhydropy.WrfHydroDomain(
    domain_top_dir=domain_top_path,
    model_version='v1.2.1',
    domain_config='NWM'
)

theModel = wrfhydropy.WrfHydroModel(
    source_path,
    model_config='NWM'
)

theSetup = wrfhydropy.WrfHydroSetup(
    theModel,
    theDomain
)


#print('ensemble 0')
#e0=WrfHydroEnsembleSim([])
#print('len(e0): ',len(e0))
#e0.add_members(theSim)
#print('len(e0): ',len(e0))

print('ensemble 1')
e1=wrfhydropy.WrfHydroEnsembleSetup([theSetup])
print('len(e1): ',len(e1))
e1.members[0].description='the primal member'
print(e1.members_dict)

print('len(e1): ',len(e1))
print('e1.N: ',e1.N)
print(e1.members_dict)

e1.replicate_member(4)
e1.members[1].description='the first member'
print(e1.members_dict)

e1.members[1].number=400
print(e1.members_dict)

m2=e1.members[2]
m3=e1.members[3]

m2.domain.forcing_dir = \
    pathlib.PosixPath('/Users/jamesmcc/Downloads/domain/croton_NY/FORCING_FOO')
m2.model.source_dir = pathlib.PosixPath('foo')

## Does not recursively enter objects. All subobjects

def is_subobj(obj):
    try:
        _ = obj.__dict__
    except AttributeError:
        return False
    return True


# Get the member/setup object items which sub objects.
def get_sub_objs(obj):
    sub_obj_dict = {kk: is_subobj(obj[kk]) for (kk, vv) in obj.items()}
    return list(remap(sub_obj_dict, lambda p, k, v: v).keys())


def dictify(obj):
    the_dict = obj.__dict__
    sub_dicts = get_sub_objs(the_dict)
    for ss in sub_dicts:
        the_dict[ss] = dictify(the_dict[ss])
    return the_dict

mem0_ref_dict = dictify(e1.members[0])
mem1_ref_dict = dictify(e1.members[1])

diff1_new = DeepDiffEq(mem0_ref_dict, mem1_ref_dict, eq_types={pathlib.PosixPath})
diff1_old = DeepDiffEq(e1.members[0], e1.members[nn], eq_types={pathlib.PosixPath})

mem2_ref_dict = dictify(e1.members[2])
diff2_new = DeepDiffEq(mem0_ref_dict, mem2_ref_dict, eq_types={pathlib.PosixPath})
diff2_old = DeepDiffEq(e1.members[0], e1.members[nn], eq_types={pathlib.PosixPath})



all_diff_keys=set({})

# Verify refs

# Compare every member/setup object to the zeroth member/setup object.
if len(e1) == 1:
    return {}



def report_obj_diffs(obj1, obj2, eq_types):    
    # member/setup object differences
    diff0 = DeepDiffEq(e1.members[0], e1.members[nn], eq_types={pathlib.PosixPath})

    unexpected_diffs = set(diff0.keys()) - set(['values_changed'])
    if len(unexpected_diffs):
        unexpected_diffs1 = { uu: diff0[uu] for uu in list(unexpected_diffs) }
        raise ValueError(
            'Unexpected attribute differences between ensemble members:',
            unexpected_diffs1
        )

    diff1 = list(diff0['values_changed'].keys())
    all_diff_keys = all_diff_keys | set([ ss.replace('root.','') for ss in diff1 ])

    # Sub objects
    for ss in sub_objs
    DeepDiffEq(m1.domain, m2.domain, eq_types={pathlib.PosixPath})
    DeepDiffEq(m1.model,  m2.model,  eq_types={pathlib.PosixPath})


for nn in range(1, len(e1)):

    

# Come up with the visi
from boltons.iterutils import remap
import collections

def build_mem_refs_dict_list(bad_self):

    def build_component_types(ii):

        def visit(path, key, value):
            # r and super_path are from the calling scope
            if isinstance(value, collections.Container) or type(value) in exclude_types:
                return False
            if super_path is None:
                r[path + (key,)] = [ value ]
            else:
                r[(super_path,) + path + (key,)] = [ value ]
            return False

        r={}; super_path = None
        dum = remap(bad_self.members[ii].__dict__, visit=visit)
        ref_dict = r

        r={}; super_path = 'model'
        dum = remap(bad_self.members[ii].model.__dict__, visit=visit)
        ref_dict = { **ref_dict, **r}

        r={}; super_path = 'domain'
        dum = remap(bad_self.members[ii].domain.__dict__, visit=visit)
        ref_dict = { **ref_dict, **r}

        return(ref_dict)

    exclude_types = [wrfhydropy.WrfHydroModel, wrfhydropy.WrfHydroDomain]
    mems_refs_dict_list = [ build_component_types(mm) for mm, val in enumerate(bad_self.members) ]
    return(mems_refs_dict_list)

#e1.members[0].domain.forcing_dir = 'foobar'
#e1.members[1].domain.forcing_dir = 'barfoo'
mrdl = build_mem_refs_dict_list(e1)

from deepdiff import DeepDiff
dd=DeepDiff(mrdl[1], mrdl[2])

#e1.members

#print(e1.get_ens_attributes('hydro_namelist', 'nlastobs'))
#print(e1.get_ens_attributes('hydro_namelist', 'nlastobs'))



# Edit the forcing

# Edit the namelists




#theRun = theSim.run('/home/docker/testRun1', overwrite=True)






import pathlib
from deepdiff import DeepDiff

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
        super(DeepDiffEq, self)._DeepDiff__diff_obj(level, parents_ids=frozenset({}),
                                                    is_namedtuple=False)
