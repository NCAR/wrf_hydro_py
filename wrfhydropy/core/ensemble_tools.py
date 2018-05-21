from boltons.iterutils import remap
import copy
from deepdiff.diff import DeepDiff


def is_sub_obj(obj):
    """Test if an object has a __dict__ (may not be the best definition of an object, 
    but it works for classes in wrfhydropy)."""
    try:
        _ = obj.__dict__
    except AttributeError:
        return False
    return True


def get_sub_objs(obj):
    """Identify which attributes of an object are objects with __dicts__."""
    sub_obj_dict = {kk: is_sub_obj(obj[kk]) for (kk, vv) in obj.items()}
    return list(remap(sub_obj_dict, lambda p, k, v: v).keys())


def dictify(obj):
    """Recursively transform deepcopy sub __dicts__ of an object into dicts for dictionary
    traversal of a deepcopy of the object."""
    the_dict = copy.deepcopy(obj.__dict__)
    sub_dicts = get_sub_objs(the_dict)
    for ss in sub_dicts:
        the_dict[ss] = dictify(the_dict[ss])
    return the_dict


class DeepDiffEq(DeepDiff):
    """Extend Deep Diff to handle __eq__ for specified types."""
    def __init__(self,
                 t1,
                 t2,
                 eq_types,
                 ignore_order=False,
                 report_repetition=False,
                 significant_digits=None,
                 exclude_paths=set(),
                 #exclude_regex_paths=set(),
                 exclude_types=set(),
                 #include_string_type_changes=False,
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
                         #exclude_regex_paths=set(),
                         exclude_types=set(),
                         #include_string_type_changes=False,
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


