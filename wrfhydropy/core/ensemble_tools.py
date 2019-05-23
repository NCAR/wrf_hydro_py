from boltons.iterutils import remap
import copy
import datetime
from deepdiff.diff import DeepDiff
import os
import pathlib
import sys


def is_sub_obj(obj):
    """Test if an object is has a __dict__ (may not be the best definition of an object,
    but it works for classes in wrfhydropy)."""

    # If a dict, dont use __dict__
    if isinstance(obj, dict):
        return False

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
                 # exclude_regex_paths=set(),
                 exclude_types=set(),
                 # include_string_type_changes=False,
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
                         # exclude_regex_paths=set(),
                         exclude_types=set(),
                         # include_string_type_changes=False,
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

        super(DeepDiffEq, self)._DeepDiff__diff_obj(
            level,
            parents_ids=frozenset({}),
            is_namedtuple=False
        )


# def get_ens_file_last_restart_datetime(run_dir):
#     """Use the filesystem to probe the current ensemble time."""
#     run_dir = pathlib.Path(run_dir)
#     mem_dirs = sorted(run_dir.glob("member_*"))
#     hydro_last = [sorted(mm.glob('HYDRO_RST.*'))[-1].name for mm in mem_dirs]
#     if not all([hydro_last[0] == hh for hh in hydro_last]):
#         raise ValueError("Not all ensemble members at the same time (HYDRO_RST files).")
#     if len(sorted(mem_dirs[0].glob('RESTART.*'))):
#         lsm_last = [sorted(mm.glob('RESTART.*'))[-1] for mm in mem_dirs]
#         if not all([lsm_last[0] == ll for ll in lsm_last]):
#             raise ValueError("Not all ensemble members at the same time (RESTART files).")

#     ens_time = datetime.datetime.strptime(
#         str(hydro_last[0]).split('_RST.')[-1],
#         '%Y-%m-%d_%H:%M_DOMAIN1'
#     )
#     return ens_time


def get_ens_dotfile_end_datetime(run_dir):
    """Use the the .model_end_time files to get the current ensemble time."""
    run_dir = pathlib.Path(run_dir)
    mem_dirs = sorted(run_dir.glob("member_*"))

    def read_dot_file(file):
        with open(file) as f:
            content = f.readline()
        return datetime.datetime.strptime(content, '%Y-%m-%d %H:%M:%S')

    end_times = [read_dot_file(mm / '.model_end_time') for mm in mem_dirs]
    if not all([end_times[0] == ee for ee in end_times]):
        raise ValueError("Not all ensemble members at the same time (HYDRO_RST files).")

    return end_times[0]


def mute():
    """A initializer for multiprocessing.Pool to keep the processes quiet."""
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
