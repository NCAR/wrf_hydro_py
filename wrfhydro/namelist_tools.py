import f90nml
from deepdiff import DeepDiff
from pprint import pprint

def diff_namelist(namelist1,namelist2):
    def diff_namelist(namelist1, namelist2, **kwargs):
        # Read namelists into dicts
        namelist1 = f90nml.read(namelist1)
        namelist2 = f90nml.read(namelist2)
        # Diff the namelists
        differences = DeepDiff(namelist1, namelist2, ignore_order=True, **kwargs)
        pprint(differences)
        differences_dict = dict(differences)
        return (differences_dict)

    template_nlst = '/wrf_hydro_nwm/trunk/NDHMS/template/HYDRO/hydro.namelist'
    run_nlst = 'hydro.namelist'
    dd = diff_namelist(template_nlst, run_nlst, view='tree')
    pprint(dd)

