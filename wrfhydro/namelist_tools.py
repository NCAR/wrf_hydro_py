import f90nml
import json
from deepdiff import DeepDiff
#file = '/Volumes/d1/jmills/NCAR-WRF-Hydro/sixmile_docker_tests/hydro.namelist'
def namelist_to_json(file,output_file=''):

    #Default to same path and filename as input if unspecified
    if output_file == '':
        output_file = file + '.json'

    #Read in namelist to dict
    namelist=f90nml.read(file)

    #Write dict to json
    with open(output_file,mode = 'w') as f:
        namelist_json = json.dump(namelist, f,indent=4)
        f.close()


#file='/Volumes/d1/jmills/NCAR-WRF-Hydro/sixmile_docker_tests/hydro.namelist.json'
#patch_file='/Volumes/d1/jmills/NCAR-WRF-Hydro/sixmile_docker_tests/DOMAIN.json'
def domain_to_namelist(file,patch_file=None,output_file=''):
    #Default to same path and filename as input if unspecified
    if output_file == '':
        output_file = file + '.nml'

    #Read in the master json file
    namelist_json = json.load(open(file))

    if patch_file is not None:
        patch_json=json.load(open(patch_file))

        for sub_namelist in namelist_json:
            if sub_namelist in patch_json:
                namelist_json[sub_namelist].update(patch_json[sub_namelist])

    f90nml.write(namelist_json,output_file)

#namelist1='/Volumes/d1/jmills/NCAR-docker/wrf_hydro_docker/domains/croton_NY/domain/Gridded/hydro.namelist'
#namelist2='/Volumes/d1/jmills/NCAR-docker/wrf_hydro_docker/domains/croton_NY/domain/Reach/hydro.namelist'

import f90nml
import json
from deepdiff import DeepDiff
from pprint import pprint

def diff_namelist(namelist1,namelist2):
    def diff_namelist(namelist1, namelist2, **kwargs):
        # Read namelists into dicts
        namelist1 = f90nml.read(namelist1)
        namelist2 = f90nml.read(namelist2)
        # Diff the namelists
        differences = DeepDiff(namelist1, namelist2, ignore_order=True, **kwargs)
        pprint.pprint(differences)
        differences_dict = dict(differences)
        return (differences_dict)

    template_nlst = '/wrf_hydro_nwm/trunk/NDHMS/template/HYDRO/hydro.namelist'
    run_nlst = 'hydro.namelist'
    dd = diff_namelist(template_nlst, run_nlst)
    dd = diff_namelist(template_nlst, run_nlst, view='tree')
    pprint(dd)

# def main():
#     inputFile = argv[1]
#     outputFile = argv[2]
#     namelist_to_json(inputFile, outputFile)
#
# if __name__ == "__main__":
#     main()