import pathlib
import json
import pytest
import warnings
import subprocess

from wrfhydropy import Model

@pytest.fixture()
def model_dir(tmpdir):
    model_dir_path = pathlib.Path(tmpdir).joinpath('wrf_hydro_nwm_public/trunk/NDHMS')
    model_dir_path.mkdir(parents=True)

    # Make namelist patch files
    hrldas_namelist = {
        "base": {
            "noahlsm_offline": {
                "btr_option": 1,
                "canopy_stomatal_resistance_option": 1,
            },
            "wrf_hydro_offline": {
                "forc_typ": "NULL_specified_in_domain.json"
            }
        },
        "nwm_ana": {
            "noahlsm_offline": {},
            "wrf_hydro_offline": {}
        }
    }

    hydro_namelist = {
        "base": {
            "hydro_nlist": {
                "channel_option": 2,
                "chanobs_domain": 0,
                "chanrtswcrt": 1,
                "chrtout_domain": 1,
            },
            "nudging_nlist": {
                "maxagepairsbiaspersist": 3,
                "minnumpairsbiaspersist": 1,
            }
        },

        "nwm_ana": {
            "hydro_nlist": {},
            "nudging_nlist": {}
        }
    }

    json.dump(hrldas_namelist,model_dir_path.joinpath('hrldas_namelists.json').open('w'))
    json.dump(hydro_namelist,model_dir_path.joinpath('hydro_namelists.json').open('w'))

    compile_options = {
        "nwm": {
            "WRF_HYDRO": 1,
            "HYDRO_D": 0,
            "SPATIAL_SOIL": 1,
            "WRF_HYDRO_RAPID": 0,
            "WRFIO_NCD_LARGE_FILE_SUPPORT": 1,
            "NCEP_WCOSS": 0,
            "WRF_HYDRO_NUDGING": 1
        }
    }
    json.dump(compile_options,model_dir_path.joinpath('compile_options.json').open('w'))

    with model_dir_path.joinpath('.version').open('w') as f:
        f.write('v5.1.0')

    with model_dir_path.joinpath('configure').open('w') as f:
        f.write('#dummy configure')

    with model_dir_path.joinpath('./compile_offline_NoahMP.sh').open('w') as f:
        f.write('#dummy compile')

    subprocess.run(['chmod', '-R', '755', str(model_dir_path)])

    return(model_dir_path)


def test_model_init(model_dir):
    model = Model(source_dir=model_dir,
                  model_config='nwm_ana')
    assert type(model) == Model

def test_model_setenvar(model_dir,tmpdir):
    model = Model(source_dir=model_dir,
                  model_config='nwm_ana')

    assert model.compile_options == {
        "WRF_HYDRO": 1,
        "HYDRO_D": 0,
        "SPATIAL_SOIL": 1,
        "WRF_HYDRO_RAPID": 0,
        "WRFIO_NCD_LARGE_FILE_SUPPORT": 1,
        "NCEP_WCOSS": 0,
        "WRF_HYDRO_NUDGING": 1
    }

    compile_dir = pathlib.Path(tmpdir).joinpath('compile_dir_setenvar')

    # Compile will fail so trap axception and check compile artifacts instead
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.compile(compile_dir=compile_dir)
    except:
        pass

    with model_dir.joinpath('compile_options.sh').open('r') as f:
        assert f.read() == 'export WRF_HYDRO=1\n' \
                           'export HYDRO_D=0\n' \
                           'export SPATIAL_SOIL=1\n' \
                           'export WRF_HYDRO_RAPID=0\n' \
                           'export WRFIO_NCD_LARGE_FILE_SUPPORT=1\n' \
                           'export NCEP_WCOSS=0\n' \
                           'export WRF_HYDRO_NUDGING=1\n'

#model_dir=pathlib.Path('test')
def test_model_compile(model_dir,tmpdir):
    model = Model(source_dir=model_dir,
                  model_config='nwm_ana')

    compile_dir = pathlib.Path(tmpdir).joinpath('compile_dir_compile')

    # Compile will fail so trap exception and check compile artifacts instead
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.compile(compile_dir=compile_dir)
    except:
        pass

    assert model.compile_log.returncode == 0

def test_model_copyfiles(model_dir, tmpdir):

    model = Model(source_dir=model_dir,
                  model_config='nwm_ana')

    compile_dir = pathlib.Path(tmpdir).joinpath('compile_dir_compile')
    compile_dir.mkdir(parents=True)
    copy_dir = pathlib.Path(tmpdir).joinpath('compile_dir_copy')
    copy_dir.mkdir(parents=True)

    # Set table files and exe file attributes
    model.table_files = [compile_dir.joinpath('file1.tbl'),compile_dir.joinpath('file2.tbl')]
    model.wrf_hydro_exe = compile_dir.joinpath('wrf_hydro.exe')

    # Make fake run directory with files that would have been produced at compile
    with model.wrf_hydro_exe.open('w') as f:
        f.write('#dummy exe file')

    for file in model.table_files:
        with file.open('w') as f:
            f.write('#dummy table file')

    model.copy_files(str(copy_dir))

    actual_files_list = list(copy_dir.glob('*'))
    expected_files_list = list()
    for file in model.table_files:
        expected_files_list.append(file.name)
    expected_files_list.append(model.wrf_hydro_exe.name)

    for file in actual_files_list:
        assert file.name in expected_files_list
