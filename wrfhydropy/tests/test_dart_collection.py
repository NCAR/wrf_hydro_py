import os
import pathlib
import pytest
import shutil
import wrfhydropy
from .data import collection_data_download
from wrfhydropy.core.ioutils import md5

# The answer reprs are found here.
from .data.collection_data_answer_reprs import *

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))
dart_data_dir = test_dir / 'collection_data/dart_output'

# The data are found here.
collection_data_download.download()

#md5_answer_key = {ff.name: md5(ff) for ff in output_files}
md5_answer_key = {
    'all_analysis_ensemble.nc': 'a17f1c9c2f82f8643d7c990d60c355c2',
    'all_analysis_mean.nc': '429decdf4a096b63eed4a1469859e08f',
    'all_analysis_priorinf_mean.nc': '12ebe5703e84ac9e487d341a77c51170',
    'all_analysis_priorinf_sd.nc': '34845df8c5fbd93c8df1ff6c4e3cee8a',
    'all_analysis_sd.nc': '8d4bf08b48ef2c739e2f5645870638d4',
    'all_output_mean.nc': '1dcf81a346fcd127839e29560f10bc3d',
    'all_output_priorinf_mean.nc': 'b3ebb7949a4e20bc8db267f8053a3ac7',
    'all_output_priorinf_sd.nc': '3d2e1deab5999e9d261b9d85f8a6c166',
    'all_output_sd.nc': '9e83d86df1a42e99babcc53ccac1b638',
    'all_preassim_ensemble.nc': 'd02a920051b8b9ce07470c7c5b5bd46f',
    'all_preassim_mean.nc': '211b985720587f0aa91478dbb9a4f9c3',
    'all_preassim_priorinf_mean.nc': '927c1a2edce6c1bf448ba1ea6d921de0',
    'all_preassim_priorinf_sd.nc': '5ea07dd7fccd1c166a31b08759d214be',
    'all_preassim_sd.nc': 'd7e35fa9d8475b59fd9365dab897cb6e'}

@pytest.mark.parametrize('n_cores', [1,3])
def test_collect_dart_output(
    n_cores,
    tmpdir
):
    _ = wrfhydropy.collect_dart_output(
        run_dir=dart_data_dir, out_dir=tmpdir, n_cores=n_cores)

    output_files = sorted(pathlib.Path(tmpdir).glob('*.nc'))
    for ff in output_files:
        check_md5 = md5(ff)
        assert md5_answer_key[ff.name] == check_md5


