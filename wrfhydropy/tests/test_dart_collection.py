import os
import pathlib
import pytest
import shutil
import wrfhydropy
from .data import collection_data_download
from wrfhydropy.core.ioutils import md5
import xarray as xr

# The answer reprs are found here.
from .data.collection_data_answer_reprs import *

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))
dart_data_dir = test_dir / 'data/collection_data/dart_output'

# The data are found here.
collection_data_download.download()

# md5_answer_key = {ff.name: md5(ff) for ff in output_files}
md5_answer_key = {
    'all_analysis_ensemble.nc': 'ff2333f0a950cf966880bdc9904b2ed6',
    'all_analysis_mean.nc': 'f6389525d8d872b85eff6981342c35b4',
    'all_analysis_priorinf_mean.nc': 'ef1679c8561a9d494ca50f327dd76fd6',
    'all_analysis_priorinf_sd.nc': '1ca7b203293884c18823ae351d69d4e4',
    'all_analysis_sd.nc': 'c6ac39792a93a0191af299b3961b0df7',
    'all_output_mean.nc': '475c9b99686d3a371706b424c86e5f23',
    'all_output_priorinf_mean.nc': '085120dd550bb513ffd56a3cbb6c02cd',
    'all_output_priorinf_sd.nc': '69d0364eb3b8aa31a3ab6e8696326f7e',
    'all_output_sd.nc': 'c4bc7829cb9a543f8f7968e461d0c7be',
    'all_preassim_ensemble.nc': 'c54d961e8ce665a6e4515cc6ff2cd5f3',
    'all_preassim_mean.nc': '6381ca923e0dc379dd7ae334f88e60f0',
    'all_preassim_priorinf_mean.nc': 'd5f783e7869fa4ecafc47592b614fee1',
    'all_preassim_priorinf_sd.nc': 'a07bdbbadf6b2e5534d0d3e1c8ce03cd',
    'all_preassim_sd.nc': 'caf37d3193fa5b58e414eb07ff46411f'}


@pytest.mark.parametrize('n_cores', [1, 3])
def test_collect_dart_output(
    n_cores,
    tmpdir
):
    _ = wrfhydropy.collect_dart_output(
        run_dir=dart_data_dir,
        out_dir=tmpdir,
        n_cores=n_cores
    )

    output_files = sorted(pathlib.Path(tmpdir).glob('*.nc'))
    assert len(output_files) == len(md5_answer_key)

    for ff in output_files:
        check_md5 = md5(ff)
        assert md5_answer_key[ff.name] == check_md5

    chunk_dir = tmpdir / 'file_chunk_size'
    os.mkdir(str(chunk_dir))
    _ = wrfhydropy.collect_dart_output(
        run_dir=dart_data_dir,
        out_dir=chunk_dir,
        n_cores=n_cores,
        file_chunk_size=8  # 3 times with 3 members, need at least 4 to get this to work
    )

    chunk_files = sorted(pathlib.Path(chunk_dir).glob('*.nc'))
    for chunk, no_chunk in zip(chunk_files, output_files):
        ds_chunk = xr.open_dataset(chunk)
        ds_nochunk = xr.open_dataset(no_chunk)
        assert ds_chunk.equals(ds_nochunk)
