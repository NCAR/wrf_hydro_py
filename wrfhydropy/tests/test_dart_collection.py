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

output_ds_reprs = {
    'all_analysis_ensemble.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, member: 3, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\n  * member   (member) int64 1 2 3\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, member, links) float32 0.83186156 ... -31.114584',
    'all_analysis_mean.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.69106185 0.00013627816 ... 1.2680831\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  analysis ensemble mean\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              analysis ensemble mean',
    'all_analysis_priorinf_mean.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 1.0 1.0 1.0 ... 1.9079121 4.5559134 4.083666\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  analysis prior inflation mean\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              analysis prior inflation mean',
    'all_analysis_priorinf_sd.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.6 0.6 0.6 ... 6.8881593 8.627698 8.030106\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  analysis prior inflation sd\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              analysis prior inflation sd',
    'all_analysis_sd.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.26237342 2.8160735e-05 ... 58.97208\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  analysis ensemble sd\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              analysis ensemble sd',
    'all_output_mean.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.69106185 0.00013627816 ... 1.2680831\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  output ensemble mean\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              output ensemble mean[clamped]',
    'all_output_priorinf_mean.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 1.0 1.0 1.0 ... 1.9079121 4.5559134 4.083666\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  output prior inflation mean\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              output prior inflation mean',
    'all_output_priorinf_sd.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.6 0.6 0.6 ... 6.8881593 8.627698 8.030106\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  output prior inflation sd\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              output prior inflation sd',
    'all_output_sd.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.26237342 2.8160735e-05 ... 58.97208\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  output ensemble sd\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              output ensemble sd',
    'all_preassim_ensemble.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, member: 3, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\n  * member   (member) int64 1 2 3\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, member, links) float32 0.83186156 ... -14.374111',
    'all_preassim_mean.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.69106185 0.00013627816 ... 14.919881\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  preassim ensemble mean\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              preassim ensemble mean',
    'all_preassim_priorinf_mean.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 1.0 1.0 1.0 ... 1.8894768 4.505077 4.0609803\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  preassim prior inflation mean\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              preassim prior inflation mean',
    'all_preassim_priorinf_sd.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.6 0.6 0.6 ... 7.8793244 9.292467 9.631818\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  preassim prior inflation sd\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              preassim prior inflation sd',
    'all_preassim_sd.nc':
        '<xarray.Dataset>\nDimensions:  (links: 1642, time: 3)\nCoordinates:\n  * time     (time) datetime64[ns] 2018-09-19 ... 2018-09-19T02:00:00\nDimensions without coordinates: links\nData variables:\n    qlink1   (time, links) float32 0.26237342 2.8160735e-05 ... 61.26723\nAttributes:\n    HYDRO_filename:         /glade/work/jamesmcc/domains/private/florence_933...\n    Restart_Time:           2018-08-01_00:00:00\n    Since_Date:             2009-10-01_00:00:00\n    DTCT:                   300.0\n    channel_only:           0\n    channelBucket_only:     0\n    DART_file_information:  preassim ensemble sd\n    DART_creation_date:     YYYY MM DD HH MM SS = 2019 10 15 17 37 49\n    DART_source:            $URL: https://svn-dares-dart.cgd.ucar.edu/DART/br...\n    DART_revision:          $Revision: 12865 $\n    DART_revdate:           $Date: 2018-09-28 08:50:47 -0600 (Fri, 28 Sep 201...\n    DART_clamp_qlink1:      min_val =   0.00000000000 , max val =            ...\n    DART_note:              preassim ensemble sd'}

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
    assert len(output_files) == len(output_ds_reprs)

    for ff in output_files:
        assert output_ds_reprs[str(ff.name)] == repr(xr.open_dataset(ff).load())

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
