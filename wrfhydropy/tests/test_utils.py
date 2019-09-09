import os
import pathlib
import pytest

from wrfhydropy.util.xrcmp import xrcmp
from wrfhydropy.util.xrnan import xrnan

test_dir = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))
collection_data_dir = test_dir / 'data/collection_data/simulation'
nan_na_data_dir = test_dir / 'data/nan_na_data'


@pytest.mark.parametrize(
    ['filename'],
    [
        ('201108260100.CHANOBS_DOMAIN1',),
        ('201108260100.CHRTOUT_DOMAIN1',),
        ('201108260100.GWOUT_DOMAIN1',),
        ('201108260100.LAKEOUT_DOMAIN1',),
        ('201108260100.LDASOUT_DOMAIN1',),
        ('201108260100.LSMOUT_DOMAIN',),
        ('201108260100.RTOUT_DOMAIN1',),
        ('HYDRO_RST.2011-08-26_01:00_DOMAIN1',),
        ('nudgingLastObs.2011-08-26_01:00:00.nc',),
        ('RESTART.2011082601_DOMAIN1',),
    ],
    ids=[
        'xrcmp-equals-CHANOBS',
        'xrcmp-equals-CHRTOUT',
        'xrcmp-equals-GWOUT',
        'xrcmp-equals-LAKEOUT',
        'xrcmp-equals-LDASOUT',
        'xrcmp-equals-LSMOUT',
        'xrcmp-equals-RTOUT',
        'xrcmp-equals-HYDRO_RST',
        'xrcmp-equals-nudginglastobs',
        'xrcmp-equals-RESTART',
    ]
)
def test_xrcmp_eq(filename, tmpdir):
    file_path = test_dir.joinpath(collection_data_dir)
    the_file = file_path.joinpath(filename)
    log_file = pathlib.Path(tmpdir).joinpath('log.txt')
    result = xrcmp(the_file, the_file, log_file)
    assert result == 0


@pytest.mark.parametrize(
    ['filename1', 'filename2'],
    [
        ('201108260100.CHANOBS_DOMAIN1', '201108260200.CHANOBS_DOMAIN1'),
        ('201108260100.CHRTOUT_DOMAIN1', '201108260200.CHRTOUT_DOMAIN1'),
        ('201108260100.GWOUT_DOMAIN1', '201108260200.GWOUT_DOMAIN1'),
        ('201108260100.LAKEOUT_DOMAIN1', '201108260200.LAKEOUT_DOMAIN1'),
        # ('201108260100.LDASOUT_DOMAIN1', '201108260200.LDASOUT_DOMAIN1'),
        # ('201108260100.LSMOUT_DOMAIN', '201108260200.LSMOUT_DOMAIN'),
        # ('201108260100.RTOUT_DOMAIN1', '201108260200.RTOUT_DOMAIN1'),
        ('HYDRO_RST.2011-08-26_01:00_DOMAIN1', 'HYDRO_RST.2011-08-26_02:00_DOMAIN1'),
        # ('nudgingLastObs.2011-08-26_01:00:00.nc', 'nudgingLastObs.2011-08-26_02:00:00.nc'),
        # ('RESTART.2011082601_DOMAIN1', 'RESTART.2011082602_DOMAIN1'),
    ],
    ids=[
        'xrcmp-unequal-CHANOBS',
        'xrcmp-unequal-CHRTOUT',
        'xrcmp-unequal-GWOUT',
        'xrcmp-unequal-LAKEOUT',
        # 'xrcmp-unequal-LDASOUT',
        # 'xrcmp-unequal-LSMOUT',
        # 'xrcmp-unequal-RTOUT',
        'xrcmp-unequal-HYDRO_RST',
        # 'xrcmp-unequal-nudginglastobs', # identical data is the problem
        # 'xrcmp-unequal-RESTART',
    ]
)
def test_xrcmp_uneq(filename1, filename2, tmpdir):
    file_path = test_dir.joinpath(collection_data_dir)
    the_file1 = file_path.joinpath(filename1)
    the_file2 = file_path.joinpath(filename2)
    log_file = pathlib.Path(tmpdir).joinpath('log.txt')
    result = xrcmp(the_file1, the_file2, log_file)
    assert result == 1


@pytest.mark.parametrize(
    ['filename', 'expected'],
    [
        ('201108260200.CHANOBS_DOMAIN1', None),
        ('201108260200.CHRTOUT_DOMAIN1', None),
        ('201108260200.GWOUT_DOMAIN1', None),
        ('201108260200.LAKEOUT_DOMAIN1', None),
        ('201108260200.LDASOUT_DOMAIN1', None),
        ('201108260200.LSMOUT_DOMAIN', None),
        ('201108260200.RTOUT_DOMAIN1', None),
        ('HYDRO_RST.2011-08-26_02:00_DOMAIN1', None),
        ('nudgingLastObs.2011-08-26_02:00:00.nc', None),
        ('RESTART.2011082602_DOMAIN1', None),
    ],
    ids=[
        'xrnan-CHANOBS',
        'xrnan-CHRTOUT',
        'xrnan-GWOUT',
        'xrnan-LAKEOUT',
        'xrnan-LDASOUT',
        'xrnan-LSMOUT',
        'xrnan-RTOUT',
        'xrnan-HYDRO_RST',
        'xrnan-nudginglastobs',
        'xrnan-RESTART',
    ]
)
def test_xrnan_none(filename, expected, tmpdir):
    # Perhaps this test is extraneous?
    # Right now only have real data on hand without NaNs.
    file_path = test_dir.joinpath(collection_data_dir)
    the_file = file_path.joinpath(filename)
    result = xrnan(the_file)
    assert result is expected


@pytest.mark.parametrize(
    ['filename', 'expected'],
    [
        ('fill_value.nc', 'None'),
        ('nan_fill.nc', "{'vars': ['some_var']}"),
        ('nan_value.nc', "{'vars': ['some_var']}"),
        ('value_value.nc', 'None'),
    ],
    ids=[
        'xrnan-fill_value',
        'xrnan-nan_fill',
        'xrnan-nan_value',
        'xrnan-value_value',
    ]
)
def test_xrnan_matrix(filename, expected, tmpdir):
    file_path = test_dir.joinpath(nan_na_data_dir)
    the_file = file_path.joinpath(filename)
    result = xrnan(the_file)
    assert repr(result) == expected
