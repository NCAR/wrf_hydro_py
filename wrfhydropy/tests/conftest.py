import pytest
import xarray as xr
import numpy as np
import pandas as pd
import pathlib


@pytest.fixture(scope="session")
def ds_1d():
    # Create a dummy dataset
    vals_1d = np.random.randn(3)
    time = pd.to_datetime('1984-10-14')
    location = ['loc1', 'loc2', 'loc3']

    ds_1d = xr.Dataset({'var1': (('location'), vals_1d)},
                    {'Time': time, 'location': location})

    return ds_1d


@pytest.fixture(scope="session")
def ds_2d():
    x = [10,11,12]
    y = [101,102,103]
    vals_2d = np.random.randn(3,3)
    time = pd.to_datetime('1984-10-14')

    ds_2d = xr.Dataset({'var1': (('x','y'), vals_2d)},
                    {'Time': time, 'x': x,'y':y})

    return ds_2d


