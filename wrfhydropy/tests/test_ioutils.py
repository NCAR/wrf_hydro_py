# import pytest
# import numpy as np
# import pandas as pd
# import xarray as xr
#
# @pytest.fixture
# def ds_timeseries():
#     # Create a dummy dataset
#     vals_ts = np.random.randn(3)
#     reference_time = (['1984-10-13 00:00:00','1984-10-13 01:00:00','1984-10-13 02:00:00'])
#     times = pd.to_datetime(['1984-10-14 00:00:00','1984-10-14 01:00:00','1984-10-14 02:00:00'])
#     location = ['loc1', 'loc2', 'loc3']
#
#     for time in enumerate(times):
#     ds_ts = xr.Dataset({'var1': (('location','Time'), vals_ts)},
#                     {'Time': time,
#                      'location': location})
#
#     return ds_ts