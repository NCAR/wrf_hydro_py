![](https://ral.ucar.edu/sites/default/files/public/wrf_hydro_symbol_logo_2017_09_150pxby63px.png) WRF-HYDRO

[![Build Status](https://travis-ci.org/NCAR/wrf_hydro_py.svg?branch=master)](https://travis-ci.org/NCAR/wrf_hydro_py)
[![Coverage Status](https://coveralls.io/repos/github/NCAR/wrf_hydro_py/badge.svg?branch=master)](https://coveralls.io/github/NCAR/wrf_hydro_py?branch=master)
[![PyPI](https://img.shields.io/pypi/v/wrfhydropy.svg)](https://pypi.python.org/pypi/wrfhydropy)
[![GitHub release](https://img.shields.io/github/release/NCAR/wrf_hydro_py.svg)](https://github.com/NCAR/wrf_hydro_py/releases/latest)
[![Documentation Status](https://readthedocs.org/projects/wrfhydropy/badge/?version=latest)](https://wrfhydropy.readthedocs.io/en/latest/?badge=latest)


**IMPORTANT:** This package is in the very early stages of development and the package API may change at any time. It is not recommended that this package be used for significant work until version 0.1

## Description
wrfhydrpy is a Python API for the WRF-Hydro modelling system. The goal of this project is to provide a clean, feature-rich, and unified API for interacting with the many components of the WRF-Hydro modelling system.

## Contributing standards
Failure to adhere to contributing standards may result in your Pull Request being rejected.
### Style Guidlines
* Max line length: 100 chars.
* docstrings: [Google style](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
* All other guidance follows [Google style guide](https://google.github.io/styleguide/pyguide.html)
### Testing
All new functions or classes must be accompanied by additional unit/integration tests in the `wrf_hydro_py/wrfhydropy/tests` directory.
If tests require complex or large data objects, those objects need to be created and placed in the `wrf_hydro_py/wrfhydropy/tests/data` directory.
Additionally, test data should be able to reproduced using the test script. See [test_WrfHydroClasses.py](https://github.com/NCAR/wrf_hydro_py/blob/master/wrfhydropy/tests/test_WrfHydroClasses.py) for an example.

## Documentation
Documentation will be forthcoming once the API becomes more stable.
