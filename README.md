![](https://ral.ucar.edu/sites/default/files/public/wrf_hydro_symbol_logo_2017_09_150pxby63px.png) WRF-HYDRO-PY ![](https://www.python.org/static/community_logos/python-powered-h-140x182.png)

[![Build Status](https://travis-ci.org/NCAR/wrf_hydro_py.svg?branch=master)](https://travis-ci.org/NCAR/wrf_hydro_py)
[![Coverage Status](https://coveralls.io/repos/github/NCAR/wrf_hydro_py/badge.svg?branch=master&service=github)](https://coveralls.io/github/NCAR/wrf_hydro_py?branch=master)
[![PyPI](https://img.shields.io/pypi/v/wrfhydropy.svg)](https://pypi.python.org/pypi/wrfhydropy)
[![GitHub release](https://img.shields.io/github/release/NCAR/wrf_hydro_py.svg)](https://github.com/NCAR/wrf_hydro_py/releases/latest)
[![Documentation Status](https://readthedocs.org/projects/wrfhydropy/badge/?version=latest)](https://wrfhydropy.readthedocs.io/en/latest/?badge=latest)

**IMPORTANT:** This package is in the very early stages of development and the package API may change at any time. It is not recommended that this package be used for significant work until version 0.1

## Description
*wrfhydropy* provides an end-to-end python interface to support reproducible research and construction of workflows involving the 
WRF-Hydro model. See the docs for an extended description of [what-and-why wrfhydropy](https://wrfhydropy.readthedocs.io/en/latest/what-and-why.html).

## Documentation
Documentation is available on-line through `help()` and via [readthedocs](https://wrfhydropy.readthedocs.io/en/latest/index.html). Documentation is a work in progress, please feel free to help improve the documentation or to make an issue when the docs are inaccurate!

## Contributing standards
Failure to adhere to contributing standards may result in your Pull Request being rejected.

### pep8speaks
All pull requests will be linted automatically by pep8speaks and reported as a comment into the pull request. The pep8speaks configuration is specified in .pep8speaks.yml. All pull requests must satisfy pep8speaks.  
Local linting can be performed after a `pip install` of [pycodestyle](https://github.com/PyCQA/pycodestyle). Pep8speaks linting reports also update with updated pull requests.

### Additional Style Guidelines
* Max line length: 100 chars.
* docstrings: [Google style](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
* All other guidance follows [Google style guide](https://google.github.io/styleguide/pyguide.html)
* General advice: [Hitchhiker's guide to code style](https://goo.gl/hqbW4r)

### Testing
All pull requests must pass automated testing (via TravisCI). Testing can be performed locally by running `pytest` in the `wrfhydropy/tests` directory. Currently, this testing relies on the [`nccp`](https://gitlab.com/remikz/nccmp) binary for comparing netcdf files. A docker container can be supplied for testing on request (and documentation will subsequently be placed here).

### Coverage
Testing concludes by submitting a request to [coveralls](https://coveralls.io/). This will automatically report changes of code coverage by the testing. Coverage should be maximized with every pull request. That is all new functions or classes must be accompanied by comprehensive additional unit/integration tests in the `wrf_hydro_py/wrfhydropy/tests` directory. Running coverage locally can be achieved by `pip` installing [`coverage`](https://pypi.org/project/coverage/) and [`pytest-cov`](https://pypi.org/project/pytest-cov/) following a process similar to the following: 
```
cd wrfhydropy/tests/
pytest --cov=wrfhydropy 
coverage html -d coverage_html
chrome coverage_html/index.html  # or your browser of choice
```
