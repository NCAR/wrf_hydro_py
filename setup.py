from setuptools import setup

setup(
    name='wrfhydro',
    version='0.0.2',
    packages=['wrfhydro'],
    url='https://github.com/NCAR/wrf_hydro_py',
    license='MIT',
    install_requires=['pandas','f90nml','deepdiff','pathlib','xarray','datetime','pytest','pytest-datadir-ng'],
    author='Joe Mills',
    author_email='jmills@ucar.edu',
    description='Crude API for the WRF-Hydro model',
)
