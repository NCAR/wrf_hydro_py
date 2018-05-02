from setuptools import find_packages, setup

setup(
    name='wrfhydropy',
    version='0.0.3dev0',
    packages=find_packages(),
    url='https://github.com/NCAR/wrf_hydro_py',
    license='MIT',
    install_requires=['pandas','f90nml','deepdiff','pathlib','xarray','datetime','pytest','pytest-datadir-ng'],
    author='Joe Mills',
    author_email='jmills@ucar.edu',
    description='Crude API for the WRF-Hydro model',
)
