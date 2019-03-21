from setuptools import find_packages, setup

setup(
    name='wrfhydropy',
    version='0.0.16',
    packages=find_packages(),
    package_data={'wrfhydropy': ['core/data/*']},
    url='https://github.com/NCAR/wrf_hydro_py',
    license='MIT',
    install_requires=[
        'pandas',
        'f90nml',
        'netCDF4',
        'deepdiff',
        'pathlib',
        'xarray',
        'datetime',
        'pytest',
        'pytest-html',
        'pytest-datadir-ng',
        'boltons',
        'bs4',
        'requests',
        'dask[bag]'
    ],
    author='Ryan Cabell',
    author_email='rcabell@ucar.edu',
    description='Crude API for the WRF-Hydro model',
)
