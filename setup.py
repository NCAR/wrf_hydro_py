from setuptools import find_packages, setup

setup(
    name='wrfhydropy',
    version='0.0.4dev1',
    packages=find_packages(),
    package_data={'wrfhydropy': ['core/data/*']},
    url='https://github.com/NCAR/wrf_hydro_py',
    license='MIT',
    install_requires=['pandas',
                      'f90nml',
                      'deepdiff',
                      'pathlib',
                      'xarray',
                      'datetime',
                      'pytest',
                      'pytest-datadir-ng',
                      'boltons'],
    author='Joe Mills',
    author_email='jmills@ucar.edu',
    description='Crude API for the WRF-Hydro model',
)
