from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='wrfhydropy',
    version='0.0.18',
    packages=find_packages(),
    package_data={'wrfhydropy': ['core/data/*']},
    url='https://github.com/NCAR/wrf_hydro_py',
    license='MIT',
    install_requires=[
        'boltons>=20.2.1',
        'bs4>=0.0.1',
        'dask[bag]>=2.14.0',
        'deepdiff==6.2.3',
        'f90nml>=1.2',
        'importlib-metadata==4.13.0',
        'netCDF4>=1.5.3',
        'numpy==1.22.4',
        'pandas>=1.3.5',
        'properscoring==0.1',
        'pytest>=5.4.1',
        'pytest-html>=3.0.0',
        'pytest-datadir-ng>=1.1.1',
        'pytest-lazy-fixture>=0.6.3',
        'requests>=2.23.0',
        'spotpy>=1.6.0',
        'urllib3==1.26.18',
        'xarray>=0.19'
    ],
    author='WRF-Hydro Team',
    author_email='@ucar.edu',
    description='API for the WRF-Hydro model',
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
)
