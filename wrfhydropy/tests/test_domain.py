import xarray as xr
import pytest
import pathlib
import netCDF4

@pytest.fixture()
def domain_dir(tmpdir, ds_1d):
    domain_dir_path = pathlib.Path(tmpdir).joinpath('example_case/DOMAIN')
    domain_dir_path.mkdir(parents=True)

    # Make a list of filenames to create
    domain_file_names = ['Fulldom_hires.nc',
                         'Route_Link.nc',
                         'soil_properties.nc',
                         'GEOGRID_LDASOUT_Spatial_Metadata.nc',
                         'geo_em.d01.nc',
                         'spatialweights.nc',
                         'GWBUCKPARM.nc',
                         'hydro2dtbl.nc',
                         'wrfinput_d01.nc',
                         'LAKEPARM.nc',
                         'nudgingParams.nc']

    for file in domain_file_names:
        file_path = domain_dir_path.joinpath(file)
        ds_1d.to_netcdf(str(file_path))

    return domain_dir_path

def test_domain(domain_dir):
    xr.open_dataset(domain_dir.joinpath('Fulldom_hires.nc'))