import pathlib

from wrfhydropy import Domain, WrfHydroStatic, WrfHydroTs


def test_domain_init(domain_dir):
    domain = Domain(domain_top_dir=domain_dir,
                    domain_config='nwm_ana',
                    compatible_version='v5.0.1')
    assert type(domain) == Domain

def test_domain_namelists(domain_dir):
    domain = Domain(domain_top_dir=domain_dir,
                    domain_config='nwm_ana',
                    compatible_version='v5.0.1')

    # Check namelist configuration
    assert domain.hydro_namelist_patches == {
        'hydro_nlist':
            {'geo_static_flnm': './NWM/DOMAIN/geo_em.d01.nc',
             'restart_file': './NWM/RESTART/HYDRO_RST.2011-08-26_00:00_DOMAIN1',
             'aggfactrt': 4,
             'udmp_opt': 1},
        'nudging_nlist': {
            'nudginglastobsfile': './NWM/RESTART/nudgingLastObs.2011-08-26_00:00:00.nc'}
    }, 'hydro_namelist JSONNamelist did not return expected dictionary ' \
       'for config nwm_ana'

    assert domain.hrldas_namelist_patches == {
        'noahlsm_offline':
            {'hrldas_setup_file': './NWM/DOMAIN/wrfinput_d01.nc',
             'restart_filename_requested': './NWM/RESTART/RESTART.2011082600_DOMAIN1',
             'indir': './FORCING'},
        'wrf_hydro_offline': {'forc_typ': 1}}, 'hrldas_namelist JSONNamelist did not return ' \
                                               'expected dictionary for config nwm_ana'

def test_domain_filepaths(domain_dir):
    domain = Domain(domain_top_dir=domain_dir,
                    domain_config='nwm_ana',
                    compatible_version='v5.0.1')
    assert type(domain.hydro_files) == list and type(domain.hydro_files[0]) == WrfHydroStatic, \
        'hydro files not imported correctly'
    assert type(domain.lsm_files) == list and type(domain.lsm_files[0]) == WrfHydroStatic, \
        'lsm files not imported correctly'
    assert type(domain.forcing_data) == WrfHydroTs and len(domain.forcing_data) == 3, \
        'forcing files not imported correctly'

def test_domain_copyfiles(tmpdir,domain_dir):
    domain = Domain(domain_top_dir=str(domain_dir),
                    domain_config='nwm_ana',
                    compatible_version='v5.0.1')
    tmpdir = pathlib.Path(tmpdir)
    copy_dir = tmpdir.joinpath('domain_copy_test')
    domain.copy_files(str(copy_dir))

    namelist_files = []
    for item in domain.hydro_files:
        # Make relative for ease of comparison
        relative_path = item.absolute().relative_to(domain_dir.absolute())
        namelist_files.append(str(relative_path))
    for item in domain.lsm_files:
        relative_path = item.absolute().relative_to(domain_dir.absolute())
        namelist_files.append(str(relative_path))
    for item in domain.nudging_files:
        relative_path = item.absolute().relative_to(domain_dir.absolute())
        namelist_files.append(str(relative_path))

    copied_files = []
    for file in list(copy_dir.rglob('*')):
        # Get path as relative so that can be compared to namelist paths
        relative_path = file.absolute().relative_to(copy_dir.absolute())
        copied_files.append(str(relative_path))

    # Manually check that FORCING got copied, rglob is ignoring contents of symlinked dir
    assert 'FORCING' in copied_files, 'Forcing data not copied'

    # Check the rest of the files
    for file in namelist_files:
        if file not in ['FORCING']:
            assert file in copied_files, 'file ' + file + ' was not copied successfully'

    # Check the special case of RESTARTS which should be symlinked into main dir
    restart_file_patterns = ['*RESTART*','*HYDRO_RST*','*nudgingLastObs*']
    for file_pattern in restart_file_patterns:
        assert len(list(copy_dir.glob(file_pattern))) == 1, \
            'restart file ' + file_pattern + ' not copied'

