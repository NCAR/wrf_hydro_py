#######################################################
# Before Docker
WRF_HYDRO_NWM_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_nwm_myFork
WRF_HYDRO_PY_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_py
WRF_HYDRO_TESTS_PATH=/Users/${USER}/WRF_Hydro/wrf_hydro_tests

docker create --name croton wrfhydro/domains:croton_NY
## The complement when youre done with it:
## docker rm -v sixmile_channel-only_test

docker run -it \
    -e GITHUB_AUTHTOKEN=$GITHUB_AUTHTOKEN \
    -e GITHUB_USERNAME=$GITHUB_USERNAME \
    -e WRF_HYDRO_TESTS_USER_SPEC=/home/docker/wrf_hydro_tests/template_user_spec.yaml \
    -v ${WRF_HYDRO_NWM_PATH}:/wrf_hydro_nwm \
    -v ${WRF_HYDRO_PY_PATH}:/home/docker/wrf_hydro_py \
    -v ${WRF_HYDRO_TESTS_PATH}:/home/docker/wrf_hydro_tests \
    --volumes-from croton \
    wrfhydro/dev:conda

#######################################################
# Inside docker (before python)
cd ~/wrf_hydro_tests/
pip install boltons

## run and check the logs
python3 take_test.py ; echo; echo ---------------------; echo; cat example.log ; rm example.log 


