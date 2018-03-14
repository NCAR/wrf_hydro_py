# # import pytest
# #
# # class Fundamental(object):
# #
# #
# import pytest
# from utilities import *
# #This will return a WrfHydroModel object for use in the test
# # @pytest.fixture
# # def model(source_dir,compiler,compile_options):
# #     return WrfHydroModel(source_dir)
# #
# # def test_is_healthy(fruit):
# #      assert fruit == 'apple'
#
# @pytest.fixture
# def a_param():
#     return 1
#
# # ###This is a function from our module
# # def test1(x):
# #     return x + 1
#
# #This is a test function
# def test_test1(a_param):
#     assert a_param == 1
#
# #This is a test function
# # def test_test2(a_param):
# #     assert a_param == 2
#
# #And another using a function from our utilities module
# def test_restart(a_param):
#     assert diff_namelist(a_param,a_param) == 1