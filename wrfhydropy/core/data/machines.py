# # Note: All other imports for individual schedulers should be done in the respective scheduler
# # class functions so that imports can be isolated to relevant schedulers
#
# from abc import ABC, abstractmethod
#
# class Machine(ABC):
#     def __init__(self):
#         super().__init__()
#
#     @abstractmethod
#     def configure(self):
#         pass
#
# class CheyenneMachine(Machine):
#     """An object to be used as the machine for Cheyenne."""
#     def __init__(self):
#         self.modules = {'ifort':['intel/16.0.3','ncarenv/1.2','ncarcompilers/0.4.1','mpt/2.15f',
#                                  'netcdf/4.4.1'],
#                         'gfort':['gnu/7.1.0','ncarenv/1.2','ncarcompilers/0.4.1','mpt/2.15',
#                                  'netcdf/4.4.1.1']}
