class Machine(object):
    """An object that specifies run time particulars for a machine"""
    def __init__(self, model_exe_cmd: str = None, entry_cmd: dict = None, exit_cmd: dict = None):
        self.entry_cmd = entry_cmd
        self.exe_cmd = model_exe_cmd
        self.exit_cmd = exit_cmd