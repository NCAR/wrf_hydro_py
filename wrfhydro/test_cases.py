class FundamentalTest(object):
    def __init__(self,candidate_sim,reference_sim):
        self.candidate_sim = candidate_sim
        self.reference_sim = reference_sim

    ###Compile questions
    def test_compile_candidate(self, compiler: str,
                compile_dir: str = None,
                overwrite: bool = False,
                compile_options: dict = None):

        self.candidate_sim.compile(compiler,
                                   compile_dir,
                                   overwrite,
                                   compile_options)

        if self.candidate_sim.compile.compile_log.return_code != 0:
            exit(1)
            # TODO - Dont want to exit, better to print an error and return using try:

    def test_compile_reference(self, compiler: str = None,
                compile_dir: str = None,
                overwrite: bool = False,
                compile_options: dict = None):

        if compiler == None:
            compiler = self.candidate_sim.compiler
        if compile_dir == None:
            compile_dir = self.candidate_sim.compile_dir
        if compile_options == None:
            compile_options = self.candidate_sim.compile_options

        self.reference_sim.compile(compiler,
                                   compile_dir,
                                   overwrite,
                                   compile_options)

    ###Run questions

    ###NCores questions

    ###Restart questions

    ###Regression questions


        if self.reference_sim.compile.compile_log.return_code != 0:
            exit(1)
            # TODO - Dont want to exit, better to print an error and return using try: