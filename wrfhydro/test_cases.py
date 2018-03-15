from pathlib import Path

class FundamentalTest(object):
    def __init__(self,candidate_sim,reference_sim,test_output_dir):
        self.candidate_sim = candidate_sim
        self.reference_sim = reference_sim
        self.test_output_dir = Path(test_output_dir)
        self.test_results = {}

    ###Compile questions
    def test_compile_candidate(self, compiler: str,
                               overwrite: bool = False,
                               compile_options: dict = None):

        compile_dir = self.test_output_dir.joinpath('compile_candidate')

        #Compile the model
        self.candidate_sim.compile(compiler,
                                   compile_dir,
                                   overwrite,
                                   compile_options)
        #Check compilation status
        if self.candidate_sim.compile.compile_log.return_code != 0:
            self.test_results.update({'compile_candidate':'fail'})

    def test_compile_reference(self, compiler: str = None,
                               overwrite: bool = False,
                               compile_options: dict = None):

        compile_dir = self.test_output_dir.joinpath('compile_reference')

        #If not specifying different compile options default to candidate options
        if compiler == None:
            compiler = self.candidate_sim.compiler
        if compile_options == None:
            compile_options = self.candidate_sim.compile_options

        #Compile the model
        self.reference_sim.compile(compiler,
                                   compile_dir,
                                   overwrite,
                                   compile_options)

        # Check compilation status
        if self.reference_sim.compile_log.return_code != 0:
            self.test_results.update({'compile_reference':'fail'})

    ###Run questions
    def test_run_candidate(self,num_cores: int = 2):

        #Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('run_candidate')

        #Run the simulation
        self.candidate_run = self.candidate_sim.run(simulation_dir,num_cores)

        #Check subprocess and model run status
        if self.candidate_run.run_log.return_code != 0 | self.candidate_run.run_status != 0:
            self.test_results.update({'run_candidate':'fail'})

    def test_run_reference(self,
                           num_cores: int = 2):

        #Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('run_reference')

        # If not specifying different run options default to candidate options
        if num_cores is None:
            num_cores = self.candidate_run.num_cores

        # Run the simulation
        self.reference_run = self.reference_sim.run(simulation_dir,
                                                    num_cores)

        # Check subprocess and model run status
        if self.reference_run.run_log.return_code != 0 | self.reference_run.run_status != 0:
            self.test_results.update({'run_reference': 'fail'})

    #Ncores question
    def test_ncores_candidate(self, num_cores: int = 1):

        # Set simulation directory
        simulation_dir = self.test_output_dir.joinpath('ncores_candidate')

        # Run the simulation
        self.candidate_run = self.candidate_sim.run(simulation_dir, num_cores)

        # Check subprocess and model run status
        if self.candidate_run.run_log.return_code != 0 | self.candidate_run.run_status != 0:
            self.test_results.update({'run_ncores': 'fail'})

        #Check against initial run
        for candidate_restart in self.candidate_run.hydro_restart:
            restart_name = candidate_restart.name




###Restart questions

###Regression questions
