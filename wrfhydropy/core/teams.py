import itertools
import math
import operator
import os
import pathlib
import pickle
import wrfhydropy

def parallel_teams_run(arg_dict):
    """
    Parallelizable function to run simulations across nodes.
    On the master node, python runs multiprocessing. Each separate process
    is a "team". Multiprocessing makes MPI calls with a specific syntax to
    run the MPI executable on specfific (potentially other) nodes. This
    provides 2 levels of parallelism.

    This function is called (in parallel) once for each team by
    multiprocessing. Each team runs its set of simulations sequentially but
    each simulation it runs is parallel via MPI.

    Input:
        arg_dict:
            arg_dict == {
               'obj_name'   : string, either "member" or "cast" (or some other
                              object), matches the object name used in the
                              team_dict below (first argument)
               'compose_dir': <pathlib.Path absolute path to the cycle top
                              level/compse, dir where the individual cycle dirs
                              are found>,
               'team_dict'  : <dict: the information needed for the team, see
                              below>
            }
        where:
            team_dict == {
                object_name: key/name is either 'members' or 'casts', the value
                             is a <list: groups of simulations to run either
                              simulation objects or their run_dirs>
                'nodes'    : <list: the nodes previously parsed from something
                               like$PBS_NODE_FILE>,
                'exe_cmd'  : <string: the MPI-specific model invokation
                             command>,
                'env'      : <dict: containging the environment in which to run
                             the cmds, may be None or 'None'>
            }

        The 'exe_cmd' is a form of invocation for the distribution of MPI to be
        used. For openmpi, for example for OpenMPI, this is
            exe_cmd: 'mpirun --host {hostnames} -np {nproc} {cmd}'
        The variables in brackets are expanded by internal variables. The
        'exe_cmd' command substitutes the wrfhydropy of 'wrf_hydro.exe'
        convention for {cmd}.
        The {nproc} argument is the length of the list passed in the nodes
        argument, and the {hostnames} are the comma separated arguments in that
        list.

        The "entry_cmd" and "exit_cmd" ARE TAKEN FROM THE JOB object.
          1) can be semicolon-separated commands
          2) where these are run depends on MPI. OpenMPI, for example, handles
            these on the same processor set as the model runs.

    Notes:
        Currently this is working/tested with openmpi and intel mpi.
        MPT requires MPI_SHEPERD env variable and it's performance is not
            satisfactory so far.
    """

    obj_name = arg_dict['obj_name']
    compose_dir = arg_dict['compose_dir']
    team_dict = arg_dict['team_dict']

    exit_statuses = {}
    for obj in team_dict[obj_name]:
        if type(obj) is str:
            os.chdir(str(pathlib.Path(compose_dir) / obj))
        else:
            os.chdir(str(pathlib.Path(compose_dir) / obj.run_dir))

        object_pkl_file = "WrfHydroSim.pkl"
        object_pkl = pickle.load(open(object_pkl_file, "rb"))
        job = object_pkl.jobs[0]

        if job._entry_cmd is not None:
            entry_cmds = job._entry_cmd.split(';')
            new_entry_cmd = []
            for cmd in entry_cmds:
                if 'mpirun' not in cmd:
                    new_entry_cmd.append(
                        # Switch out the ./wrf_hydro.exe cmd with each command.
                        team_dict['exe_cmd'].format(
                            **{
                                'cmd': cmd,
                                'hostname': team_dict['nodes'][0],  # only use one task
                                'nproc': 1
                            }
                        )
                    )
                else:
                    new_entry_cmd.append(cmd)
            job._entry_cmd = '; '.join(new_entry_cmd)

        if job._exit_cmd is not None:
            exit_cmds = job._exit_cmd.split(';')
            new_exit_cmd = []
            for cmd in exit_cmds:
                if 'mpirun' not in cmd:
                    new_exit_cmd.append(
                        # Switch out the ./wrf_hydro.exe cmd with each command.
                        team_dict['exe_cmd'].format(
                            **{
                                'cmd': cmd,
                                'hostname': team_dict['nodes'][0],  # only use one task
                                'nproc': 1
                            }
                        )
                    )
                else:
                    new_exit_cmd.append(cmd)
            job._exit_cmd = '; '.join(new_exit_cmd)

        job._exe_cmd = team_dict['exe_cmd'].format(
            **{
                'cmd': './wrf_hydro.exe',
                'hostname': ','.join(team_dict['nodes']),
                'nproc': len(team_dict['nodes'])
            }
        )

        object_pkl.pickle(object_pkl_file)
        object_pkl.run(env=team_dict['env'])

        exit_statuses.update({obj: object_pkl.jobs[0].exit_status})

    return exit_statuses


def assign_teams(
    obj,
    teams_exe_cmd: str,
    teams_exe_cmd_nproc: int,
    teams_node_file: dict = None,
    env: dict = None    
) -> dict:
    """
    Assign teams for parallel runs across nodes.
    Inputs:
        obj: The ensemble or cycle object, containin lists of members or casts
            to be run.
        teams_exe_cmd: str, The mpi-specific syntax needed. For example
            'mpirun --host {hostname} -np {nproc} {cmd}'
        teams_exe_cmd_nproc: int, The number of cores per model/wrf_hydro
            simulation to be run.
        teams_node_file: dict = None, Optional file that acts like a node file.
            It is not currently implemented but the key specifies the scheduler
            format that the file follows. An example pbs node file is in
            tests/data and this argument is used here to test without a sched.
        env: dict = None, optional envionment to pass to the run.
    Outputs:
        dict: the teams_dict to be used by parallel_teams_run. See requirements
            above.
    """
    if 'casts' in dir(obj): 
        object_list = obj.casts
        object_name = 'casts'
    elif 'members' in dir(obj):
        object_list = obj.members
        object_name = 'members'

    if isinstance(teams_node_file, dict):
        if 'pbs' in teams_node_file.keys():
            pbs_node_file = teams_node_file['pbs']
    else:
        pbs_node_file = os.environ.get('PBS_NODEFILE')
        # Merge other schduler files here.

    n_runs = len(object_list)

    if pbs_node_file is not None:
        pbs_nodes = []
        # TODO: comment the target format here.
        with open(pbs_node_file, 'r') as infile:
            for line in infile:
                pbs_nodes.append(line.rstrip())

        n_total_processors = len(pbs_nodes) # less may be used.
        n_teams = min(math.floor(len(pbs_nodes) / teams_exe_cmd_nproc), n_runs)
        teams_dict = {}

        # Map the objects on to the teams (this seems overly complicated, should prob
        # consider using pandas:

        # If the cast/ensemble is still in memory, this looks different.
        if isinstance(object_list[0], wrfhydropy.Simulation):
            object_dirs = [oo.run_dir for oo in object_list]
        else:
            object_dirs = object_list
        
        object_teams = [the_object % n_teams for the_object in range(n_runs)]
        object_team_seq = [ [dir, team] for dir,team in zip(object_dirs, object_teams)]
        object_team_seq.sort(key = operator.itemgetter(1))
        team_groups = itertools.groupby(object_team_seq, operator.itemgetter(1))
        team_objects = [[item[0] for item in data] for (key, data) in team_groups]

        # Map the nodes on to the teams
        # Homogonization step here to avoid communication across nodes...
        # Sorting necessary for testing.
        unique_nodes = sorted([node.split('.')[0] for node in list(set(pbs_nodes))])
        print("\n*** Team " + object_name + ' ***')
        print("Running on nodes: " + ', '.join(unique_nodes))
        del pbs_nodes
        pbs_nodes = []
        for i_team in range(n_teams):
            pbs_nodes = pbs_nodes + (
                [unique_nodes[i_team % len(unique_nodes)]] * teams_exe_cmd_nproc)
        node_teams = [the_node // teams_exe_cmd_nproc for the_node in range(len(pbs_nodes))]
        node_team_seq = [ [node, team] for node,team in zip(pbs_nodes, node_teams)]
        
        node_team_seq.sort(key = operator.itemgetter(1))
        team_groups = itertools.groupby(node_team_seq, operator.itemgetter(1))
        team_nodes = [[item[0] for item in data] for (key, data) in team_groups]

        # Get the entry and exit commands from the job on the first cast/member
        # Foolery for in/out of memory
        if isinstance(object_list[0], str):
            pkl_file = obj._compose_dir / (object_list[0] + '/WrfHydroSim.pkl')
            jobs = pickle.load(pkl_file.open('rb')).jobs
        else:
            jobs = object_list[0].jobs
        if len(jobs) > 1:
            raise ValueError('Teams runs only support single job simulations')
        entry_cmd = jobs[0]._entry_cmd
        exit_cmd = jobs[0]._entry_cmd

        # Assign teams!
        for team in range(n_teams):
            teams_dict.update({
                team: {
                    object_name: team_objects[team],
                    'nodes': team_nodes[team],
                    'entry_cmd': entry_cmd,
                    'exit_cmd': exit_cmd,
                    'exe_cmd': teams_exe_cmd,
                    'env': env
                }
            })

        print('\nPBS_NODE_FILE present: ')
        print('    ' + str(len(unique_nodes)) + ' nodes with')
        print('    ' + str(n_total_processors) + ' TOTAL processors requested.')

        print('\nTeams parallelization:')
        print('    ' + str(n_runs) + ' total ' + object_name)
        print('    ' + str(n_teams) + ' concurrent teams using')
        print('    ' + str(teams_exe_cmd_nproc) + ' processors each.')

        return teams_dict
