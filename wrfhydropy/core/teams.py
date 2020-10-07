import collections
import itertools
import math
import operator
import os
import pathlib
import pickle
from pprint import pprint
import wrfhydropy


def set_cycle_ens_sim_jobs(ens_obj, job):
    members = ens_obj.members
    for mem in members:
        # Currently these are always strings, never in memory.
        if isinstance(mem, str):
            pkl_file = ens_obj._compose_dir / (mem + '/WrfHydroSim.pkl')
            sim = pickle.load(pkl_file.open('rb'))
            sim.jobs[0]._entry_cmd = job._entry_cmd
            sim.jobs[0]._exe_cmd = job._exe_cmd
            sim.jobs[0]._exit_cmd = job._exit_cmd
            sim.pickle(pkl_file)


def get_cycle_ens_sim_job_exits(cycle_obj):
    members = cycle_obj.members
    statuses = {}
    for mem in members:
        pkl_file = cycle_obj._compose_dir / (mem + '/WrfHydroSim.pkl')
        sim = pickle.load(pkl_file.open('rb'))
        statuses.update({pkl_file: sim.jobs[0].exit_status})
    success = all([value == 0 for key, value in statuses.items()])
    if success:
        return 0
    else:
        return 1


def parallel_teams_run(arg_dict):
    """
    Parallelizable function to run simulations across nodes.
    On the master node, python runs multiprocessing. Each separate process
    is a "team" of simulations to run. Multiprocessing makes MPI calls with
    a specific syntax to run the MPI executable on specfific (potentially
    other) nodes. This provides 2 levels of parallelism.

    This function is called (in parallel) once for each team by
    multiprocessing. Each team runs its set of simulations sequentially but
    each simulation it runs is parallel via MPI. (In the case of
    ensemble-cycles each team runs an ensemble but the ensemble runs its
    members sequentially.)

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
            exe_cmd: 'mpirun --host {nodelist} -np {nproc} {cmd}'
        The variables in brackets are expanded by internal variables. The
        'exe_cmd' command substitutes the wrfhydropy of 'wrf_hydro.exe'
        convention for {cmd}.
        The {nproc} argument is the length of the list passed in the nodes
        argument, and the {nodellist} are the comma separated arguments in that
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

        # The cycle ensemble has an extra level of ensemble between the casts and the sims.
        # An ensemble and a non-ensemble-cycle have sim objects at this level
        have_cycle_ens = False
        object_pkl_file = pathlib.Path("WrfHydroSim.pkl")
        if not object_pkl_file.exists():
            # But a cycle ensemble will have ensembles at this level....
            have_cycle_ens = True
            object_pkl_file = pathlib.Path("WrfHydroEns.pkl")
        if not object_pkl_file.exists():
            raise FileNotFoundError(
                "No appropriate pickle object for running " + obj_name + ".")

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
                                'nodelist': team_dict['nodes'][0],  # only use one task
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
                                'nodelist': team_dict['nodes'][0],  # only use one task
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
                'nodelist': ','.join(team_dict['nodes']),
                'nproc': len(team_dict['nodes'])
            }
        )

        # This will write the cmd to be executed into the member dir.
        # with open('team_run_cmd', 'w') as opened_file:
        #    opened_file.write(job._exe_cmd)

        object_pkl.pickle(object_pkl_file)
        if have_cycle_ens:
            # An ensemble-cycle neeeds the job components set on the simulations.
            # This object is acutally an ensemble...
            set_cycle_ens_sim_jobs(object_pkl, job)

        object_pkl.run(env=team_dict['env'])

        if have_cycle_ens:
            # An ensemble-cycle neeeds the job components set on the simulations.
            exit_status = get_cycle_ens_sim_job_exits(object_pkl)
        else:
            exit_status = object_pkl.jobs[0].exit_status

    exit_statuses.update({obj: exit_status})
    return exit_statuses


def assign_teams(
        obj,
        teams_exe_cmd: str,
        teams_exe_cmd_nproc: int,
        teams_node_file: dict = None,
        scheduler: str = 'pbs',
        env: dict = None
) -> dict:
    """
    Assign teams for parallel runs across nodes.
    Inputs:
        obj: The ensemble or cycle object, containin lists of members or casts
            to be run.
        teams_exe_cmd: str, The mpi-specific syntax needed. For example
            'mpirun --host {nodelist} -np {nproc} {cmd}'
        teams_exe_cmd_nproc: int, The number of cores per model/wrf_hydro
            simulation to be run.
        teams_node_file: [str, pathlib.Path] = None,
    Optional file that acts like a node file.
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

    n_runs = len(object_list)

    if scheduler is 'pbs':

        if teams_node_file is None:
            teams_node_file = os.environ.get('PBS_NODEFILE')

        pbs_nodes = []
        # TODO: comment the target format here.
        with open(teams_node_file, 'r') as infile:
            for line in infile:
                pbs_nodes.append(line.rstrip())

        n_total_processors = len(pbs_nodes)  # less may be used.
        n_teams = min(math.floor(len(pbs_nodes) / teams_exe_cmd_nproc), n_runs)
        pbs_nodes_counts = dict(collections.Counter(pbs_nodes))
        if n_teams == 0:
            raise ValueError("teams_exe_cmd_nproc > total number of cores available")
        if (n_teams > 1 and
            any([ teams_exe_cmd_nproc > val for val in pbs_nodes_counts.values()])):
                raise ValueError("teams_exe_cmd_nproc > number of cores/node: "
                                 'teams does not currently function in this capacity.')

        # Map the objects on to the teams (this seems overly complicated, should prob
        # consider using pandas:
        teams_dict = {}

        # If the cast/ensemble is still in memory, this looks different.
        if isinstance(object_list[0], wrfhydropy.Simulation):
            object_dirs = [oo.run_dir for oo in object_list]
        else:
            object_dirs = object_list

        object_teams = [the_object % n_teams for the_object in range(n_runs)]
        object_team_seq = [[dir, team] for dir, team in zip(object_dirs, object_teams)]
        object_team_seq.sort(key=operator.itemgetter(1))
        team_groups = itertools.groupby(object_team_seq, operator.itemgetter(1))
        team_objects = [[item[0] for item in data] for (key, data) in team_groups]

        # Map the nodes on to the teams
        # Homogonization step here to avoid communication across nodes...
        # Sorting necessary for testing.
        unique_nodes = sorted([node for node in list(set(pbs_nodes))])
        print("\n*** Team " + object_name + ' ***')
        print("Running on nodes: " + ', '.join(unique_nodes))
        del pbs_nodes
        pbs_nodes = []

        # This is a proposal for cross-node execution setup that seems to work
        # but it crashes.
        # if any([ teams_exe_cmd_nproc > val for val in pbs_nodes_counts.values()]):
        #     pbs_nodes_avail = [ nn.split('.')[0] for nn in pbs_nodes_in]
        #     # copy.deepcopy(pbs_nodes_in)
        #     for i_team in range(n_teams):
        #         the_team_nodes = []
        #         for ii in range(teams_exe_cmd_nproc):
        #             the_team_nodes += [pbs_nodes_avail.pop(0)]
        #         pbs_nodes += [the_team_nodes]
        #     team_nodes = pbs_nodes
        # else:

        for i_team in range(n_teams):
            pbs_nodes = pbs_nodes + (
                [unique_nodes[i_team % len(unique_nodes)]] * teams_exe_cmd_nproc)
        node_teams = [the_node // teams_exe_cmd_nproc for the_node in range(len(pbs_nodes))]
        node_team_seq = [[node, team] for node, team in zip(pbs_nodes, node_teams)]

        node_team_seq.sort(key=operator.itemgetter(1))
        team_groups = itertools.groupby(node_team_seq, operator.itemgetter(1))
        team_nodes = [[item[0] for item in data] for (key, data) in team_groups]
        # End else

        # Get the entry and exit commands from the job on the first cast/member
        # Foolery for in/out of memory
        if isinstance(object_list[0], str):
            # An ensemble and a non-ensemble-cycle have sim objects at this level
            pkl_file = obj._compose_dir / (object_list[0] + '/WrfHydroSim.pkl')
            if not pkl_file.exists():
                # But a cycle ensemble will have ensembles at this level....
                pkl_file = obj._compose_dir / (object_list[0] + '/WrfHydroEns.pkl')
            if not pkl_file.exists():
                raise FileNotFoundError(
                    "No appropriate pickle object for running " + object_name + ".")
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

        print('\nTeams dict:')
        pprint(teams_dict)
        print('\n')

        return teams_dict
