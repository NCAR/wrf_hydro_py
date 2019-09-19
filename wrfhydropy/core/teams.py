import pathlib
import os
import pickle


def parallel_teams_run(arg_dict):
    """Parallelizable function for teams to run an EnsembleSimuation.

    This function is called (in parallel) once for each team. (First level of parallelism
    is from python multiprocessing.
    This function (a team) sequentially runs (loops over) the ensemble members for which
    the team is responsible.
    This function (a team) makes a system call using MPI. (The second level of parallelism).

    # TODO: extract this to be used by both cycles and ensembles.

    Arguments:
        arg_dict:
            arg_dict == {
               'obj_name'   : string, either "member" or "cast" (or some other object),
                              matches the object name used in the team_dict below (first
                              argument)
               'compose_dir': <pathlib.Path absolute path to the cycle top level/compse,
                               dir where the individual cycle dirs are found>,
               'team_dict'  : <dict: the information needed for the team, see below>
            }
            where
            team_dict == {
                'casts'    : <list: length n_team, groups of cast run_dirs>
                'nodes'    : <list: the nodes previously parsed from something like
                              $PBS_NODEFILE>,
                'entry_cmd': <string: the entry cmd to be run>,
                'exe_cmd'  : <string: the MPI-specific model invokation command>,
                'exit_cmd' : <string: exit cmd to be run>,
                'env'      : <dict: containging the environment in which to run the
                              cmds, may be None or 'None'>
            }

            The 'exe_cmd' is a form of invocation for the distribution of MPI to be used. For
            openmpi, for example for OpenMPI, this is
                exe_cmd: 'mpirun --host {hostnames} -np {nproc} {cmd}'
            The variables in brackets are expanded by internal variables. The 'exe_cmd'
            command substitutes the wrfhydropy of 'wrf_hydro.exe' convention for {cmd}.
            The {nproc} argument is the length of the list passed in the nodes argument,
            and the {hostnames} are the comma separated arguments in that list.

            The "entry_cmd" and "exit_cmd"
              1) can be semicolon-separated commands
              2) where these are run depends on MPI. OpenMPI, for example, handles these
                 on the same processor set as the model runs.

    Notes:
        Currently this is working/tested with openmpi.
        MPT requires MPI_SHEPERD env variable and it's performance is not satisfactory so far.
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

        obj_pkl_file = "WrfHydroSim.pkl"
        obj_pkl = pickle.load(open(obj_pkl_file, "rb"))
        job = obj_pkl.jobs[0]

        if job._entry_cmd is not None:
            entry_cmds = job._entry_cmd.split(';')
            new_entry_cmd = []
            for cmd in entry_cmds:
                if 'mpirun' not in cmd:
                    new_entry_cmd.append(
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

        obj_pkl.pickle(obj_pkl_file)
        obj_pkl.run(env=team_dict['env'])

        exit_statuses.update({obj: obj_pkl.jobs[0].exit_status})

    return exit_statuses
