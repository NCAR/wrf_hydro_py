import subprocess
import sys

# Where is wrfhydropy/core dir in the filesystem?
# A method (used by  django) about specifying the root dir of the project.
# https://stackoverflow.com/questions/25389095/python-get-path-of-root-project-structure
#core_dir = pathlib.Path(__file__).absolute().parent
#DATA_PATH = pathlib.Path(pkg_resources.resource_filename('wrfhydropy', 'core/data/'))

def seconds(walltime):
    """Convert [[[DD:]HH:]MM:]SS to hours"""
    wtime = walltime.split(":")
    if len(wtime) == 1:
        return float(wtime[0])
    elif len(wtime) == 2:
        return float(wtime[0])*60.0 + float(wtime[1])
    elif len(wtime) == 3:
        return float(wtime[0])*3600.0 + float(wtime[1])*60.0 + float(wtime[2])
    elif len(wtime) == 4:
        return (float(wtime[0])*24.0*3600.0
                + float(wtime[0])*3600.0
                + float(wtime[1])*60.0
                + float(wtime[2]))
    else:
        print("Error in walltime format:", walltime)
        sys.exit()


def hours(walltime):
    """Convert [[[DD:]HH:]MM:]SS to hours"""
    wtime = walltime.split(":")
    if len(wtime) == 1:
        return float(wtime[0])/3600.0
    elif len(wtime) == 2:
        return float(wtime[0])/60.0 + float(wtime[1])/3600.0
    elif len(wtime) == 3:
        return float(wtime[0]) + float(wtime[1])/60.0 + float(wtime[2])/3600.0
    elif len(wtime) == 4:
        return (float(wtime[0])*24.0
                + float(wtime[0])
                + float(wtime[1])/60.0
                + float(wtime[2])/3600.0)
    else:
        print("Error in walltime format:", walltime)
        sys.exit()


def strftimedelta(seconds):     #pylint: disable=redefined-outer-name
    """Convert seconds to D+:HH:MM:SS"""
    seconds = int(seconds)

    day_in_seconds = 24.0*3600.0
    hour_in_seconds = 3600.0
    minute_in_seconds = 60.0

    day = int(seconds/day_in_seconds)
    seconds -= day*day_in_seconds

    hour = int(seconds/hour_in_seconds)
    seconds -= hour*hour_in_seconds

    minute = int(seconds/minute_in_seconds)
    seconds -= minute*minute_in_seconds

    return str(day) + ":" + ("%02d" % hour) + ":" + ("%02d" % minute) + ":" + ("%02d" % seconds)

def generic_popen(cmd_list):
    p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = p.communicate()
    return p.returncode

def delete(jobid, sched_name):
    """qdel (PBS) or scancel (slurm) a job."""
    if sched_name == 'PBS': cmd = 'qdel'
    if sched_name == 'slurm':  cmd = 'scancel'
    return(generic_popen([cmd, jobid]))

def hold(jobid, sched_name):
    """qhold (PBS) or scontrol (slurm) a job."""
    if sched_name == 'PBS': cmd = 'qhold'
    if sched_name == 'slurm':  cmd = 'scontrol'
    return(generic_popen([cmd, jobid]))

def release(sched):
    """qrls (PBS) or scontrol un-delay (slurm) a job."""
    print('sched.sched_job_id: ', sched.sched_job_id)
    if sched.sched_name == 'PBS':
        cmd_list = ['qrls', sched.sched_job_id]
    if sched.sched_name == 'slurm':
        cmd_list = ["scontrol", "update", "JobId=", sched.sched_job_id, "StartTime=", "now"]
    return(generic_popen(cmd_list))

