from boltons.iterutils import remap, get_path
import datetime
from distutils.spawn import find_executable
import io
import os
import pathlib
import pickle
import re
import pkg_resources
import shlex
import socket
import subprocess
import sys
import time
import warnings
import yaml
# Where is wrfhydropy/core dir in the filesystem?
# A method (used by  django) about specifying the root dir of the project.
# https://stackoverflow.com/questions/25389095/python-get-path-of-root-project-structure
core_dir = pathlib.PosixPath(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = pathlib.Path(pkg_resources.resource_filename('wrfhydropy', 'core/data/'))


class PBSError(Exception):
    """ A custom error class for pbs errors """
    def __init__(self, jobid, msg):
        self.jobid = jobid
        self.msg = msg
        super(PBSError, self).__init__()

    def __str__(self):
        return self.jobid + ": " + self.msg


def get_sched_name():
    """Tries to find qsub, then sbatch. Returns "PBS" if qsub
    is found, else returns "slurm" if sbatch is found, else returns
    "other" if neither is found. """
    if find_executable("qsub") is not None:
        return "PBS"
    elif find_executable("sbatch") is not None:
        return "slurm"
    else:
        return None


def in_docker():
    path = "/proc/" + str(os.getpid()) + "/cgroup"
    if not os.path.isfile(path): return False
    with open(path) as f:
        for line in f:
            if re.match("\d+:[\w=]+:/docker(-[ce]e)?/\w+", line):
                return True
        return False


def get_machine():
    hostname = socket.gethostname()
    if re.match('cheyenne', hostname):
        machine='cheyenne'
    else:
        machine='docker'
        if not in_docker():
            warnings.warn('This machine is not recognized, using docker defaults.')
    return machine


def get_user():
    """Returns the user name/handle."""
    try:
        return os.getlogin()
    except OSError:
        if 'USER' in os.environ.keys():
            return os.environ['USER']
        else:
            sp = subprocess.run(['whoami'], stdout=subprocess.PIPE)
            return sp.stdout.decode('utf-8').split('\n')[0]
    else:
        return "?"


def get_version(software=None):
    """Returns the software version """
    if software is None:
        software = get_sched_name()
    if software is "PBS":
        opt = ["qstat", "--version"]

        # call 'qstat' using subprocess
        p = subprocess.Popen(opt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) #pylint: disable=invalid-name
        stdout, stderr = p.communicate()    #pylint: disable=unused-variable
        sout = io.StringIO(stdout.decode('utf-8'))

        # return the version number
        return sout.read().rstrip("\n").lstrip("version: ")
    elif software is "slurm":
        opt = ["squeue", "--version"]

        # call 'squeue' using subprocess
        p = subprocess.Popen(opt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) #pylint: disable=invalid-name
        stdout, stderr = p.communicate()    #pylint: disable=unused-variable
        sout = io.StringIO(stdout.decode('utf-8'))

        # return the version number
        return sout.read().rstrip("\n").lstrip("slurm ")

    else:
        return "0"


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


def exetime(deltatime):
    """Get the exetime string for the PBS '-a'option from a [[[DD:]MM:]HH:]SS string

       exetime string format: YYYYmmddHHMM.SS
    """
    return (datetime.datetime.now()
            +datetime.timedelta(hours=hours(deltatime))).strftime("%Y%m%d%H%M.%S")


def touch(filename, mode=0o666, dir_fd=None, **kwargs):
    flags = os.O_CREAT | os.O_APPEND
    with os.fdopen(os.open(filename, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(f.fileno() if os.utime in os.supports_fd else filename,
                 dir_fd=None if os.supports_fd else dir_fd, **kwargs)


def _qstat(jobid=None,
           username=get_user(),
           full=False,
           version=int(14)): #re.split("[\+\ \.]", get_version())[2])):
    """Return the stdout of qstat minus the header lines.

       By default, 'username' is set to the current user.
       'full' is the '-f' option
       'id' is a string or list of strings of job ids

       Returns the text of qstat, minus the header lines
    """

    # -u and -f contradict in earlier versions of PBS
    if full and username is not None and (version < 5.0 and jobid is None):
        # First get all jobs by the user
        qopt = ["qselect"]
        qopt += ["-u", username]

        # Call 'qselect' using subprocess
        q = subprocess.Popen(qopt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)     #pylint: disable=invalid-name
        stdout, stderr = q.communicate()    #pylint: disable=unused-variable

        qsout = io.StringIO(stdout)

        # Get the jobids
        jobid = []
        for line in qsout:
            jobid += [line.rstrip("\n")]

    opt = ["qstat"]
    # If there are jobid(s), you don't need a username
    if username is not None and jobid is None:
        opt += ["-u", username]
    # But if there are jobid(s) and a username, you need -a to get full output
    elif username is not None and jobid is not None and not full:
        opt += ["-a"]
    # By this point we're guaranteed torque ver >= 5.0, so -u and -f are safe together
    if full:
        opt += ["-f"]
    if jobid is not None:
        if isinstance(jobid, str) or isinstance(jobid, unicode):
            jobid = [jobid]
        elif isinstance(jobid, list):
            pass
        else:
            print("Error in scheduler_misc.qstat(). type(jobid):", type(jobid))
            sys.exit()
        opt += jobid

    # call 'qstat' using subprocess
    # print opt
    p = subprocess.Popen(opt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)     #pylint: disable=invalid-name
    stdout, stderr = p.communicate()        #pylint: disable=unused-variable

    sout = io.StringIO(stdout)

    # strip the header lines
    if full is False:
        for line in sout:
            if line[0] == "-":
                break

    # return the remaining text
    return sout.read()


def job_id(all=False, name=None):       #pylint: disable=redefined-builtin
    """If 'name' given, returns a list of all jobs with a particular name using qstat.
       Else, if all=True, returns a list of all job ids by current user.
       Else, returns this job id from environment variable PBS_JOBID (split to get just the number).

       Else, returns None

    """
    if all or name is not None:

        jobid = []
        stdout = _qstat()
        sout = io.StringIO(stdout)
        for line in sout:
            if name is not None:
                if line.split()[3] == name:
                    jobid.append((line.split()[0]).split(".")[0])
            else:
                jobid.append((line.split()[0]).split(".")[0])
        return jobid
    
    else:

        if 'PBS_JOBID' in os.environ:
            return os.environ['PBS_JOBID'].split(".")[0]
        elif 'SLURM_JOBID' in os.environ:
            return os.environ['SLURM_JOBID'].split(".")[0]
        else:
            return None
        #raise PBSError(
        #    "?",
        #    "Could not determine jobid. 'PBS_JOBID' environment variable not found.\n"
        #    + str(os.environ))


def job_rundir(jobid, sched_name):
    """Return the directory job "id" was run in using qstat.
       Returns a dict, with id as key and rundir and value.
    """
    rundir = dict()

    if sched_name == 'PBS':

        if isinstance(jobid, (list)):
            for i in jobid:
                stdout = _qstat(jobid=i, full=True)
                match = re.search(",PWD=(.*),", stdout)
                rundir[i] = match.group(1)
        else:
            stdout = _qstat(jobid=jobid, full=True)
            match = re.search(",PWD=(.*),", stdout)
            rundir[i] = match.group(1)
        return rundir

    elif sched_name == 'slurm':

        if isinstance(jobid, (list)):
            for i in jobid:
                stdout = _squeue(jobid=i, full=True)
                match = re.search("WorkDir=(.*),", stdout)
                rundir[i] = match.group(1)
        else:
            stdout = _squeue(jobid=jobid, full=True)
            match = re.search("WorkDir=(.*),", stdout)
            rundir[i] = match.group(1)
        return rundir

    else:

        # TODO JLM: harden.
        warnings.warn("sched_name matches neither 'PBS' nor 'slurm': FIX THIS.")


def job_status_PBS(jobid=None):
    """Return job status using qstat

      Returns a dict of dict, with jobid as key in outer dict.
        Inner dict contains:
        "name", "nodes", "procs", "walltime",
        "jobstatus": status ("Q","C","R", etc.)
        "qstatstr": result of qstat -f jobid, None if not found
        "elapsedtime": None if not started, else seconds as int
        "starttime": None if not started, else seconds since epoch as int
        "completiontime": None if not completed, else seconds since epoch as int
    
       *This should be edited to return job_status_dict()'s*
        """
    status = dict()

    stdout = _qstat(jobid=jobid, full=True)
    sout = io.StringIO(stdout)

    # TODO: figure out why jobstatus is being initialized as a None vs as a dict() and then checked for content ### pylint: disable=fixme
    jobstatus = None

    for line in sout:

        m = re.search(r"Job Id:\s*(.*)\s", line)      #pylint: disable=invalid-name
        if m:
            if jobstatus is not None:
                if jobstatus["jobstatus"] == "R":           #pylint: disable=unsubscriptable-object
                    jobstatus["elapsedtime"] = int(time.time()) - jobstatus["starttime"]    #pylint: disable=unsubscriptable-object
                    status[jobstatus["jobid"]] = jobstatus #pylint: disable=unsubscriptable-object
                    jobstatus = dict()
                    jobstatus["jobid"] = m.group(1).split(".")[0]
                    jobstatus["qstatstr"] = line
                    jobstatus["elapsedtime"] = None
                    jobstatus["starttime"] = None
                    jobstatus["completiontime"] = None
            continue

        jobstatus["qstatstr"] += line

        #results = line.split()
        #jobid = results[0].split(".")[0]
        #jobstatus = dict()
        #jobstatus["jobid"] = jobid

        #jobstatus["jobname"] = results[3]
        m = re.match(r"\s*Job_Name\s*=\s*(.*)\s", line)       #pylint: disable=invalid-name
        if m:
            jobstatus["jobname"] = m.group(1)
            continue

        #jobstatus["nodes"] = int(results[5])
        #jobstatus["procs"] = int(results[6])
        m = re.match(r"\s*Resource_List\.nodes\s*=\s*(.*):ppn=(.*)\s", line)  #pylint: disable=invalid-name
        if m:
            jobstatus["nodes"] = m.group(1)
            jobstatus["procs"] = int(m.group(1))*int(m.group(2))
            continue

        #jobstatus["walltime"] = int(seconds(results[8]))
        m = re.match(r"\s*Resource_List\.walltime\s*=\s*(.*)\s", line)      #pylint: disable=invalid-name
        if m:
            jobstatus["walltime"] = int(seconds(m.group(1)))
            continue

        #jobstatus["jobstatus"] = results[9]
        m = re.match(r"\s*job_state\s*=\s*(.*)\s", line)        #pylint: disable=invalid-name
        if m:
            jobstatus["jobstatus"] = m.group(1)
            continue

        #elapsedtime = line.split()[10]
        #if elapsedtime == "--":
        #    jobstatus["elapsedtime"] = None
        #else:
        #    jobstatus["elapsedtime"] = int(seconds(elapsedtime))
        #
        #qstatstr = qstat(jobid, full=True)
        #if not re.match("^qstat: Unknown Job Id Error.*",qstatstr):
        #    jobstatus["qstatstr"] = qstatstr
        #    m = re.search("Job_Name = (.*)\n",qstatstr)
        #    if m:
        #        jobstatus["jobname"] = m.group(1)

        #m = re.match("\s*resources_used.walltime\s*=\s*(.*)\s",line)
        #if m:
        #    print line
        #    jobstatus["elapsedtime"] = int(seconds(m.group(1)))

        m = re.match(r"\s*start_time\s*=\s*(.*)\s", line)    #pylint: disable=invalid-name
        if m:
            jobstatus["starttime"] = int(time.mktime(datetime.datetime.strptime(
                m.group(1), "%a %b %d %H:%M:%S %Y").timetuple()))
            continue

        m = re.search(r"\s*comp_time\s*=\s*(.*)\s", line)   #pylint: disable=invalid-name
        if m:
            jobstatus["completiontime"] = int(time.mktime(datetime.datetime.strptime(
                m.group(1), "%a %b %d %H:%M:%S %Y").timetuple()))
            continue

    if jobstatus is not None:
        if jobstatus["jobstatus"] == "R":
            jobstatus["elapsedtime"] = int(time.time()) - jobstatus["starttime"]
        status[jobstatus["jobid"]] = jobstatus

    return status


def submit_scheduler(substr, sched_name, hold=False):
    """Submit a PBS job using qsub.

       substr: The submit script string
    """

    if type(substr) is bytearray:
        substr_str = substr.decode('utf-8')
    else:
        substr_str = substr
        
    m = re.search(r"-N\s+(.*)\s", substr_str)       #pylint: disable=invalid-name
    if m:
        jobname = m.group(1)        #pylint: disable=unused-variable
    else:
        raise PBSError(
            None,
            r"Error in scheduler_misc.submit(). Jobname (\"-N\s+(.*)\s\") not found in submit string.")

    
    if sched_name == 'PBS':

        qsub_cmd = "qsub"
        if hold:
            qsub_cmd += " -h"

        p = subprocess.Popen( shlex.split(qsub_cmd),
                              stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
        
        stdout, stderr = p.communicate(input=substr)
        if re.search("error", stdout.decode('utf-8')):
            raise PBSError(0, "PBS Submission error.\n" + stdout + "\n" + stderr)
        else:
            jobid = stdout.decode('utf-8').split(".")[0]
            return jobid

    elif sched_name == 'slurm':

        p = subprocess.Popen( "sbatch", stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = p.communicate(input=substr)       #pylint: disable=unused-variable
        if re.search("error", stdout):
            raise PBSError(0, "PBS Submission error.\n" + stdout + "\n" + stderr)
        else:
            jobid = stdout.rstrip().split()[-1]
            return jobid

    else:

        # TODO JLM: harden.
        warnings.warn("sched_name matches neither 'PBS' nor 'slurm': FIX THIS.")


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
    
    if sched.sched_name == 'PBS':
        cmd_list = ['qrls', sched.sched_job_id]
    if sched.sched_name == 'slurm':
        cmd_list = ["scontrol", "update", "JobId=", sched.sched_job_id, "StartTime=", "now"]
    return(generic_popen(cmd_list))


def alter(jobid, arg):
    """qalter (PBS) or scontrol update (slurm) a job.
         'arg' is a pbs command option string. For instance, "-a 201403152300.19"
    """
    if sched_name == 'PBS':
        cmd_list = ["qalter"] + arg.split() + [jobid]
    if sched_name == 'slurm':
        cmd_list = ["scontrol", "update", "JobId=", jobid] + arg.split()
    return(generic_popen(cmd_list))


# #######################################################
# SLURM-only section follows 

def _squeue(jobid=None,
            username=get_user(),
            full=False,
            version=int(14),
            sformat=None):
    """Return the stdout of squeue minus the header lines.

       By default, 'username' is set to the current user.
       'full' is the '-f' option
       'jobid' is a string or list of strings of job ids
       'version' is a software version number, used to
            determine compatible ops
       'sformat' is a squeue format string (e.g., "%A %i %j %c")

       Returns the text of squeue, minus the header lines
    """

    # If Full is true, we need to use scontrol:
    if full is True:
        if jobid is None:
            if username is None:
                # Clearly we want ALL THE JOBS
                sopt = ["scontrol", "show", "job"]

                # Submit the command
                p = subprocess.Popen(sopt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)     #pylint: disable=invalid-name
                stdout, stderr = p.communicate()        #pylint: disable=unused-variable

                sout = io.StringIO(stdout)

                # Nothing to strip, as scontrol provides no headers
                return sout.read()

            else:
                # First, get jobids that belong to that username using
                # squeue (-h strips the header)
                sopt = ["squeue", "-h", "-u", username]

                q = subprocess.Popen(sopt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)     #pylint: disable=invalid-name
                stdout, stderr = q.communicate()    #pylint: disable=unused-variable

                qsout = io.StringIO(stdout)

                # Get the jobids
                jobid = []
                for line in qsout:
                    jobid += [line.rstrip("\n")]
                # Great, now we have some jobids to pass along

        # Ensure the jobids are a list, even if they're a list of 1...
        if not isinstance(jobid, list) and jobid is not None:
            jobid = [jobid]
        if isinstance(jobid, list):
            opt = ["scontrol", "show", "job"]
            sreturn = ""
            for my_id in jobid:
                sopt = opt + [str(my_id)]

                q = subprocess.Popen(sopt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)     #pylint: disable=invalid-name
                stdout, stderr = q.communicate()    #pylint: disable=unused-variable

                sreturn = sreturn + stdout + "\n"

            return sreturn

    else:
        sopt = ["squeue", "-h"]
        if username is not None:
            sopt += ["-u", username]
        if jobid is not None:
            sopt += ["--job="]
            if isinstance(jobid, list):
                sopt += ["'"+",".join([str(i) for i in jobid])+"'"]
            else:
                sopt += [str(jobid)]
        if sformat is not None:
            sopt += ["-o", "'" + sformat + "'"]
        else:
            if jobid is None and username is None:
                sopt += ["-o", "'%i %u %P %j %U %D %C %m %l %t %M'"]
            else:
                sopt += ["-o", "'%i %j %u %M %t %P'"]

        q = subprocess.Popen(sopt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)     #pylint: disable=invalid-name
        stdout, stderr = q.communicate()    #pylint: disable=unused-variable

        sout = io.StringIO(stdout)

        # return the remaining text
        return sout.read()


def job_status_slurm(jobid=None):
    """Return job status using squeue

       Returns a dict of dict, with jobid as key in outer dict.
       Inner dict contains:
       "name", "nodes", "procs", "walltime",
       "jobstatus": status ("Q","C","R", etc.)
       "qstatstr": result of squeue -f jobid, None if not found
       "elapsedtime": None if not started, else seconds as int
       "starttime": None if not started, else seconds since epoch as int
       "completiontime": None if not completed, else seconds since epoch as int

       *This should be edited to return job_status_dict()'s*
    """
    status = dict()

    stdout = _squeue(jobid=jobid, full=True)
    sout = io.StringIO(stdout)

### TODO: figure out why jobstatus is being initialized as a None vs as a dict() and then checked for content ### pylint: disable=fixme
    # jobstatus = None
    jobstatus = {"jobid" : None, "name" : None, "nodes" : None, "procs" : None, "walltime" : None, "qstatstr" : None, "elapsedtime" : None, "starttime" : None, "completiontime" : None, "jobstatus" : None, "cluster": None}

    for line in sout:
        # Check for if we're at a new job header line
        m = re.search(r"JobId=\s*(\S*)\s*", line)      #pylint: disable=invalid-name
        if m:
            if jobstatus["jobstatus"] is not None:
                status[jobstatus["jobid"]] = jobstatus
            jobstatus = {"jobid" : None, "name" : None, "nodes" : None, "procs" : None, "walltime" : None, "qstatstr" : None, "elapsedtime" : None, "starttime" : None, "completiontime" : None, "jobstatus" : None, "cluster" : None}
            jobstatus["jobid"] = m.group(1)

            # Grab the job name
            m = re.match(r"\S*\s*Name=\s*(.*)\s?", line)       #pylint: disable=invalid-name
            if m:
                jobstatus["jobname"] = m.group(1)

            # Save the full output
            jobstatus["qstatstr"] = line
            continue

        jobstatus["qstatstr"] += line

        # Look for the Nodes/PPN Info
        m = re.search(r"NumNodes=\s*([0-9]*)\s", line) #pylint: disable=invalid-name
        if m:
            jobstatus["nodes"] = int(m.group(1))
            m = re.match(r"\S*\s*NumCPUs=\s*([0-9]*)\s", line) #pylint: disable=invalid-name
            if m:
                jobstatus["procs"] = int(m.group(1))
            continue


        # Look for timing info
        m = re.search(r"RunTime=\s*([0-9]*:[0-9]*:[0-9]*)\s", line) #pylint: disable=invalid-name
        if m:
            if m.group(1) == "Unknown":
                continue
            hrs, mns, scs = m.group(1).split(":")
            runtime = datetime.timedelta(hours=int(hrs), minutes=int(mns), seconds=int(scs))
            jobstatus["elapsedtime"] = runtime.seconds

            m = re.match(r"\S*\s*TimeLimit=\s*([0-9]*:[0-9]*:[0-9]*)\s", line) #pylint: disable=invalid-name
            if m:
                walltime = datetime.timedelta(hours=int(hrs), minutes=int(mns), seconds=int(scs))
                jobstatus["walltime"] = walltime.seconds
            continue

        # Grab the job start time
        m = re.search(r"StartTime=\s*([0-9]*\-[0-9]*\-[0-9]*T[0-9]*:[0-9]*:[0-9]*)\s", line) #pylint: disable=invalid-name
        if m:
            if m.group(1) == "Unknown":
                continue
            year, month, day = m.group(1).split("T")[0].split("-")
            hrs, mns, scs = m.group(1).split("T")[1].split(":")
            starttime = datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hrs), minute=int(mns), second=int(scs))
            jobstatus["starttime"] = time.mktime(starttime.timetuple())
            continue

        # Grab the job status
        m = re.search(r"JobState=\s*([a-zA-Z]*)\s", line) #pylint: disable=invalid-name
        if m:
            my_status = m.group(1)
            if my_status == "RUNNING" or my_status == "CONFIGURING":
                jobstatus["jobstatus"] = "R"
            elif my_status == "BOOT_FAIL" or my_status == "FAILED" or my_status == "NODE_FAIL" or my_status == "CANCELLED" or my_status == "COMPLETED" or my_status == "PREEMPTED" or my_status == "TIMEOUT":
                jobstatus["jobstatus"] = "C"
            elif my_status == "COMPLETING" or my_status == "STOPPED":
                jobstatus["jobstatus"] = "E"
            elif my_status == "PENDING" or my_status == "SPECIAL_EXIT":
                jobstatus["jobstatus"] = "Q"
            elif my_status == "SUSPENDED":
                jobstatus["jobstatus"] = "S"
            else:
                jobstatus["jobstatus"] = "?"
            continue

        # Grab the cluster/allocating node:
        m = re.search(r"AllocNode:\s*.*=(.*):.*", line) #pylint: disable=invalid-name
        if m:
            raw_str = m.group(1)
            m = re.search(r"(.*?)(?=[^a-zA-Z0-9]*login.*)", raw_str)    #pylint: disable=invalid-name
            if m:
                jobstatus["cluster"] = m.group(1)
            else:
                jobstatus["cluster"] = raw_str


    if jobstatus["jobstatus"] is not None:
        status[jobstatus["jobid"]] = jobstatus

    return status


def default_job_spec(machine='docker'):
    if machine != 'docker':
        warnings.warn("Default job sepcs do not currently make sense except for docker.")
    default_job_specs_file = DATA_PATH / 'default_job_specs.yaml'
    with open(default_job_specs_file) as ff:
        default_job_specs = yaml.safe_load(ff)
    default_job_spec = default_job_specs[machine]
    # One can not really construct a default scheduler without a user spec.
    default_job_spec.pop('scheduler', None)
    if 'modules' in default_job_spec.keys() and default_job_spec['modules'] is not None:
        default_job_spec['modules'] = \
            default_job_spec['modules']['base'] + ' ' + default_job_spec['modules']['gfort']
    default_job_spec['exe_cmd'] = default_job_spec['exe_cmd']['default']
    return default_job_spec


def compose_scheduled_python_script(
    py_run_cmd: str,
    model_exe_cmd: str
):
    jobstr  = "#!/usr/bin/env python\n"
    jobstr += "\n"

    jobstr += "import argparse\n"
    jobstr += "import datetime\n"
    jobstr += "import os\n"
    jobstr += "import pickle\n"
    jobstr += "import sys\n"
    jobstr += "import wrfhydropy\n"
    jobstr += "\n"

    jobstr += "parser = argparse.ArgumentParser()\n"
    jobstr += "parser.add_argument('--sched_job_id',\n"
    jobstr += "                    help='The numeric part of the scheduler job ID.')\n"
    jobstr += "parser.add_argument('--job_date_id',\n"
    jobstr += "                    help='The date-time identifier created by Schduler obj.')\n"
    jobstr += "args = parser.parse_args()\n"
    jobstr += "\n"

    jobstr += "print('sched_job_id: ', args.sched_job_id)\n"
    jobstr += "print('job_date_id: ', args.job_date_id)\n"
    jobstr += "\n"

    jobstr += "run_object = pickle.load(open('WrfHydroRun.pkl', 'rb'))\n"
    jobstr += "\n"

    jobstr += "# The lowest index jobs_pending should be the job to run. Verify that it \n"
    jobstr += "# has the same job_date_id as passed first, then set job to active.\n"
    jobstr += "if run_object.job_active:\n"
    jobstr += "    msg = 'There is an active job conflicting with this scheduled job.'\n"
    jobstr += "    raise ValueError(msg)\n"
    jobstr += "if not run_object.jobs_pending[0].job_date_id == args.job_date_id:\n"
    jobstr += "    msg = 'The first pending job does not match the passed job_date_id.'\n"
    jobstr += "    raise ValueError(msg)\n"
    jobstr += "\n"

    jobstr += "# Promote the job to active.\n"
    jobstr += "run_object.job_active = run_object.jobs_pending.pop(0)\n"

    jobstr += "# Set some run-time attributes of the job.\n"
    jobstr += "run_object.job_active.py_exe_cmd = \"" + py_run_cmd + "\"\n"
    jobstr += "run_object.job_active.scheduler.sched_job_id = args.sched_job_id\n"
    jobstr += "run_object.job_active.exe_cmd = \"" + model_exe_cmd + "\"\n"
    jobstr += "# Pickle before running the job.\n"
    jobstr += "run_object.pickle()\n"
    jobstr += "\n"

    jobstr += "print(\"Running the model.\")\n"
    jobstr += "run_object.job_active.job_start_time = str(datetime.datetime.now())\n"
    jobstr += "run_object.job_active.run(run_object.run_dir)\n"
    jobstr += "run_object.job_active.job_start_time = str(datetime.datetime.now())\n"
    jobstr += "\n"

    jobstr += "print(\"Collecting model output.\")\n"
    jobstr += "run_object.collect_output()\n"
    jobstr += "print(\"Job completed.\")\n"
    jobstr += "\n"

    jobstr += "run_object.job_active._sched_job_complete = True\n"
    jobstr += "run_object.jobs_completed.append(run_object.job_active)\n"
    jobstr += "run_object.job_active = None\n"
    jobstr += "run_object.pickle()\n"
    jobstr += "\n"
    jobstr += "sys.exit(0)\n"

    return jobstr


def compose_scheduled_bash_script(
        run_dir: str,
        job: object
):
    """ Write Job as a string suitable for job.scheduler.sched_name """

    if job.scheduler.sched_name.lower() == "slurm":
        ###Write this Job as a string suitable for slurm
        ### NOT USED:
        ###    exetime
        ###    priority
        ###    auto
        jobstr = "#!/bin/bash\n"

        ## FROM DART for job arrays.
        #JOBNAME=$SLURM_JOB_NAME
        #JOBID=$SLURM_JOBID
        #ARRAY_INDEX=$SLURM_ARRAY_TASK_ID
        #NODELIST=$SLURM_NODELIST
        #LAUNCHCMD="mpirun -np $SLURM_NTASKS -bind-to core"

        jobstr += "#SBATCH -J {0}\n".format(job.scheduler.job_name)
        if job.scheduler.account is not None:
            jobstr += "#SBATCH -A {0}\n".format(job.scheduler.account)
        jobstr += "#SBATCH -t {0}\n".format(job.scheduler.walltime)
        jobstr += "#SBATCH -n {0}\n".format(job.scheduler.nnodes*job.scheduler.ppn)
        if job.scheduler.pmem is not None:
            jobstr += "#SBATCH --mem-per-cpu={0}\n".format(job.scheduler.pmem)
        if job.scheduler.qos is not None:
            jobstr += "#SBATCH --qos={0}\n".format(job.scheduler.qos)
        if job.scheduler.email != None and job.scheduler.message != None:
            jobstr += "#SBATCH --mail-user={0}\n".format(job.scheduler.email)
            if 'b' in job.scheduler.message:
                jobstr += "#SBATCH --mail-type=BEGIN\n"
            if 'e' in job.scheduler.message:
                jobstr += "#SBATCH --mail-type=END\n"
            if 'a' in job.scheduler.message:
                jobstr += "#SBATCH --mail-type=FAIL\n"
        # SLURM does assignment to no. of nodes automatically
        # jobstr += "#SBATCH -N {0}\n".format(job.scheduler.nodes)
        if job.scheduler.queue is not None:
            jobstr += "#SBATCH -p {0}\n".format(job.scheduler.queue)
        jobstr += "{0}\n".format(job.exe_cmd)

        return jobstr

    else:

        # Write this Job as a string suitable for PBS #

        jobstr = ""            
        jobstr += "#!/bin/sh\n"
        jobstr += "#PBS -N {0}\n".format(job.scheduler.job_name)
        jobstr += "#PBS -A {0}\n".format(job.scheduler.account)
        jobstr += "#PBS -q {0}\n".format(job.scheduler.queue)
        jobstr += "#PBS -M {0}\n".format(job.scheduler.email_who)
        jobstr += "#PBS -m {0}\n".format(job.scheduler.email_when)
        jobstr += "\n"

        jobstr += "#PBS -l walltime={0}\n".format(job.scheduler.walltime)
        jobstr += "\n"

        if job.scheduler.nproc_last_node == 0:
            prcstr = "select={0}:ncpus={1}:mpiprocs={1}\n"
            prcstr = prcstr.format(job.scheduler.nnodes, job.scheduler.ppn)
        else:
            prcstr = "select={0}:ncpus={1}:mpiprocs={1}+1:ncpus={2}:mpiprocs={2}\n"
            prcstr = prcstr.format(job.scheduler.nnodes-1,
                                   job.scheduler.ppn,
                                   job.scheduler.nproc_last_node)

        jobstr += "#PBS -l " + prcstr
        jobstr += "\n"

        jobstr += "# Not using PBS standard error and out files to capture model output\n"
        jobstr += "# but these hidden files might catch output and errors from the scheduler.\n"
        jobstr += "#PBS -o {0}\n".format(job.stdout_pbs_tmp(run_dir))
        jobstr += "#PBS -e {0}\n".format(job.stderr_pbs_tmp(run_dir))
        jobstr += "\n"

        if job.scheduler.afterok:
            if job.machine == 'cheyenne':
                cheyenne_afterok = get_cheyenne_job_dependency_id(job.scheduler.afterok)
                jobstr += "#PBS -W depend=afterok:{0}\n".format(cheyenne_afterok)
            else: 
                jobstr += "#PBS -W depend=afterok:{0}\n".format(job.scheduler.afterok)
                
        if job.scheduler.array_size:
            jobstr += "#PBS -J 1-{0}\n".format(job.scheduler.array_size)
        if job.scheduler.exetime:
            jobstr += "#PBS -a {0}\n".format(job.scheduler.exetime)
        if job.scheduler.pmem:
            jobstr += "#PBS -l pmem={0}\n".format(job.scheduler.pmem)
        if job.scheduler.grab_env:
            jobstr += "#PBS -V\n"
        if job.scheduler.array_size or \
           job.scheduler.exetime or \
           job.scheduler.pmem or \
           job.scheduler.grab_env:
            jobstr += "\n"

        # End PBS Header

        if job.modules:
            jobstr += 'module purge\n'
            jobstr += 'module load {0}\n'.format(job.modules)
            jobstr += "\n"

        jobstr += "echo PBS_JOBID: $PBS_JOBID\n"
        jobstr += "sched_job_id=`echo ${PBS_JOBID} | cut -d'.' -f1`\n"
        jobstr += "echo sched_job_id: $sched_job_id\n"
        jobstr += "job_date_id={0}\n".format(job.job_date_id)
        jobstr += "echo job_date_id: $job_date_id\n"
        jobstr += "\n"

        jobstr += "export TMPDIR=/glade/scratch/$USER/temp\n"
        jobstr += "mkdir -p $TMPDIR\n"

        if job.scheduler.queue == 'share':
            jobstr += "export MPI_USE_ARRAY=false\n"

        jobstr += "cd {0}\n".format(run_dir)
        jobstr += "echo \"pwd:\" `pwd`\n"
        jobstr += "\n"

        jobstr += "# DART job variables for future reference\n"
        jobstr += "# JOBNAME=$PBS_JOBNAME\n"
        jobstr += "# JOBID=\"$PBS_JOBID\"\n"
        jobstr += "# ARRAY_INDEX=$PBS_ARRAY_INDEX\n"
        jobstr += "# NODELIST=`cat \"${PBS_NODEFILE}\"`\n"
        jobstr += "# LAUNCHCMD=\"mpiexec_mpt\"\n"
        jobstr += "# \n"

        jobstr += "# CISL suggests users set TMPDIR when running batch jobs on Cheyenne.\n"
        jobstr += "export TMPDIR=/glade/scratch/$USER/temp\n"
        jobstr += "mkdir -p $TMPDIR\n"
        jobstr += "\n"

        exestr  = "{0} ".format(job.exe_cmd)
        exestr += "2> {0} 1> {1}".format(job.stderr_exe(run_dir), job.stdout_exe(run_dir))
        jobstr += "echo \"" + exestr + "\"\n"
        jobstr += exestr + "\n"
        jobstr += "\n"

        jobstr += "cmd_status=$?\n"
        jobstr += "echo \"cmd_status: $cmd_status\"\n"
        jobstr += "\n"

        jobstr += "# Touch these files just to get the job_date_id in their file names.\n"
        jobstr += "# Can identify the files by sched_job_id and replace contents...\n"
        jobstr += "touch {0}\n".format(job.tracejob_file(run_dir))
        jobstr += "touch {0}\n".format(job.stdout_pbs(run_dir))
        jobstr += "touch {0}\n".format(job.stderr_pbs(run_dir))
        jobstr += "\n"

        jobstr += "# Simple, file-based method for checking if the job is done.\n"
        jobstr += "# qstat is a bad way of doing this, apparently.\n"
        jobstr += "rm .job_not_complete\n"
        jobstr += "\n"

        ## TODO(JLM): the tracejob execution gets called by the waiting process.
        jobstr += "exit $cmd_status\n"

        return jobstr


def get_cheyenne_job_dependency_id(numeric_job_id):
    """Lovely bug in cheyenne's PBS that requires the full name on the job id."""
    cmd = 'qstat -w ' + str(numeric_job_id) + '| grep ' + str(numeric_job_id) + ' | cut -d" " -f1'
    cmd = "/bin/bash -c '" + cmd + "'" 
    cmd_run = subprocess.run(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return cmd_run.stdout.decode("utf-8").rstrip()


def solve_model_start_end_times(model_start_time, model_end_time, setup_obj):

    # model_start_time
    if model_start_time is None:

        # Get the namelist from the sim_object
        nlst_noah =setup_obj.namelist_hrldas['noahlsm_offline']
        start_noah_keys = {'year':'start_year', 'month':'start_month',
                      'day':'start_day', 'hour':'start_hour', 'minute':'start_min'}
        start_noah_times = { kk:nlst_noah[vv] for (kk, vv) in start_noah_keys.items() } 
        model_start_time = datetime.datetime(**start_noah_times)

    elif type(model_start_time) is str:

        # Allow minutes to be optional
        if not bool(re.match('.*:.*', model_start_time)):
            model_start_time += ':00'
        model_start_time = datetime.datetime.strptime(model_start_time, '%Y-%m-%d %H:%M')

    elif type(model_start_time) is not datetime.datetime:

        raise TypeError('model_start_time is NOT one of type: None, str, or datetime.datetime')

    # model_end_time
    if type(model_end_time) is datetime.datetime:

        pass

    elif model_end_time is None:

        # get one of kday or khour, convert it to timedelta
        nlst_noah = setup_obj.namelist_hrldas['noahlsm_offline']
        if 'khour' in nlst_noah.keys():
            duration = {'hours': nlst_noah['khour']}
        elif 'kday' in nlst_noah.keys():
            duration = {'days': nlst_noah['kday']}
        else:
            raise ValueError("Neither KDAY nor KHOUR in the setup's namelist.hrldas.")
        model_end_time = model_start_time + datetime.timedelta(**duration)

    elif type(model_end_time) is str:
        
        # Allow minutes to be optional
        if not bool(re.match('.*:.*', model_end_time)):
            model_end_time += ':00'
        model_end_time = datetime.datetime.strptime(model_end_time, '%Y-%m-%d %H:%M')

    elif type(model_end_time) is datetime.timedelta:

        model_end_time = model_start_time + model_end_time

    elif type(model_end_time) is dict:
        
        model_end_time = model_start_time + datetime.timedelta(**model_end_time)

    else:

        raise TypeError('model_end_time is NOT one of type: datetime.datetime, ' + 
                        'None, str, datetime.timedelta, dict.')

    return model_start_time, model_end_time


def check_file_exist_colon(run_dir, file_str):
    """Takes a file WITH A COLON (not without)."""
    if type(file_str) is not str:
        file_str = str(file_str)
    file_colon = pathlib.PosixPath(file_str)
    file_no_colon = pathlib.PosixPath(file_str.replace(':','_'))
    if (run_dir / file_colon).exists():
        return './' + str(file_colon)
    if (run_dir / file_no_colon).exists():
        return './' + str(file_no_colon)
    return None


def check_job_input_files(job_obj, run_dir):

    # A run object, check it's next (first pending) job for all the dependencies.
    # This is after this jobs namelists are established.
    # Properties of the setup_obj identify some of the required input files.

    def visit_is_file(path, key, value):
        if value is None:
            return False
        return type(value) is str or type(value) is dict

    def visit_not_none(path, key, value):
        return bool(value)

    def visit_str_posix_exists(path, key, value):
        if type(value) is dict:
            return True
        return key, (run_dir / pathlib.PosixPath(value)).exists()

    def remap_nlst(nlst):
        # The outer remap removes empty dicts
        files = remap(nlst,  visit=visit_is_file)
        files = remap(files, visit=visit_not_none)
        exists = remap(files, visit=visit_str_posix_exists)
        return exists

    hrldas_file_dict = remap_nlst(job_obj.namelist_hrldas)
    hydro_file_dict = remap_nlst(job_obj.hydro_namelist)

    # INDIR is a special case: do some regex magic and counting.

    # What are the colon cases? Hydro/nudging restart files
    hydro_file_dict['hydro_nlist']['restart_file'] = \
        bool(check_file_exist_colon(run_dir,
                                    job_obj.hydro_namelist['hydro_nlist']['restart_file']))
    if 'nudging_nlist' in hydro_file_dict.keys():
        hydro_file_dict['nudging_nlist']['nudginglastobsfile'] = \
            bool(check_file_exist_colon(run_dir,
                                        job_obj.hydro_namelist['nudging_nlist']['nudginglastobsfile']))

    hrldas_exempt_list = []
    hydro_exempt_list = ['nudginglastobsfile', 'timeslicepath']

    # Build conditional exemptions.
    if job_obj.hydro_namelist['hydro_nlist']['udmp_opt'] == 0:
        hydro_exempt_list = hydro_exempt_list + ['udmap_file']

    def check_nlst(nlst, file_dict):

        # Scan the dicts for FALSE exempting certain ones for certain configs.
        def visit_missing_file(path, key, value):
            if type(value) is dict:
                return True
            if not value:
                message = 'The namelist file ' + key + ' = ' + \
                          str(get_path(nlst, (path))[key]) + ' does not exist'
                if key not in [*hrldas_exempt_list, *hydro_exempt_list]:
                    raise ValueError(message)
                else:
                    warnings.warn(message)
            return False

        remap(file_dict, visit=visit_missing_file)
        return None

    check_nlst(job_obj.namelist_hrldas, hrldas_file_dict)
    check_nlst(job_obj.hydro_namelist, hydro_file_dict)

    # Check the parameter table files: do the ones in the model match the ones in the rundir?
    # Will this be by construction?

    return None


def job_complete(run_dir):
    if type(run_dir) is str:
        run_dir = libpath.PosixPath(run_dir)
    check_file = run_dir / '.job_not_complete'
    return not(check_file.exists())


def restore_completed_scheduled_job(run_dir):
    while not job_complete(run_dir):
        time.sleep(10)
    return pickle.load(open(run_dir / 'WrfHydroRun.pkl', 'rb'))

