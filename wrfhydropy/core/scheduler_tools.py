""" Tools for interacting between the OS and PBS/slurm """

import subprocess
import os
import io
import re
import datetime
import time
import sys
import warnings
from distutils.spawn import find_executable

#dummy_scheduler = {}

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
        return "other"

def getlogin():
    """Returns os.getlogin(), else os.environ["LOGNAME"], else "?" """
    try:
        return os.getlogin()
    except OSError:
        return os.environ["LOGNAME"]
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
           username=getlogin(),
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


def submit_scheduler(substr, sched_name):
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

        p = subprocess.Popen( "qsub", stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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

def release(jobid):
    """qrls (PBS) or scontrol un-delay (slurm) a job."""
    if sched_name == 'PBS':
        cmd_list = ['qhold', jobid]
    if sched_name == 'slurm':
        cmd_list = ["scontrol", "update", "JobId=", jobid, "StartTime=", "now"]
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
            username=getlogin(),
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


