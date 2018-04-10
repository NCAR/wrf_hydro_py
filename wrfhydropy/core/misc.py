""" Misc functions for interacting between the OS and the pbs module """

import subprocess
import os
import io
# import re
import datetime
# import time
import sys
from distutils.spawn import find_executable

class PBSError(Exception):
    """ A custom error class for pbs errors """
    def __init__(self, jobid, msg):
        self.jobid = jobid
        self.msg = msg
        super(PBSError, self).__init__()

    def __str__(self):
        return self.jobid + ": " + self.msg

def getsoftware():
    """Tries to find qsub, then sbatch. Returns "torque" if qsub
    is found, else returns "slurm" if sbatch is found, else returns
    "other" if neither is found. """
    if find_executable("qsub") is not None:
        return "torque"
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

def getversion(software=None):
    """Returns the software version """
    if software is None:
        software = getsoftware()
    if software is "torque":
        opt = ["qstat", "--version"]

        # call 'qstat' using subprocess
        p = subprocess.Popen(opt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) #pylint: disable=invalid-name
        stdout, stderr = p.communicate()    #pylint: disable=unused-variable
        sout = io.StringIO(stdout)

        # return the version number
        return sout.read().rstrip("\n").lstrip("version: ")
    elif software is "slurm":
        opt = ["squeue", "--version"]

        # call 'squeue' using subprocess
        p = subprocess.Popen(opt, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) #pylint: disable=invalid-name
        stdout, stderr = p.communicate()    #pylint: disable=unused-variable
        sout = io.StringIO(stdout)

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
