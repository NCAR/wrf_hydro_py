""" Class for individual Job objects """
### External ###
# import subprocess
import re
import os
import sys
#import io

### Local ###
#import jobdb
import misc

class Job(object):  #pylint: disable=too-many-instance-attributes
    """A qsub Job object.

    Initialize either with all the parameters, or with 'qsubstr' a PBS submit script as a string.
    If 'qsubstr' is given, all other arguments are ignored and set using Job.read().


    Variables 
        On cheyenne, PBS attributes are described in `man qsub` and `man pbs_resources`.
        See also: https://www2.cisl.ucar.edu/resources/computational-systems/cheyenne/running-jobs/submitting-jobs-pbs
        A dictionary can be constructed in advance from specification files by the function
        get_sched_args_from_specs().

    Var Name    Cheyenne       default            example         Notes
    -------------------------------------------------------------------------------------------
    name        -N                                "my_job"            
    account     -A                                "NRAL0017"
    
    email_when  -m             "a"                 "abe"
    email_who   -M             "${USER}@ucar.edu"  "johndoe@ucar.edu"

    queue       -q             "regular"           "regular"
    walltime    -l walltime=   "12:00"             "10:00:00" or "10:00" (seconds coerced)

    Sepcify: np + 
    np                                             500             Number of procs
    nodes                                          4               Number of nodes
    ppn                        Default:            24              Number of procs/node
                               machine_spec_file.cores_per_node

    

    command                                        "echo \"hello\" > test.txt"

    sched_name                 "torque"            "slurm"        
    modules

    -*-*-*-*-*-*-*-  FOLLOWING NOT TESTED ON CHEYENNE  -*-*-*-*-*-*-*-

    pmem        -l pmem=                          "2GB"           Default is no restriction.
    exetime     -a                                "1100"          Not tested
    """


    def __init__(self,
                 name=None,
                 account=None,
                 email_when="a",
                 email_who="${USER}@ucar.edu",
                 queue='regular',
                 walltime="12:00", 
                 nodes=None,
                 ppn=None,
                 command=None,
                 sched_name="torque",
                 pmem=None,
                 exetime=None):

        # Check for required inputs
        # TODO: Deal with setting ppn from machine_spec_file.
        # TODO: Set nodes from 
        req_args = { 'name':name, 'account':account, 'nodes':nodes,
                     'ppn':ppn, 'command':command }
        
        def check_req_args(arg_name, arg):
            if not arg:
                raise ValueError(arg_name + " is a required argument.")
   
        [ check_req_args(n,a) for n, a in req_args.items() ]

        # Determine the software and loads the appropriate package
        if sched_name is None:
            sched_name = misc.getsched_name()
        self.sched_name = sched_name

        global misc_pbs

        if self.sched_name is "slurm":
            misc_pbs = __import__("misc_slurm", globals(), locals(), [], 0)
        else:
            misc_pbs = __import__("misc_torque", globals(), locals(), [], 0)


        # Required
        self.name       = name
        self.account    = account
        self.command    = command

        # Defaults in arglist
        self.email_when = email_when
        self.queue      = queue
        self.sched_name = sched_name

        # Extra Coercion 
        self.email_who  = os.path.expandvars(email_who)
        self.walltime   = ':'.join((walltime+':00').split(':')[0:3])

        # Construction
        self.nodes      = int(nodes)
        self.ppn        = int(ppn)
        
        
        # Currently unsupported.
        self.pmem = pmem
        self.exetime = exetime



        ##################################
        # Submission status:

        # jobID
        self.jobID = None   #pylint: disable=invalid-name

    #

    def sub_string(self):   #pylint: disable=too-many-branches
        """ Write Job as a string suitable for self.sched_name """
        if self.sched_name.lower() == "slurm":
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
            
            jobstr += "#SBATCH -J {0}\n".format(self.name)
            if self.account is not None:
                jobstr += "#SBATCH -A {0}\n".format(self.account)
            jobstr += "#SBATCH -t {0}\n".format(self.walltime)
            jobstr += "#SBATCH -n {0}\n".format(self.nodes*self.ppn)
            if self.pmem is not None:
                jobstr += "#SBATCH --mem-per-cpu={0}\n".format(self.pmem)
            if self.qos is not None:
                jobstr += "#SBATCH --qos={0}\n".format(self.qos)
            if self.email != None and self.message != None:
                jobstr += "#SBATCH --mail-user={0}\n".format(self.email)
                if 'b' in self.message:
                    jobstr += "#SBATCH --mail-type=BEGIN\n"
                if 'e' in self.message:
                    jobstr += "#SBATCH --mail-type=END\n"
                if 'a' in self.message:
                    jobstr += "#SBATCH --mail-type=FAIL\n"
            # SLURM does assignment to no. of nodes automatically
            # jobstr += "#SBATCH -N {0}\n".format(self.nodes)
            if self.queue is not None:
                jobstr += "#SBATCH -p {0}\n".format(self.queue)
            jobstr += "{0}\n".format(self.command)

            return jobstr

        else:
            ###Write this Job as a string suitable for torque###


            # TODO JLM: What are the required fields for Torque?
            # account. no default

            # name? default?
            
                        
            jobstr += "#!/bin/bash\n"
            jobstr += "#PBS -N {0}\n".format(self.name)
            jobstr += "#PBS -A {0}\n".format(self.account)

            if self.exetime is not None: jobstr += "#PBS -a {0}\n".format(self.exetime)
            jobstr += "#PBS -l walltime={0}\n".format(self.walltime)
            jobstr += "#PBS -l nodes={0}:ppn={1}\n".format(self.nodes, self.ppn)
            if self.pmem is not None: jobstr += "#PBS -l pmem={0}\n".format(self.pmem)
            if self.qos  is not None: jobstr += "#PBS -l qos={0}\n".format(self.qos)
            if self.queue is not None: jobstr += "#PBS -q {0}\n".format(self.queue)
            if self.email != None and self.message != None:
                jobstr += "#PBS -M {0}\n".format(self.email)
                jobstr += "#PBS -m {0}\n".format(self.message)
            jobstr += "#PBS -V\n"
            jobstr += "#PBS -p {0}\n\n".format(self.priority)
            jobstr += "#auto={0}\n\n".format(self.auto)
            jobstr += "echo \"I ran on:\"\n"
            jobstr += "cat $PBS_NODEFILE\n\n"
            jobstr += "cd $PBS_O_WORKDIR\n"
            jobstr += "{0}\n".format(self.command)


            #JOBNAME=$PBS_JOBNAME
            #JOBID="$PBS_JOBID"
            #ARRAY_INDEX=$PBS_ARRAY_INDEX
            #NODELIST=`cat "${PBS_NODEFILE}"`
            #LAUNCHCMD="mpiexec_mpt"
            
            # CISL suggests users set TMPDIR when running batch jobs on Cheyenne
            #export TMPDIR=/glade/scratch/$USER/temp
            #mkdir -p $TMPDIR

            
            jobstr += "#PBS -l walltime=${wallTime}:00\n"
            jobstr += "#PBS -q $queue\n"
            jobstr += "#PBS -l select=${nNodesM1}:ncpus=36:mpiprocs=36+1:ncpus=${nCoresLeft}:mpiprocs=${nCoresLeft}\n"
            jobstr += "## Not using PBS standard error and out files to capture model output\n"
            jobstr += "## but these hidden files might catch output and errors from the scheduler.\n"
            jobstr += "#PBS -o ${workingDir}/.${theDate}.pbs.stdout\n"
            jobstr += "#PBS -e ${workingDir}/.${theDate}.pbs.stderr\n"
            jobstr += "\n"            
            jobstr += "numJobId=\`echo \${PBS_JOBID} | cut -d'.' -f1\`\n"
            jobstr += "echo PBS_JOBID:  \$PBS_JOBID\n"
            jobstr += "echo numJobId: \$numJobId\n"
            jobstr += "\n"
            
            jobstr += "## To communicate where the stderr/out and job scripts are and their ID\n"
            jobstr += "export cleanRunDateId=${theDate}\n"
            jobstr += "\n"
            
            jobstr += "cd $workingDir\n"
            jobstr += "echo `pwd`\n"
            jobstr += "\n"
            
            jobstr += "numJobId=\$(echo \${PBS_JOBID} | cut -d'.' -f1)\n"
            jobstr += "echo \"mpiexec_mpt $theBinary 2> \${cleanRunDateId}.\${numJobId}.stderr 1> \${cleanRunDateId}.\${numJobId}.stdout\"\n"
            
            jobstr += "mpiexec_mpt $theBinary 2> \${cleanRunDateId}.\${numJobId}.stderr 1> \${cleanRunDateId}.\${numJobId}.stdout\n"
            jobstr += "\n"
            
            jobstr += "mpiexecreturn=\$?\n"
            jobstr += "echo \"mpiexec_mpt return: \$mpiExecReturn\"\n"
            jobstr += "\n"
            
            jobstr += "# Touch these files just to get the cleanRunDateId in their file names.\n"
            jobstr += "# Can identify the files by jobId and replace contents...\n"
            jobstr += "touch ${workingDir}/\${cleanRunDateId}.\${numJobId}.tracejob\n"
            jobstr += "touch ${workingDir}/.\${cleanRunDateId}.\${numJobId}.stdout\n"
            jobstr += "touch ${workingDir}/.\${cleanRunDateId}.\${numJobId}.stderr\n"
            jobstr += "\n"
            
            jobstr += "exit \$mpiExecReturn\n"
            
            return jobstr



    def script(self, filename="submit.sh"):
        """Write this Job as a bash script

        Keyword arguments:
        filename -- name of the script (default "submit.sh")

        """
        with open(filename, "w") as myfile:
            myfile.write(self.sub_string())

    def submit(self, add=True, dbpath=None, configpath=None):
        """Submit this Job using qsub

           add: Should this job be added to the JobDB database?
           dbpath: Specify a non-default JobDB database

           Raises PBSError if error submitting the job.

        """

        try:
            self.jobID = misc_pbs.submit(substr=self.sub_string())
        except misc.PBSError as e:  #pylint: disable=invalid-name
            raise e

        if add:
            db = jobdb.JobDB(dbpath=dbpath, configpath=configpath) #pylint: disable=invalid-name
            status = jobdb.job_status_dict(jobid=self.jobID, jobname=self.name,
                                           rundir=os.getcwd(), jobstatus="?",
                                           auto=self.auto, qsubstr=self.sub_string(),
                                           walltime=misc.seconds(self.walltime),
                                           nodes=self.nodes, procs=self.nodes*self.ppn)
            db.add(status)
            db.close()



def get_sched_args_from_specs(machine_spec_file:   str=None,
                              user_spec_file:      str=None,
                              candidate_spec_file: str=None):

    


