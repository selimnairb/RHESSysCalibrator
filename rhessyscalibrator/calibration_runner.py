"""@package rhessyscalibrator.calibration_runner

@brief Classes that manage calibration process

This software is provided free of charge under the New BSD License. Please see
the following license information:

Copyright (c) 2013-2015, University of North Carolina at Chapel Hill
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the University of North Carolina at Chapel Hill nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY OF NORTH CAROLINA AT CHAPEL HILL
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


@author Brian Miles <brian_miles@unc.edu>
"""
import os
import stat
from subprocess import *
import thread # _thread in Python 3
import Queue  # queue in Python 3
import string
import re
import time
from datetime import datetime

from rhessyscalibrator.model_runner_db2 import *

class CalibrationRunner(object):
    """ Abstract super class for all consumer objects that run ModelRun  
        objects placed in a dispatch queue by a producer thread.               
        Subclasses must implement the run method.
        
        Subclasses may override default implementation for
        self.jobCompleteCallback() if they wish to perform an action
        when a job is marked as complete.
    """
    def __init__(self, basedir, session_id, queue, db_path, run_path, logger, restart_runs=False):
        """ 
            @param basedir String representing the basedir of the calibration session
            @param session_id Integer representing the session ID of current calibration session
            @param queue Reference to Queue.Queue representing dispatch queue shared with
                                    producer thread
            @param db_path String representing the path of sqlite DB to store the 
                                    job (run) in
            @param run_path String representing the absolute path of the directory from 
                which jobs should be run.
            @param logger logging.Logger to use to for debug messages
            @param restart_runs Boolean indicating that runs are to be restarted 
            (i.e. do not insert new runs into DB)
        """
        self.basedir = basedir
        self.session_id = session_id
        self.queue = queue
        self.db_path = db_path
        self.run_path = run_path
        self.logger = logger
        self.restart_runs = restart_runs

        self.db = ModelRunnerDB2(db_path) 
        self.numActiveJobs = 0
    
    def __del__(self):
        self.db.close()
        self.db = None
    
    def run(self):
        raise NotImplementedError()
    
    def storeJobInDB(self, job):
        """ Store new job in ModelRunnerDB2
        
            @param job ModelRun2 object representing the new job
            
            @note Will set job.id based on ID of inserted run.
        """
        # New run, store in ModelRunnerDB2 (insertRun)
        insertedRunID = self.db.insertRun(job.session_id,
                                          job.worldfile,
                                          job.param_s1, job.param_s2,
                                          job.param_s3, job.param_sv1,
                                          job.param_sv2, job.param_gw1,
                                          job.param_gw2, job.param_vgsen1,
                                          job.param_vgsen2, job.param_vgsen3,
                                          job.param_svalt1, job.param_svalt2,
                                          job.cmd_raw,
                                          job.output_path,
                                          job.job_id)
        job.id = insertedRunID
        if job.run_fitness:
            # Behavioral runs will already have run fitness results
            self.db.insertRunFitnessResults(job.run_fitness.postprocess_id, job.id, 
                                            job.run_fitness.nse, job.run_fitness.nse_log, 
                                            job.run_fitness.pbias, job.run_fitness.rsr, 
                                            job.run_fitness.runoff_ratio, 
                                            job.run_fitness.userfitness)        
    
    def jobCompleteCallback(self, *args, **kwargs):
        """ Called when a job is complete. 
        """
        try:
            self.queue.task_done()
        except ValueError:
            return

class CalibrationRunnerSubprocess(CalibrationRunner):
    """ Class that implements consumer/worker thread behavior for the subprocess
        threading model.

        Subclasses may override default implementation for
        self.jobCompleteCallback() if they wish to perform an action
        when a job is marked as complete.
    """

    QUEUE_GET_TIMEOUT_SECS = 15
    INIT_SLEEP_SECS = 15
    JOB_STATUS_SLEEP_SECS = 60
    JOB_SUBMIT_PENDING_THRESHOLD_SLEEP_SECS = 90

    def __init__(self, basedir, session_id, queue, db_path, run_path, logger, restart_runs=False):
        """ 
            @param basedir String representing the basedir of the calibration session
            @param session_id Integer representing the session ID of current calibration session
            @param queue Reference to Queue.Queue representing dispatch queue shared with
                                    producer thread
            @param db_path String representing the path of sqlite DB to store the 
                                    job (run) in
            @param run_path String representing the absolute path of the directory from 
                which jobs should be run.
            @param logger logging.Logger to use to for debug messages
            @param restart_runs Boolean indicating that runs are to be restarted 
            (i.e. do not insert new runs into DB)
        """
        super(CalibrationRunnerSubprocess, self).__init__(basedir, session_id, queue, 
                                                          db_path, run_path, logger, restart_runs)

    def runJobInSubprocess(self, job):
        """ Run a job using subprocess.  Will add job to DB.
        
            @param job model_runner_db.ModelRun representing the job to run
            
            @raise Exception if bsub output is not what was expected
            @raise Exception if run to restart is not present
        """
        self.logger.critical("Launching job %s in subprocess" % (job.job_id))
        
        if self.restart_runs:
            # Ensure run to restart exists
            run = self.db.getRun(job.id)
            if run is None:
                raise Exception("Run %d does not exist and cannot be restarted" % (job.id,) )
        else:
            # New run, store in DB
            self.storeJobInDB(job)
               
        # Open model process
        process = Popen(job.cmd_raw, shell=True, stdout=PIPE, stderr=PIPE,
                        cwd=self.run_path, bufsize=1)
        
        (process_stdout, process_stderr) = process.communicate()
        
        # Write model command to file
        fileName = "cmd.txt"
        cmdOutFile = os.path.join(self.run_path, job.output_path,
                                  fileName)
        cmdOut = open(cmdOutFile, "w")
        cmdOut.write(job.cmd_raw)
        cmdOut.close()
        
        # Write stdout to file named: 
        #  ${self.run_path}/${job.output_path}/${job.job_id}.out
        fileName = str(job.job_id) + ".out"
        processOutFile = os.path.join(self.run_path, job.output_path,
                                      fileName)
        processOut = open(processOutFile, "w")
        processOut.write(process_stdout)
        processOut.close()
        
        processErrFile = None
        if '' != process_stderr:
            # Write stderr to file named: 
            #  ${self.run_path}/${job.output_path}/${job.job_id}.err
            fileName = str(job.job_id) + ".err"
            processErrFile = os.path.join(self.run_path, job.output_path,
                                             fileName)
            processErr = open(processErrFile, "w")
            processErr.write(process_stderr)
            processErr.close()
          
        if 0 == process.returncode:
            # Update run
            self.db.updateRunEndtime(job.id, datetime.utcnow(), "DONE")
            self.logger.critical("Job %s completed, output written to %s" % 
                          (job.job_id, processOutFile))
        else:
            # Job failed
            self.db.updateRunEndtime(job.id, datetime.utcnow(), "EXIT")
            if processErrFile:
                self.logger.critical("Job %s FAILED, output written to %s" % 
                                     (job.job_id, processErrFile))
            else:
                self.logger.critical("Job %s FAILED" % (job.job_id,) )

    def run(self):
        """ Method to be run in a consumer thread/process to launch a run
            submitted by producer thread/process
        """
        # Wait for a bit for jobs to be submitted to the queue
        time.sleep(self.INIT_SLEEP_SECS)
        while True:
           
            try:
                # Get a job off of the queue
                run = self.queue.get(block=True, 
                                    timeout=self.QUEUE_GET_TIMEOUT_SECS)
                # Run the job
                self.runJobInSubprocess(run)
                self.jobCompleteCallback()

            except Queue.Empty:
                self.logger.critical("Queue.Empty")

                # Kludge to make sure we call self.queue.task_done() enough
                while self.numActiveJobs > 0:
                    try:
                        self.queue.task_done()
                        self.numActiveJobs -= 1
                    except ValueError:
                        break
                break


class CalibrationRunnerQueue(CalibrationRunner):
    """ Abstract class for use with queue-based job management tools.

        Subclasses must be override the run() method to handle implementation-
        specific job queueing suystem.  run methods should check that 
        self.numActiveJobs never exceeds self.max_active_jobs.
    """
    INIT_SLEEP_SECS = 15
    JOB_STATUS_SLEEP_SECS = 60
    JOB_SUBMIT_PENDING_THRESHOLD_SLEEP_SECS = 90
    
    QUEUE_GET_TIMEOUT_SECS = 15
    EXIT_SLEEP_SECS = 5
    
    def getRunCmd(self, *args, **kwargs):
        """ Get job submission command given selected options
         
            @return String representing job submission command
        """
        raise NotImplementedError()
    
    def getRunStatusCmd(self, *args, **kwargs):
        """ Get job status command
        
            @return String representing job status command
        """
        raise NotImplementedError()
    
    def getRunCmdRegex(self):
        """ Get compiled regular expression for parsing run command output
        """
        raise NotImplementedError()
    
    def getRunStatusCmdRegex(self):
        """ Get compiled regular expression for parsing run status 
            command output.  Concrete classes must return a regex
            that matches the job ID in group 1 and the job status 
            in group 2.
        """
        raise NotImplementedError()
    
    def submitJob(self, job):
        """ Submit a job to the underlying queue system.  
        
            @note Will add job to calibration DB.
            
            @param job model_runner_db.ModelRun representing the job to run
            
            @raise Exception if run status command output is not what was 
            expected
        """
        raise NotImplementedError()
    
    def mapStatusCode(self, status_code):
        """ Map between status codes of the underlying queue system
            and calibrator status codes
            
            @param status_code String representing status code of underlying 
                queue system
            
            @return String representing calibrator status code 
        """
        raise NotImplementedError()
    
    def __init__(self, basedir, session_id, queue, 
                 db_path, run_path, logger, restart_runs,
                 submit_queue, polling_delay, mem_limit, max_active_jobs):
        """ 
            Concrete classes must use getRunCmd() and getRunStatusCmd() to 
            initialize self.run_cmd and self.run_status_cmd before the run() method
            can be called.
        
            @param basedir String representing the basedir of the calibration session
            @param session_id Integer representing the session ID of current calibration session
            @param queue Queue that jobs will be read from
            @param db_path String representing the path of sqlite DB to store the 
                job (run) in
            @param run_path String representing the absolute path of the directory from 
                which jobs should be run.
            @param logger logging.Logger to use to for debug messages
            @param restart_runs Boolean indicating that runs are to be restarted 
            
            @param submit_queue String representing the name of the queue to submit
                jobs to
            @param polling_delay Integer representing the multiplier to applied to
                self.JOB_STATUS_SLEEP_SECS
            @param mem_limit Integer representing the memory limit for jobs. Units GB.
            @param max_active_jobs Integer representing the max active jobs permitted
        """
        super(CalibrationRunnerQueue, self).__init__(basedir, session_id, queue, 
                 db_path, run_path, logger, restart_runs)
        
        self.submit_queue = submit_queue
        self.JOB_STATUS_SLEEP_SECS *= polling_delay
        self.mem_limit = mem_limit
        self.max_active_jobs = max_active_jobs
        
        self.run_cmd = None
        self.run_status_cmd = None
        
    def pollJobsStatus(self):
        """ Check status of jobs submitted.  Will update status
            for each job (run) in the DB.
            
            @return Tuple (integer, integer, integer) that represent 
            the number of pending, running, and retired jobs for the 
            current invocation.

            @raise Exception if qstat output is not what was expected.
        """
        numPendingJobs = 0
        numRunningJobs = 0
        numRetiredJobs = 0
        
        statusCmd = self.run_status_cmd
        statusRegex = self.getRunStatusCmdRegex()
        
        # Call status command
        process = Popen(statusCmd, shell=True, stdout=PIPE,
                        cwd=self.run_path)
        (process_stdout, process_stderr) = process.communicate()
        # Read output, foreach job, update status in DB
        for line in string.split(process_stdout, '\n'):
            self.logger.debug('|' + line + '|')
            match = statusRegex.match(line)
            if None == match:
                # Don't choke on header or blank lines of qstat output.
                continue
            job_id = match.group(1)
            run = self.db.getRunInSession(self.session_id, job_id)
            if None == run:
                # The run does not exist (potentially a run not
                # associated with our session, ignore it)
                continue
            
            # Job is ours, get status
            stat = self.mapStatusCode(match.group(2))
            self.logger.debug("job_id: %s, stat: %s; session_id: %s" % 
                              (job_id, stat, self.session_id))
            
            if "DONE" == run.status:
                # This job is already done, continue ...
                continue
            elif "PEND" == stat:
                numPendingJobs += 1
            elif "RUN" == stat:
                numRunningJobs += 1
                
            if run.status != stat:
            # Update status for run (implementation-specific)
                if "DONE" == stat or "EXIT" == stat:
                    self.db.updateRunEndtime(run.id, datetime.utcnow(), stat)
                    numRetiredJobs += 1;
                    #  Job is DONE, call self.jobCompleteCallback
                    self.logger.critical("Job %s (run %s) has completed (numRetired: %d), status set to %s, calling jobCompleteCallback" % \
                                         (job_id, run.id, numRetiredJobs, stat))
                    self.jobCompleteCallback(job_id, run)
                else:
                    self.db.updateRunStatus(run.id, stat)

        self.logger.critical("There are %s jobs pending, and %s jobs running" %
                             (numPendingJobs, numRunningJobs))
        self.logger.critical("numRetiredJobs: %s" % (numRetiredJobs,))
        return (numPendingJobs, numRunningJobs, numRetiredJobs)                    

    def run(self):
        """ Method to be run in a consumer thread/process to launch a run
            submitted by procuder thread/process
        """
        assert(self.run_cmd is not None)
        assert(self.run_status_cmd is not None)
        # Wait for a bit for jobs to be submitted to the queue
        time.sleep(self.INIT_SLEEP_SECS)
        while True:
            pendingJobs = 0
            runningJobs = 0
            retiredJobs = 0
            self.logger.critical("Active jobs: %d" % (self.numActiveJobs))
            self.logger.critical("Queue full: %d" % (self.queue.full()))
            try:
                if self.numActiveJobs < self.max_active_jobs:
                    self.logger.critical("numActiveJobs < %d" % (self.max_active_jobs,))
                    # Only try to run a new job if < self.max_active_jobs
                    #  jobs are currently active
                    run = self.queue.get(block=True, 
                                           timeout=self.QUEUE_GET_TIMEOUT_SECS)
                    # Submit a job
                    self.submitJob(run)
                else:
                    self.logger.critical("numActiveJobs >= max_active_jobs, sleeping ...")
                    time.sleep(self.JOB_STATUS_SLEEP_SECS)

                # Check for job completion
                (pendingJobs, runningJobs, retiredJobs) = \
                    self.pollJobsStatus()                                   
                self.numActiveJobs -= retiredJobs

            except Queue.Empty:
                self.logger.critical("Queue.Empty")

                # Wait for jobs to finish                                   
                (pendingJobs, runningJobs, retiredJobs) = \
                    self.pollJobsStatus()
                self.numActiveJobs -= retiredJobs
                while pendingJobs > 0 or runningJobs > 0:
                    # Make sure we're not too aggressively polling job status
                    time.sleep(self.JOB_STATUS_SLEEP_SECS)
                    (pendingJobs, runningJobs, retiredJobs) = \
                        self.pollJobsStatus()
                    self.numActiveJobs -= retiredJobs

                # Kludge to make sure we call self.queue.task_done() enough
                time.sleep(self.EXIT_SLEEP_SECS)
                while self.numActiveJobs > 0:
                    try:
                        self.queue.task_done()
                        self.numActiveJobs -= 1
                    except ValueError:
                        break
                break
        
            
class CalibrationRunnerLSF(CalibrationRunnerQueue):
    """ CalibrationRunner for use with LSF job management system.

    """            
    def getRunCmd(self, *args, **kwargs):
        """ Get job submission command given selected options
         
            Valid keyword args:
            @param simulator_path String representing path to LSF simulator.  Default: None. If specified
                simulator will be used instead of real LSF commands.
            @param mem_limit Integer representing memory limit for jobs. Unit: GB, Default: 4.
            @param bsub_exclusive_mode Boolean specifying that bsub_exclusive_mode. Default: False
            should be used
         
            @return String representing job submission command
        """
        simulator_path = kwargs.get('simulator_path', None)
        
        run_cmd = None
        if simulator_path:
            run_cmd = os.path.join(simulator_path, "bsub.py")
        else:
            mem_limit = int(kwargs.get('mem_limit', '4'))
            bsub_exclusive_mode = bool(kwargs.get('bsub_exclusive_mode', False))
            
            run_cmd = "bsub -n 1,1"
            if bsub_exclusive_mode:
                run_cmd += """ -R "span[hosts=1]" -x"""
            run_cmd += " -M " + str(mem_limit)
        
        return run_cmd
    
    def getRunStatusCmd(self, *args, **kwargs):
        """ Get job status command
        
            Valid keyword args:
            @param simulator_path String representing path to LSF simulator.  Default: None. If specified
                simulator will be used instead of real LSF commands.
        
            @return String representing job status command
        """
        simulator_path = kwargs.get('simulator_path', None)
        
        if simulator_path:
            return os.path.join(simulator_path, "bjobs.py") 
        else:
            return "bjobs -a"
    
    def getRunCmdRegex(self):
        """ Get compiled regular expression for parsing run command output
        """
        return re.compile("^Job\s<([0-9]+)>\s.+$")
    
    def getRunStatusCmdRegex(self):
        """ Get compiled regular expression for parsing run status 
            command output.  Concrete classes must return a regex
            that matches the job ID in group 1 and the job status 
            in group 2.
        """
        return re.compile("^([0-9]+)\s+\w+\s+(\w+)\s+.+$")
    
    def mapStatusCode(self, status_code):
        """ Map between status codes of the underlying queue system
            and calibrator status codes
            
            @param status_code String representing status code of underlying 
                queue system
            
            @return String representing calibrator status code 
        """
        # Calibrator uses LSF status codes natively, so do nothing here.
        return status_code

    def __init__(self, basedir, session_id, queue, 
                 db_path, run_path, logger, restart_runs,
                 submit_queue, polling_delay, mem_limit, max_active_jobs,
                 bsub_exclusive_mode=False, simulator_path=None):
        super(CalibrationRunnerLSF, self).__init__(basedir, session_id, queue, 
                 db_path, run_path, logger, restart_runs,
                 submit_queue, polling_delay, mem_limit, max_active_jobs)
        
        self.bsub_exclusive_mode = bsub_exclusive_mode
        self.simulator_path = simulator_path
        
        self.run_cmd = self.getRunCmd(simulator_path=self.simulator_path,
                                      bsub_exclusive_mode=self.bsub_exclusive_mode,
                                      mem_limit=self.mem_limit)
        self.run_status_cmd = self.getRunStatusCmd(simulator_path=self.simulator_path)
        
    def submitJob(self, job):
        """ Submit a job using LSF.  Will add job to DB.
         
            @param job model_runner_db.ModelRun representing the job to run
             
            @raise Exception if bsub output is not what was expected
        """
        # Call bsub
        bsub_cmd = self.run_cmd
        if None != self.submit_queue:
            bsub_cmd += " -q " + self.submit_queue
        bsub_cmd += " -o " + job.output_path + " " + job.cmd_raw
        self.logger.debug("Running bsub: %s" % bsub_cmd)
        process = Popen(bsub_cmd, shell=True, stdout=PIPE, stderr=PIPE,
                        cwd=self.run_path, bufsize=1)
         
        (process_stdout, process_stderr) = process.communicate()    
         
        # Read job id from bsub outupt (e.g. "Job <ID> is submitted ...")
        self.logger.critical("stdout from bsub: %s" % process_stdout)
        self.logger.critical("stderr from bsub: %s" % process_stderr)
        match = self.getRunCmdRegex().match(process_stdout)
        if None == match:
            raise Exception("Error while reading output from bsub:\ncmd: %s\n\n|%s|\n%s,\npattern: |%s|" %
                            (bsub_cmd, process_stdout, process_stderr, 
                             self.getRunCmdRegex().pattern))
        self.numActiveJobs += 1
        job.job_id = match.group(1)
         
        if self.restart_runs:
            # Ensure run to restart exists
            run = self.db.getRun(job.id)
            if run is None:
                raise Exception("Run %d does not exist and cannot be restarted" % (job.id,) )
            # Update job_id
            self.db.updateRunJobId(job.id, job.job_id)
        else:
            # New run, store in DB
            self.storeJobInDB(job)
             
        self.logger.critical("Run %s submitted as job %s" % 
                             (job.id, job.job_id))
            
class CalibrationRunnerPBS(CalibrationRunnerQueue):
    """ CalibrationRunner for use with PBS/TORQUE job management system.

    """
    # Map PBS job status codes to our own status codes
    STATUS_MAP = {'C': 'DONE', 'E': 'EXIT', 'H': 'WAIT', 
                  'Q': 'PEND', 'R': 'RUN', 'T': 'UNKWN',
                  'W': 'WAIT', 'S': 'SSUSP'
                  }
    
    def getRunCmd(self, *args, **kwargs):
        """ Get job submission command given selected options
         
            @return String representing job submission command
        """
        return "qsub -d {run_path}".format(run_path=self.run_path)
    
    def getRunStatusCmd(self, *args, **kwargs):
        """ Get job status command
        
            @return String representing job status command
        """
        return "qstat"
    
    def getRunCmdRegex(self):
        """ Get compiled regular expression for parsing run command output
        """
        return re.compile("^([0-9]+\.[0-9A-Za-z-]+).*$")
    
    def getRunStatusCmdRegex(self):
        """ Get compiled regular expression for parsing run status 
            command output.  Concrete classes must return a regex
            that matches the job ID in group 1 and the job status 
            in group 2.
        """
        return re.compile("^(\S+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s+\S+\s+$")
    
    def mapStatusCode(self, status_code):
        """ Map between status codes of the underlying queue system
            and calibrator status codes
            
            @param status_code String representing status code of underlying 
                queue system
            
            @return String representing calibrator status code 
        """
        return self.STATUS_MAP[status_code]

    def __init__(self, basedir, session_id, queue, 
                 db_path, run_path, logger, restart_runs,
                 submit_queue, polling_delay, mem_limit, max_active_jobs,
                 wall_time):
        super(CalibrationRunnerPBS, self).__init__(basedir, session_id, queue, 
                 db_path, run_path, logger, restart_runs,
                 submit_queue, polling_delay, mem_limit, max_active_jobs)
        
        self.run_cmd = self.getRunCmd()
        self.run_status_cmd = self.getRunStatusCmd()
        
        self.wall_time = wall_time
        
    def submitJob(self, job):
        """ Submit a job to the underlying queue system.  
        
            @note Will add job to calibration DB.
            
            @param job model_runner_db.ModelRun representing the job to run
            
            @raise Exception if run status command output is not what was 
            expected
        """
        # Make script for running this model run
        script_filename = os.path.abspath(os.path.join(self.run_path, 
                                                       job.output_path, 'pbs.script'))
        script = open(script_filename, 'w')
        script.write('#!/bin/bash\n\n')
        script.write('#PBS -l nodes=1:ppn=1\n')
        script.write("#PBS -l vmem={mem_limit}gb\n".format(mem_limit=self.mem_limit))
        if self.wall_time:
            script.write("#PBS -l walltime={0}:00:00\n".format(self.wall_time)) # Try to get by without specifying this
        script.write('\n')
        script.write(job.cmd_raw)
        script.write('\n')
        script.close()
        os.chmod(script_filename, 
                 stat.S_IWUSR | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        
        # Build qsub command line
        stdout_file = os.path.abspath(os.path.join(self.run_path, 
                                                   job.output_path, 'pbs.out'))
        stderr_file = os.path.abspath(os.path.join(self.run_path,
                                                   job.output_path, 'pbs.err'))
        qsub_cmd = self.run_cmd
        if None != self.submit_queue:
            qsub_cmd += ' -q ' + self.submit_queue
        qsub_cmd += ' -o ' + stdout_file + ' -e ' + stderr_file
        qsub_cmd += ' ' + script_filename
        
        # Run qsub
        self.logger.debug("Running qsub: %s from path: %s" % (qsub_cmd, self.run_path))
        process = Popen(qsub_cmd, shell=True, stdout=PIPE, stderr=PIPE,
                        cwd=self.run_path, bufsize=1)
        
        (process_stdout, process_stderr) = process.communicate()    
        
        # Read job id from bsub outupt (e.g. "<Job ID>")
        self.logger.critical("stdout from qsub: %s" % process_stdout)
        self.logger.critical("stderr from qsub: %s" % process_stderr)
        match = self.getRunCmdRegex().match(process_stdout)
        if None == match:
            raise Exception("Error while reading output from qsub:\ncmd: %s\n\n|%s|\n%s,\npattern: |%s|" %
                            (qsub_cmd, process_stdout, process_stderr, 
                             self.getRunCmdRegex().pattern))
        self.numActiveJobs += 1
        job.job_id = match.group(1)
        
        if self.restart_runs:
            # Ensure run to restart exists
            run = self.db.getRun(job.id)
            if run is None:
                raise Exception("Run %d does not exist and cannot be restarted" % (job.id,) )
            # Update job_id
            self.db.updateRunJobId(job.id, job.job_id)
        else:
            # New run, store in DB
            self.storeJobInDB(job)
            
        self.logger.critical("Run %s submitted as job %s" % 
                             (job.id, job.job_id))
