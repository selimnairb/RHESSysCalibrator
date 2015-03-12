"""@package rhessyscalibrator.calibration_runner

@brief Classes that manage calibration process

This software is provided free of charge under the New BSD License. Please see
the following license information:

Copyright (c) 2013, University of North Carolina at Chapel Hill
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
from subprocess import *
import thread # _thread in Python 3
import Queue  # queue in Python 3
import string
import re
import time
from datetime import datetime

from rhessyscalibrator.model_runner_db import *

class CalibrationRunner(object):
    """ Abstract super class for all consumer objects that run ModelRun  
        objects placed in a dispatch queue by a producer thread.               
        Subclasses must implement the run method.
    """
    def run(self):
        pass


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

    def __init__(self, basedir, session_id, queue, db_path, logger, restart_runs=False):
        """ 
            @param basedir String representing the basedir of the calibration session
            @param session_id Integer representing the session ID of current calibration session
            @param queue Reference to Queue.Queue representing dispatch queue shared with
                                    producer thread
            @param db_path String representing the path of sqlite DB to store the 
                                    job (run) in
            @param logger logging.Logger to use to for debug messages
            @param restart_runs Boolean indicating that runs are to be restarted 
            (i.e. do not insert new runs into DB)
        """
        self.basedir = basedir
        self.session_id = session_id
        self._queue = queue
        self.db_path = db_path
        self.logger = logger
        self.restart_runs = restart_runs

        self.db = ModelRunnerDB(db_path)
        
        # active jobs are those jobs that have be submitted via submitJobLSF 
        self.numActiveJobs = 0

        self.__rhessys_base = os.path.join(self.basedir, "rhessys")
        
    def __del__(self):
        self.db.close()
        self.db = None

    def jobCompleteCallback(self, job_id, run):
        """ Called when a job is complete.  Calls self._queue.task_done()

            @param job_id Integer representing the ob_id of the run that completed
            @param run model_runner_db.ModelRun representing the run that has
                                                          completed
        """
        self._queue.task_done()

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
            # New run, store in ModelRunnerDB (insertRun)
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
                                              job.job_id,
                                              job.fitness_period,
                                              job.nse, job.nse_log,
                                              job.pbias, job.rsr,
                                              job.user1, job.user2, job.user3)
            job.id = insertedRunID
        
#        print("runJobInSubprocess: __rhessys_base: %s, cwd: %s, target cwd: %s, cmd: %s" % (self.__rhessys_base, os.getcwd(), os.path.abspath( self.basedir ), job.cmd_raw ) )
        
        # Open model process
        process = Popen(job.cmd_raw, shell=True, stdout=PIPE, stderr=PIPE,
                        cwd=self.__rhessys_base, bufsize=1)
        
        (process_stdout, process_stderr) = process.communicate()
        
        # Write stdout to file named: 
        #  ${self.__rhessys_base}/${job.output_path}/${job.job_id}.out
        fileName = str(job.job_id) + ".out"
        processOutFile = os.path.join(self.__rhessys_base, job.output_path,
                                         fileName)
        processOut = open(processOutFile, "w")
        processOut.write(process_stdout)
        processOut.close()
        
        processErrFile = None
        if '' != process_stderr:
            # Write stderr to file named: 
            #  ${self.__rhessys_base}/${job.output_path}/${job.job_id}.err
            fileName = str(job.job_id) + ".err"
            processErrFile = os.path.join(self.__rhessys_base, job.output_path,
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
                run = self._queue.get(block=True, 
                                      timeout=self.QUEUE_GET_TIMEOUT_SECS)
                # Run the job
                self.runJobInSubprocess(run)
                self.jobCompleteCallback(run.job_id, run)

            except Queue.Empty:
                self.logger.critical("Queue.Empty")

                # Kludge to make sure we call self._queue.task_done() enough
                #time.sleep(self.EXIT_SLEEP_SECS)
                #while self.numActiveJobs > 0:
                #    self._queue.task_done()
                #    self.numActiveJobs -= 1
                break


class CalibrationRunnerLSF(CalibrationRunner):
    """ Abstract class for use with Load Sharing Facility (LSF)              
        job management tools.

        Subclasses must be override the run method to handle implementation-
        specific IPC.  run methods should check that self.numActiveJobs never
        exceeds self.max_active_jobs.

        Subclasses may override default implementation for
        self.jobCompleteCallback() if they wish to perform an action
        when a job is marked as complete.
    """

    INIT_SLEEP_SECS = 15
    JOB_STATUS_SLEEP_SECS = 60
    JOB_SUBMIT_PENDING_THRESHOLD_SLEEP_SECS = 90

    def __init__(self, basedir, session_id, max_active_jobs,
                 db_path, submit_queue, polling_delay, 
                 run_cmd, run_status_cmd, logger, restart_runs=False):
        """ 
            @param basedir String representing the basedir of the calibration session
            @param session_id Integer representing the session ID of current calibration session
            @param max_active_jobs Integer representing the max active jobs permitted
            @param db_path String representing the path of sqlite DB to store the 
                                    job (run) in
            @param submit_queue String representing the name of the queue to submit
                                    jobs to
            @param polling_delay Integer representing the multiplier to applied to
                                    self.JOB_STATUS_SLEEP_SECS
            @param run_cmd String representing the command to use to submit jobs
            @param run_status_cmd String representing the command to use to poll job status
            @param logger logging.Logger to use to for debug messages
            @param restart_runs Boolean indicating that runs are to be restarted 
        """
        self.basedir = basedir
        self.session_id = session_id
        self.max_active_jobs = max_active_jobs
        self.db_path = db_path
        self.submit_queue = submit_queue
        self.JOB_STATUS_SLEEP_SECS *= polling_delay
        self.logger = logger
        self.restart_runs = restart_runs

        self.db = ModelRunnerDB(db_path)
        
        # active jobs are those jobs that have be submitted via submitJobLSF 
        self.numActiveJobs = 0

        self.__bsubCmd = run_cmd
        self.__bjobsCmd = run_status_cmd
        self.__bsubRegex = re.compile("^Job\s<([0-9]+)>\s.+$")
        self.__bsubErrRegex = re.compile("^User <\w+>: Pending job threshold reached. Retrying in 60 seconds...")
        self.__bjobsRegex = re.compile("^([0-9]+)\s+\w+\s+(\w+)\s+.+$")

        self.__rhessys_base = os.path.join(self.basedir, "rhessys")
        
    def __del__(self):
        self.db.close()
        self.db = None

    def jobCompleteCallback(self, job_id, run):
        """ Called when a job is complete.  May be overridden by subclasses.
            Default behavior is a NOP.

            @param job_id Integer representing the LSF job_id of the run that comlpeted
            @param run model_runner_db.ModelRun representing the run that has
                                                          completed
        """
        pass

    def submitJobLSF(self, job):
        """ Submit a job using LSF.  Will add job to DB.
        
            @param job model_runner_db.ModelRun representing the job to run
            
            @raise Exception if bsub output is not what was expected
        """
        # Call bsub
        bsub_cmd = self.__bsubCmd
        if None != self.submit_queue:
            bsub_cmd += " -q " + self.submit_queue
        bsub_cmd += " -o " + job.output_path + " " + job.cmd_raw
        self.logger.debug("Running bsub: %s" % bsub_cmd)
        process = Popen(bsub_cmd, shell=True, stdout=PIPE, stderr=PIPE,
                        cwd=self.__rhessys_base, bufsize=1)
        
        (process_stdout, process_stderr) = process.communicate()    
        
        # Read job id from bsub outupt (e.g. "Job <ID> is submitted ...")
        self.logger.critical("stdout from bsub: %s" % process_stdout)
        self.logger.critical("stderr from bsub: %s" % process_stderr)
        match = self.__bsubRegex.match(process_stdout)
        if None == match:
            raise Exception("Error while reading output from bsub:\ncmd: %s\n\n|%s|\n%s,\npattern: |%s|" %
                            (bsub_cmd, process_stdout, process_stderr, 
                             self.__bsubRegex.pattern))
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
            # New run, store in ModelRunnerDB (insertRun)
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
                                              job.job_id,
                                              job.fitness_period,
                                              job.nse, job.nse_log,
                                              job.pbias, job.rsr,
                                              job.user1, job.user2, job.user3)
            job.id = insertedRunID
            
        self.logger.critical("Run %s submitted as job %s" % 
                             (job.id, job.job_id))

    def pollJobsStatusLSF(self):
        """ Check status of jobs submitted to LSF.  Will update status
            for each job (run) in the DB.
            
            @return Tuple (integer, integer, integer) that represent 
            the number of pending, running, and retired jobs for the 
            current invocation.

            @raise Exception if bjobs output is not what was expected.
        """
        numPendingJobs = 0
        numRunningJobs = 0
        numRetiredJobs = 0
        # Call bjobs
        bjobs_cmd = self.__bjobsCmd + " -a"
        # Run bjobs in the same place as bjobs.  This is only required
        #   so that the simulator to work in cases where the absolute
        #   path to the simulator was not supplied 
        process = Popen(bjobs_cmd, shell=True, stdout=PIPE,
                        cwd=self.__rhessys_base)
        (process_stdout, process_stderr) = process.communicate()
        # Read output, foreach job, update status in DB
        for line in string.split(process_stdout, '\n'):
            self.logger.debug(line)
            match = self.__bjobsRegex.match(line)
            if None == match:
                # Don't choke on the header line of bjobs output, or other
                #  lines that are not the main tabular data.
                continue
            job_id = match.group(1)
            #stat = match.group(2)
            #if "PEND" == stat:
            #    numPendingJobs += 1
            #if "RUN" == stat:
            #    numRunningJobs += 1
            #self.logger.debug("job_id: %s, stat: %s; session_id: %s" % 
            #                  (job_id, stat, self.session_id))
            run = self.db.getRunInSession(self.session_id, job_id)
            if None == run:
                # The run does not exist (potentially a run not
                # associated with our session, ignore it)
                continue
            
            # Job is ours, get status
            stat = match.group(2)
            self.logger.debug("job_id: %s, stat: %s; session_id: %s" % 
                              (job_id, stat, self.session_id))
            
            if "DONE" == run.status:
                # This job is already done, continue ...
                continue
            elif "PEND" == stat:
                numPendingJobs += 1
            elif "RUN" == stat:
                numRunningJobs += 1
            
            # self.logger.critical("run.status: %s, stat: %s" % 
            #                      (run.status, stat))
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


class CalibrationRunnerQueueLSF(CalibrationRunnerLSF):
    """ CalibrationRunner for use with Load Sharing Facility (LSF)
        job management tools.

    """
    QUEUE_GET_TIMEOUT_SECS = 15
    EXIT_SLEEP_SECS = 5

    def __init__(self, basedir, session_id, queue, max_active_jobs,
                 db_path, submit_queue, polling_delay, 
                 run_cmd, run_status_cmd, logger, restart_runs=False):
        """ 
            @param basedir String representing the basedir of the calibration session
            @param session_id Integer representing the session ID of current calibration session
            @param queue Reference to Queue.Queue representing dispatch queue shared with
                        producer thread
            @param max_active_jobs Integer representing the max active jobs permitted
            @param db_path String representing the path of sqlite DB to store the 
                                    job (run) in
            @param submit_queue String representing the name of the queue to submit
                                    jobs to
            @param polling_delay Integer representing the multiplier to applied to
                                    self.JOB_STATUS_SLEEP_SECS
            @param run_cmd String representing the command to use to submit jobs
            @param run_status_cmd String representing the command to use to poll job status
            @param logger logging.Logger to use to for debug messages
            @param restart_runs Boolean indicating that runs are to be restarted 
        """
        super(CalibrationRunnerQueueLSF, self).__init__(basedir, session_id,
                                                        max_active_jobs, 
                                                        db_path,
                                                        submit_queue,
                                                        polling_delay,
                                                        run_cmd, run_status_cmd,
                                                        logger,
                                                        restart_runs=restart_runs)
        self._queue = queue

    def jobCompleteCallback(self, job_id, run):
        """ Called when a job is complete.  Calls self._queue.task_done()

            @param job_id Integer representing the LSF job_id of the run that comlpeted
            @param run model_runner_db.ModelRun representing the run that has
                                                          completed
        """
        self._queue.task_done()

    def run(self):
        """ Method to be run in a consumer thread/process to launch a run
            submitted by procuder thread/process
        """
        # Wait for a bit for jobs to be submitted to the queue
        time.sleep(self.INIT_SLEEP_SECS)
        while True:
            pendingJobs = 0
            runningJobs = 0
            retiredJobs = 0
            self.logger.critical("Active jobs: %d" % (self.numActiveJobs))
            self.logger.critical("Queue full: %d" % (self._queue.full()))
            try:
                if self.numActiveJobs < self.max_active_jobs:
                    self.logger.critical("numActiveJobs < %d" % (self.max_active_jobs,))
                    # Only try to run a new job if < self.max_active_jobs
                    #  jobs are currently active
                    run = self._queue.get(block=True, 
                                           timeout=self.QUEUE_GET_TIMEOUT_SECS)
                    # Submit a job
                    self.submitJobLSF(run)
                else:
                    self.logger.critical("numActiveJobs >= max_active_jobs, sleeping ...")
                    time.sleep(self.JOB_STATUS_SLEEP_SECS)

                # Check for job completion
                (pendingJobs, runningJobs, retiredJobs) = \
                    self.pollJobsStatusLSF()                                   
                self.numActiveJobs -= retiredJobs

            except Queue.Empty:
                self.logger.critical("Queue.Empty")

                # Wait for jobs to finish                                   
                (pendingJobs, runningJobs, retiredJobs) = \
                    self.pollJobsStatusLSF()
                self.numActiveJobs -= retiredJobs
                while pendingJobs > 0 or runningJobs > 0:
                    # Make sure we're not too aggressively polling job status
                    time.sleep(self.JOB_STATUS_SLEEP_SECS)
                    (pendingJobs, runningJobs, retiredJobs) = \
                        self.pollJobsStatusLSF()
                    self.numActiveJobs -= retiredJobs

                # Kludge to make sure we call self._queue.task_done() enough
                time.sleep(self.EXIT_SLEEP_SECS)
                while self.numActiveJobs > 0:
                    self._queue.task_done()
                    self.numActiveJobs -= 1
                break
            
class CalibrationRunnerPBS(CalibrationRunner):
    """ CalibrationRunner for use with PBS/TORQUE job management tools.

    """
    INIT_SLEEP_SECS = 15
    JOB_STATUS_SLEEP_SECS = 60
    JOB_SUBMIT_PENDING_THRESHOLD_SLEEP_SECS = 90
    
    QUEUE_GET_TIMEOUT_SECS = 15
    EXIT_SLEEP_SECS = 5
    
    # Map PBS job status codes to our own status codes
    STATUS_MAP = {'C': 'DONE', 'E': 'EXIT', 'H': 'WAIT', 
                  'Q': 'PEND', 'R': 'RUN', 'T': 'UNKWN',
                  'W': 'WAIT', 'S': 'SSUSP'
                  }

    def __init__(self, basedir, session_id, queue, max_active_jobs,
                 db_path, submit_queue, polling_delay, 
                 run_cmd, run_status_cmd, logger, restart_runs=False):
        """ 
            @param basedir String representing the basedir of the calibration session
            @param session_id Integer representing the session ID of current calibration session
            @param queue String representing the name of the PBS queue to submit
                                    jobs to
            @param max_active_jobs Integer representing the max active jobs permitted
            @param db_path String representing the path of sqlite DB to store the 
                                    job (run) in
            @param submit_queue String representing the name of the queue to submit
                                    jobs to
            @param polling_delay Integer representing the multiplier to applied to
                                    self.JOB_STATUS_SLEEP_SECS
            @param run_cmd String representing the command to use to submit jobs
            @param run_status_cmd String representing the command to use to poll job status
            @param logger logging.Logger to use to for debug messages
            @param restart_runs Boolean indicating that runs are to be restarted 
        """
        self.basedir = basedir
        self.session_id = session_id
        self._queue = queue
        self.max_active_jobs = max_active_jobs
        self.db_path = db_path
        self.submit_queue = submit_queue
        self.JOB_STATUS_SLEEP_SECS *= polling_delay
        self.logger = logger
        self.restart_runs = restart_runs

        self.db = ModelRunnerDB(db_path)
        
        # active jobs are those jobs that have be submitted via submitJobLSF 
        self.numActiveJobs = 0

        self.qsubCmd = run_cmd
        self.qstatCmd = run_status_cmd
        self.qsubRegex = re.compile("^([0-9]+\.\S+)$")
        #self.__bsubErrRegex = re.compile("^User <\w+>: Pending job threshold reached. Retrying in 60 seconds...")
        self.qstatRegex = re.compile("^([0-9]+\.\S+)\s+\S+\s+\S+\s+\S+\s+(\S+)\s\S+$")

        self.rhessys_base = os.path.join(self.basedir, "rhessys")
        
    def __del__(self):
        self.db.close()
        self.db = None

    def jobCompleteCallback(self, job_id, run):
        """ Called when a job is complete.  Calls self._queue.task_done()

            @param job_id Integer representing the LSF job_id of the run that comlpeted
            @param run model_runner_db.ModelRun representing the run that has
                                                          completed
        """
        self._queue.task_done()
        
    def submitJobPBS(self, job):
        """ Submit a job using PBS/TORQUE.  Will add job to DB.
        
            @param job model_runner_db.ModelRun representing the job to run
            
            @raise Exception if qsub output is not what was expected
        """
        # Make script for running this model run
        script_filename = os.path.abspath(os.path.join(self.rhessys_base, 
                                                       job.output_path, 'pbs.script'))
        script = open(script_filename, 'w')
        script.write('#!/bin/bash\n\n')
        script.write(job.cmd_raw)
        script.write('\n')
        script.close()
        
        # Build qsub command line
        stdout_file = os.path.abspath(os.path.join(self.rhessys_base, 
                                                   job.output_path, 'pbs.out'))
        stderr_file = os.path.abspath(os.path.join(self.rhessys_base,
                                                   job.output_path, 'pbs.err'))
        qsub_cmd = self.qsubCmd
        if None != self.submit_queue:
            qsub_cmd += ' -q ' + self.submit_queue
        qsub_cmd += ' -o ' + stdout_file + ' -e ' + stderr_file
        qsub_cmd += ' ' + script_filename
        
        # Run qsub
        self.logger.debug("Running qsub: %s" % qsub_cmd)
        process = Popen(qsub_cmd, shell=True, stdout=PIPE, stderr=PIPE,
                        cwd=self.rhessys_base, bufsize=1)
        
        (process_stdout, process_stderr) = process.communicate()    
        
        # Read job id from bsub outupt (e.g. "<Job ID>")
        self.logger.critical("stdout from qsub: %s" % process_stdout)
        self.logger.critical("stderr from qsub: %s" % process_stderr)
        match = self.qsubRegex.match(process_stdout)
        if None == match:
            raise Exception("Error while reading output from qsub:\ncmd: %s\n\n|%s|\n%s,\npattern: |%s|" %
                            (qsub_cmd, process_stdout, process_stderr, 
                             self.qsubRegex.pattern))
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
            # New run, store in ModelRunnerDB (insertRun)
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
                                              job.job_id,
                                              job.fitness_period,
                                              job.nse, job.nse_log,
                                              job.pbias, job.rsr,
                                              job.user1, job.user2, job.user3)
            job.id = insertedRunID
            
        self.logger.critical("Run %s submitted as job %s" % 
                             (job.id, job.job_id))

    def pollJobsStatusPBS(self):
        """ Check status of jobs submitted to PBS/TORQUE.  Will update status
            for each job (run) in the DB.
            
            @return Tuple (integer, integer, integer) that represent 
            the number of pending, running, and retired jobs for the 
            current invocation.

            @raise Exception if qstat output is not what was expected.
        """
        numPendingJobs = 0
        numRunningJobs = 0
        numRetiredJobs = 0
        
        # Get runs in session
        #runs = self.db.getRunsInSession(self.session_id)
        #job_ids = set([run.job_id for run in runs])
        
        # Call qstat
        qstat_cmd = self.qstatCmd
        process = Popen(qstat_cmd, shell=True, stdout=PIPE,
                        cwd=self.rhessys_base)
        (process_stdout, process_stderr) = process.communicate()
        # Read output, foreach job, update status in DB
        for line in string.split(process_stdout, '\n'):
            self.logger.debug(line)
            match = self.qstatRegex.match(line)
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
            stat = self.STATUS_MAP[match.group(2)]
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
        # Wait for a bit for jobs to be submitted to the queue
        time.sleep(self.INIT_SLEEP_SECS)
        while True:
            pendingJobs = 0
            runningJobs = 0
            retiredJobs = 0
            self.logger.critical("Active jobs: %d" % (self.numActiveJobs))
            self.logger.critical("Queue full: %d" % (self._queue.full()))
            try:
                if self.numActiveJobs < self.max_active_jobs:
                    self.logger.critical("numActiveJobs < %d" % (self.max_active_jobs,))
                    # Only try to run a new job if < self.max_active_jobs
                    #  jobs are currently active
                    run = self._queue.get(block=True, 
                                           timeout=self.QUEUE_GET_TIMEOUT_SECS)
                    # Submit a job
                    self.submitJobPBS(run)
                else:
                    self.logger.critical("numActiveJobs >= max_active_jobs, sleeping ...")
                    time.sleep(self.JOB_STATUS_SLEEP_SECS)

                # Check for job completion
                (pendingJobs, runningJobs, retiredJobs) = \
                    self.pollJobsStatusPBS()                                   
                self.numActiveJobs -= retiredJobs

            except Queue.Empty:
                self.logger.critical("Queue.Empty")

                # Wait for jobs to finish                                   
                (pendingJobs, runningJobs, retiredJobs) = \
                    self.pollJobsStatusPBS()
                self.numActiveJobs -= retiredJobs
                while pendingJobs > 0 or runningJobs > 0:
                    # Make sure we're not too aggressively polling job status
                    time.sleep(self.JOB_STATUS_SLEEP_SECS)
                    (pendingJobs, runningJobs, retiredJobs) = \
                        self.pollJobsStatusPBS()
                    self.numActiveJobs -= retiredJobs

                # Kludge to make sure we call self._queue.task_done() enough
                time.sleep(self.EXIT_SLEEP_SECS)
                while self.numActiveJobs > 0:
                    self._queue.task_done()
                    self.numActiveJobs -= 1
                break