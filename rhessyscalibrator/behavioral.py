"""@package rhessyscalibrator.behavioral

@brief A system for managing calibration sessions and run of RHESSys.
@brief Can by run on laptop/workstation using multiple processors or
on a cluster that runs LSF (by Platform Computing, Inc.) for job 
management.

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
import os, sys
import argparse
import logging
import re
from datetime import datetime
import time

import calibrator
from rhessyscalibrator.calibrator import RHESSysCalibrator
from rhessyscalibrator.model_runner_db2 import *

class RHESSysCalibratorBehavioral(RHESSysCalibrator):
    
    ## Driver class for performing behavioral model runs
    def __init__(self):
        RHESSysCalibrator.__init__(self)
    
    def main(self, args):
        # Set up command line options
        parser = argparse.ArgumentParser(description="Tool for performing behavioral model runs for RHESSys")
        parser.add_argument("-b", "--basedir", action="store", 
                          dest="basedir", required=True,
                          help="Base directory for the calibration session")
        
        parser.add_argument("-s", "--postprocess_session", action="store", type=int,
                          dest="postprocess_id", required=True,
                          help="Post-process session to use for behavioral runs.")
        
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-st", dest="startDate", nargs=4, type=int,
                           help='Start date and time for behavioral model runs, of the form "YYYY M D H"')
        group.add_argument("-c", "--cmdproto", dest="cmdproto",
                           help="Filename of cmd.proto to use for behavioral runs (relative to basedir)")
        
        parser.add_argument('-ed', dest='endDate', required=False, nargs=4, type=int,
                            help='Date date and time for behavioral model runs, of the form "YYYY M D H"')
        
        parser.add_argument("-f", "--behavioral_filter", action="store",
                          dest="behavioral_filter", required=False,
                          default="nse>0.5 and nse_log>0.5",
                          help="SQL where clause to use to determine which runs qualify as behavioral parameters.  E.g. 'nse>0.5 AND nse_log>0.5' (use quotes)")

        parser.add_argument("-u", "--user", action="store",
                          dest="user", required=False, default=os.getlogin(),
                          help="User to associate with the calibration session.  If not supplied, the value of os.getlogin() will be used.")
        
        parser.add_argument("-p", "--project", action="store",
                          dest="project", required=True,
                          help="Name of the project ot associate with the calibration session.")

        parser.add_argument("-j", "--jobs", action="store", type=calibrator.num_jobs_type,
                          dest="processes", required=True,
                          help="The number of simultaneous jobs (runs) to run at any given time in the calibration session (e.g. --jobs 32). Maximum is %s." % (calibrator.MAX_PROCESSORS,) ) 

        parser.add_argument("--simulator_path", action="store", 
                            dest="simulator_path", required=False,
                            help="Set path for LSF simulator.  When supplied, jobs will be submitted to the simulator, not via actual LSF commands.  Must be the absolute path (e.g. /Users/joeuser/rhessys_calibrator/lsf-sim)")

        parser.add_argument("-q", "--queue", action="store",
                          dest="queue_name", required=False,
                          help="Set queue name to submit jobs to using the underlying queue manager.  " +
                               "Applies only to non-process-based calibration runners (specified by parallel_mode option).")

        parser.add_argument("--parallel_mode", action="store", 
                          dest="parallel_mode", required=False,
                          default='lsf', choices=calibrator.PARALLEL_MODES,
                          help="Set method to use for running jobs in parallel.")

        parser.add_argument("--polling_delay", action="store", type=calibrator.polling_delay_type, 
                          dest="polling_delay", required=False,
                          default=1,
                          help="[ADVANCED] Set multiplier for how long to wait in between successive pollings of job status.  Default polling delay is 60 seconds, thus a multiplier of 5 will result in a delay of 5 minutes instead of 1 minute.  Maximum is %d." % (calibrator.MAX_POLLING_DELAY_MULT,) )

        parser.add_argument("--bsub_exclusive_mode", action="store_true",
                          dest="bsub_exclusive_mode", required=False,
                          help="[ADVANCED] For LSF parallel mode: run bsub with arguments \"-n 1 -R 'span[hosts=1]' -x\" to ensure jobs only run exclusively (i.e. the only job on a node). This can be useful for models that use a lot of memory.")

        parser.add_argument("--mem_limit", action="store", type=int, 
                          dest="mem_limit", required=False,
                          default=4,
                          help="For non-process based parallel modes: Specify memory limit for jobs.  Unit: gigabytes  Defaults to 4.")

        parser.add_argument("--wall_time", action="store",
                            type=int, dest="wall_time",
                            help="For PBS-based parallel mode: Specify wall time in hours that jobs should take.")

        parser.add_argument("-l", "--loglevel", action="store",
                          dest="loglevel", default="OFF", choices=['OFF', 'DEBUG', 'CRITICAL'], required=False,
                          help="Set logging level")
        
        options = parser.parse_args()
        
        # Handle command line parameters
        if "DEBUG" == options.loglevel:
            self._initLogger(logging.DEBUG)
        elif "CRITICAL" == options.loglevel:
            self._initLogger(logging.CRITICAL)
        else:
            self._initLogger(logging.NOTSET)
         
        if options.parallel_mode != calibrator.PARALLEL_MODE_PROCESS and not options.queue_name:
            sys.exit("""Please specify a queue name that is valid for your system.""")
            
        wall_time = None
        if options.parallel_mode == calibrator.PARALLEL_MODE_PBS and options.wall_time:
            if options.wall_time < 1 or options.wall_time > 168:
                sys.exit("Wall time must be greater than 0 and less than 169 hours")
            wall_time = options.wall_time
            
        if not os.path.isdir(options.basedir) or not os.access(options.basedir, os.R_OK):
            sys.exit("Unable to read project directory %s" % (options.basedir,) )
        self.basedir = os.path.abspath(options.basedir) 
            
        self.logger.debug("parallel mode: %s" % options.parallel_mode)
        self.logger.debug("basedir: %s" % self.basedir)
        self.logger.debug("user: %s" % options.user)
        self.logger.debug("project: %s" % options.project)
        self.logger.debug("jobs: %d" % options.processes)
               
        if options.startDate:
            if not options.endDate:
                sys.exit("You must specify a simulation end date")
            startDate = datetime(options.startDate[0], 
                                 options.startDate[1],
                                 options.startDate[2],
                                 options.startDate[3])
            endDate = datetime(options.endDate[0], 
                               options.endDate[1],
                               options.endDate[2],
                               options.endDate[3])
            # Make sure start date is before end date
            if startDate >= endDate:
                sys.exit("Start date %s is not before end date %s" % (str(startDate), str(endDate)) )
            startDateStr = ' '.join([str(d) for d in options.startDate])
            endDateStr = ' '.join([str(d) for d in options.endDate])
        
            readCmdProtoFromRun = True
        
            self.logger.debug("start date: %s" % (startDate,) )
            self.logger.debug("end date: %s" % (endDate,) )
        if options.cmdproto:
            cmdProtoPath = os.path.join(self.basedir, options.cmdproto)
            if not os.access(cmdProtoPath, os.R_OK):
                sys.exit("Unable to read behavioral cmd.proto: %s" % (cmdProtoPath,) )
            
            readCmdProtoFromRun = False
            
            self.logger.debug("behavioral cmd.proto: %s" % (cmdProtoPath,) )
        
        notes = "Behavioral run, using filter: %s" % (options.behavioral_filter,)

        try:
            dbPath = RHESSysCalibrator.getDBPath(self.basedir)
            self.calibratorDB = ModelRunnerDB2(dbPath)
        
            # Get post-process session
            postproc = self.calibratorDB.getPostProcess(options.postprocess_id)
            if None == postproc:
                raise Exception("Post-process session %d was not found in the calibration database %s" % (options.postprocess_id, dbPath))
            # Get session
            calibSession = self.calibratorDB.getSession(postproc.session_id)
            if None == calibSession:
                raise Exception("Session %d was not found in the calibration database %s" % (postproc.session_id, dbPath))
            calibItr = calibSession.iterations

            # Get runs in calibration session
            runs = self.calibratorDB.getRunsInPostProcess(postproc.id, where_clause=options.behavioral_filter)
            numRuns = len(runs)
            print("\n{0}\n".format(notes))
            response = raw_input("%d runs selected of %d total runs (%.2f%%) in post process session %d, calibration session %d, continue? [yes | no] " % \
                                (numRuns, calibItr, (float(numRuns) / float(calibItr)) * 100, options.postprocess_id, postproc.session_id ) )
            response = response.lower()
            if response != 'y' and response != 'yes':
                # Exit normally
                return 0
            self.logger.debug("%d runs selected" % (numRuns,) )
            
            # Make sure we have everything we need to run behavioral runs    
            # Get list of worldfiles
            self.worldfiles = self.getWorldfiles(self.basedir)
            if len(self.worldfiles) < 1:
                raise Exception("No worldfiles found")
            self.logger.debug("worldfiles: %s" % self.worldfiles)          
            
            # Get tecfile name
            (res, tecfilePath) = self.getTecfilePath(self.basedir)
            if not res:
                raise Exception("No tecfile found")
   
            # Get RHESSys executable path
            (rhessysExecFound, rhessysExec, rhessysExecPath) = \
                self.getRHESSysExecPath(self.basedir)
            if not rhessysExecFound:
                raise Exception("RHESSys executable not found")
            
            if readCmdProtoFromRun:
                # Rewrite cmd_proto to use dates from command line
                cmd_proto = re.sub("-st (\d{4} \d{1,2} \d{1,2} \d{1,2})",
                                   "-st %s" % (startDateStr,),
                                   calibSession.cmd_proto)
                cmd_proto = re.sub("-ed (\d{4} \d{1,2} \d{1,2} \d{1,2})",
                                   "-ed %s" % (endDateStr,),
                                   cmd_proto)
                self.logger.debug("Original cmd.proto: %s" % (calibSession.cmd_proto,) )
                self.logger.debug("Behavioral cmd.proto: %s" % (cmd_proto,) )
            else:
                # Use cmd proto from file
                fd = open(cmdProtoPath)
                cmd_proto = fd.read()
                fd.close()
            
            # Strip any parameter ranges from cmd.proto
            cmd_proto_noparam = self.stripParameterRangesFromCmdProto(cmd_proto)
            
            # Pre-process cmd.proto to add rhessys exec and tecfile path
            cmd_proto_pre = self.preProcessCmdProto(cmd_proto_noparam,
                                                    os.path.join(rhessysExecPath, rhessysExec),
                                                    tecfilePath)
            
            # Check for explicit routing and surface flowtable in cmd_proto, get dicts of
            # flowtables from basedir
            (self.flowtablePath, self.surfaceFlowtablePath) = self.determineRouting(cmd_proto_noparam)
            
            # Create behavioral session
            self.session = self.createCalibrationSession(options.user, 
                                                         options.project,
                                                         numRuns,
                                                         options.processes,
                                                         self.basedir,
                                                         notes,
                                                         cmd_proto_noparam)
            # Create postprocess session
            behave_postproc_id = self.calibratorDB.insertPostProcess(self.session.id, postproc.obs_filename, postproc.fitness_period,
                                                                     exclude_date_ranges=postproc.exclude_date_ranges)
            
            # Initialize CalibrationRunner consumers for executing jobs
            (runQueue, consumers) = \
                RHESSysCalibrator.initializeCalibrationRunnerConsumers(self.basedir, self.logger,
                                                                       self.session.id, options.parallel_mode, options.processes, options.polling_delay,
                                                                       options.queue_name, 
                                                                       mem_limit=options.mem_limit,
                                                                       wall_time=wall_time, 
                                                                       bsub_exclusive_mode=options.bsub_exclusive_mode,
                                                                       simulator_path=options.simulator_path)
            
            # Dispatch runs to consumer
            # Note: we're iterating over behavioral runs to get their paramter values
            for (i, run) in enumerate(runs):
                itr = i + 1
                # Get parameters for run
                parameterValues = run.getCalibrationParameters()
                itr_cmd_proto = self.addParametersToCmdProto(cmd_proto_pre,
                                                             parameterValues)
                # For each world file
                for worldfile in self.worldfiles.keys():
                    self.logger.critical("Iteration %d, worldfile: %s" %
                                         (itr, worldfile))
                    # Create new ModelRun2 object for this run
                    behavioralRun = ModelRun2()
                    behavioralRun.session_id = self.session.id
                    behavioralRun.worldfile = worldfile
                    behavioralRun.setCalibrationParameters(parameterValues)
                    # Create new Runfitness2 object for this run, copying fitness parameters so that we can 
                    # draw undercertainty bounds later
                    behavioralRunfit = RunFitness2()
                    behavioralRunfit.postprocess_id = behave_postproc_id
                    behavioralRunfit.nse = run.run_fitness.nse
                    behavioralRunfit.nse_log = run.run_fitness.nse_log
                    behavioralRunfit.pbias = run.run_fitness.pbias
                    behavioralRunfit.rsr = run.run_fitness.rsr
                    behavioralRunfit.runoff_ratio = run.run_fitness.runoff_ratio
                    behavioralRunfit.userfitness = run.run_fitness.userfitness
                    behavioralRun.run_fitness = behavioralRunfit
                    
                    # Add worldfile and flowtable paths to command
                    if self.explicitRouting:
                        if self.surfaceFlowtable:
                            cmd_raw_proto = self.addWorldfileAndFlowtableToCmdProto(\
                                itr_cmd_proto, self.worldfiles[worldfile], 
                                self.flowtablePath[worldfile],
                                self.surfaceFlowtablePath[worldfile])
                        else:
                            cmd_raw_proto = self.addWorldfileAndFlowtableToCmdProto(\
                                itr_cmd_proto, self.worldfiles[worldfile], 
                                self.flowtablePath[worldfile])
                    else:
                        cmd_raw_proto = self.addWorldfileToCmdProto(\
                            itr_cmd_proto, self.worldfiles[worldfile])

                    # Finally, create output_path and generate cmd_raw
                    behavioralRun.output_path = self.createOutputPath(self.basedir,
                                                            self.session.id,
                                                            worldfile,
                                                            itr)
                    behavioralRun.cmd_raw = self.getCmdRawForRun(cmd_raw_proto,
                                                       behavioralRun.output_path)
        
                    if "process" == options.parallel_mode:
                        # Set job ID if we are in process parallel mode
                        #   (in lsf mode, we will use the LSF job number instead of itr)
                        behavioralRun.job_id = itr
        
                    # Dispatch to consumer
                    runQueue.put(behavioralRun)

            time.sleep(5)

            # Wait for all jobs to finish
            self.logger.critical("calling runQueue.join() ...")
            runQueue.join()
            for consumerProcess in consumers:
                consumerProcess.join()

            # Update session endtime and status
            self.calibratorDB.updateSessionEndtime(self.session.id,
                                                   datetime.utcnow(),
                                                   "complete") 
            
            print("\n\nBehavioral results saved to session {0}, post-process session {1}".format(self.session.id, behave_postproc_id))
        except:
            raise
        else:
            self.logger.debug("exiting normally")
            return 0
        finally:
            self.calibratorDB = None
        