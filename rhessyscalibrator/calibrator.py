"""@package rhessyscalibrator.calibrator

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
import sys
import os
import errno
from optparse import OptionParser
import argparse
import logging
from string import Template
import multiprocessing
import re

from rhessyscalibrator.model_runner_db2 import *
from rhessyscalibrator.calibration_runner import *
from rhessyscalibrator.calibration_parameters import *

# Constants
PARALLEL_MODE_LSF = 'lsf'
PARALLEL_MODE_PBS = 'pbs'
PARALLEL_MODE_PROCESS = 'process'
PARALLEL_MODES = [PARALLEL_MODE_LSF, 
                  PARALLEL_MODE_PBS, 
                  PARALLEL_MODE_PROCESS]
DEFAULT_PARALLEL_MODE = PARALLEL_MODE_LSF

MAX_ITERATIONS = 10000
MAX_PROCESSORS = 1024
MAX_POLLING_DELAY_MULT = 60 * 24

FILE_READ_BUFF_SIZE = 4096

DEFAULT_FLOWTABLE_SUFFIX = '_flow_table.dat'
SURFACE_FLOWTABLE_SUFFIX = '_surface_flow_table.dat'

# Custom types for argparse
def num_jobs_type(x):
    x = int(x)
    if x < 1 or x > MAX_PROCESSORS:
        raise argparse.ArgumentTypeError("Simultaneous jobs must be between 1 and %d" % (MAX_PROCESSORS,) )
    return x

def polling_delay_type(x):
    x = int(x)
    if x < 1 or x > MAX_POLLING_DELAY_MULT:
        raise argparse.ArgumentTypeError("Polling delay multiplier must be between 1 and %d" % (MAX_POLLING_DELAY_MULT,) )
    return x



class RHESSysCalibrator(object):

    _dbPath = None
    __obsPath = None
    __rhessysPath = None
    __outputPath = None

    ## Main driver class for rhessys_calibrator tool
    def __init__(self):
        # RE filter used to exclude redefine worldfiles (i.e. those that end in ".Y%4dM%dD%dH%d")
        self.__worldfileFilterRe = re.compile("^.*\.Y[0-9]{4}M[1-9][1-2]{0,1}D[1-9][0-9]{0,1}H[0-9][1-4]{0,1}$")
        # RE filter use to exclude worldfile headers
        self.__worldfileHeaderFilterRe = re.compile("^.*\.hdr$")


    ## Define some class methods that are useful for other classes    
    @classmethod
    def initializeCalibrationRunnerConsumers(cls, basedir, logger, 
                                             session_id, parallel_mode, num_processes, polling_delay, 
                                             queue_name=None, 
                                             mem_limit=None,
                                             wall_time=None,
                                             restart_runs=False,
                                             bsub_exclusive_mode=False,
                                             simulator_path=None):
        """ Initialize a set of one or more CalibrationRunner objects
            to be used for executing calibration runs.
        
            @param
            
            @return Tuple: (multiprocessing.JoinableQueue, [CalibrationRunner 1, ...])
        """
        consumers = []
        runQueue = multiprocessing.JoinableQueue(num_processes)
        
        # Job schedulers will run our jobs us, so there is only one comsumer
        num_consumers = 1
        if PARALLEL_MODE_PROCESS == parallel_mode:
            # We will run our jobs, so we need num_processes consumer processes
            num_consumers = num_processes
        
        for i in range(1, num_consumers + 1):
            # Create CalibrationRunner object (consumer)
            if PARALLEL_MODE_LSF == parallel_mode:
                assert(queue_name is not None)
                consumer = CalibrationRunnerLSF(basedir,
                                                session_id,
                                                runQueue,
                                                RHESSysCalibrator.getDBPath(basedir),
                                                RHESSysCalibrator.getRhessysPath(basedir),
                                                logger,
                                                restart_runs,
                                                queue_name,
                                                polling_delay,
                                                mem_limit,
                                                num_processes,
                                                bsub_exclusive_mode,
                                                simulator_path)
            elif PARALLEL_MODE_PBS == parallel_mode:
                consumer = CalibrationRunnerPBS(basedir,
                                                session_id,
                                                runQueue,
                                                RHESSysCalibrator.getDBPath(basedir),
                                                RHESSysCalibrator.getRhessysPath(basedir),
                                                logger,
                                                restart_runs,
                                                queue_name,
                                                polling_delay,
                                                mem_limit,
                                                num_processes,
                                                wall_time)
            elif PARALLEL_MODE_PROCESS == parallel_mode:
                consumer = CalibrationRunnerSubprocess(basedir,
                                                       session_id,
                                                       runQueue,
                                                       RHESSysCalibrator.getDBPath(basedir),
                                                       RHESSysCalibrator.getRhessysPath(basedir),
                                                       logger,
                                                       restart_runs)
            else:
                consumer = None
            # Create process for consumer
            assert(consumer)
            proc = multiprocessing.Process(target=consumer.run,
                                           args=())
            consumers.append(proc)
            # Start the consumer
            proc.start()
        
        return (runQueue, consumers)
    
    @classmethod
    def getDBPath(cls, basedir):
        """ Returns the path to the DB file relative to basedir's parent
            directory

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path to the DB file relative to basedir's parent
            directory
        """
        path = os.path.join(basedir, 'db', 'calibration.sqlite')
        path_old = os.path.join(basedir, 'db', 'calibration.db')
         
        if not os.path.exists(path):
            if os.path.exists(path_old):
                path = path_old
        
        return path 

    @classmethod
    def getObsPath(cls, basedir):
        """ Returns the path to the directory that stores observed data for 
            a calibration session.  This path is relative to basedir's parent
            directory.

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path to the directory that stores observed data for 
            a calibration session.
        """
        return os.path.join(basedir, "obs")


    @classmethod
    def getRhessysPath(cls, basedir):
        """ Returns the path to the directory that stores RHESSys components,
            e.g.: $BASEDIR/rhessys

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path
        """
        return os.path.join(basedir, "rhessys")

    @classmethod
    def getOutputPath(cls, basedir):
        """ Returns the path to the directory that stores model output
            data for a calibration session.  This path is relative to 
            basedir's parent directory.

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path
        """
        return os.path.join(basedir, "rhessys", "output")

    @classmethod
    def getRunOutputFilePath(cls, run_output_path):
        """ Returns the path to the output file for a particular run, e.g.:
        $BASEDIR/rhessys/output/SESSION_N_worldfile_ITR_I/rhessys_basin.daily
        
            @param run_output_path String representing the directory where output file for
                                        a particular run are stored, e.g.:
                              $BASEDIR/rhessys/output/SESSION_N_worldfile_ITR_I
        
            @return String representing the path of the run output
        """
        return os.path.join(run_output_path, "rhessys_basin.daily")

    def _initLogger(self, level):
        """ Setup logger.  Log to the console for now """
        self.logger = logging.getLogger("cluster_calibrator")
        self.logger.setLevel(level)
        # console handler and set it to level
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(level)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to console handler
        consoleHandler.setFormatter(formatter)
        # add consoleHandler to logger
        self.logger.addHandler(consoleHandler)
        

    def _generateCmdProto(self, surface=False, ranges=True, all_params=False):
        """ Generates a cmd.proto string to be written to a file.

            @param surface True if surface flowtable template variable is to be generated.  Default: False
            @param ranges True if cmd.proto should include parameter range definitions.  Default: True
            @param all_params True if all supported parameters should be included in param.proto.  Default: False
            @return String representing the prototype command
        """
        if surface:
            cmd_proto = """$rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable $surface_flowtable"""
        else:
            cmd_proto = """$rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable"""
        
        if all_params:
            if ranges:
                cmd_proto += """ -pre $output_path -s $s1[0.01, 20.0] $s2[1.0, 150.0] $s3[0.1, 10.0] -sv $sv1[0.01, 20.0] $sv2[1.0, 150.0] -vgsen $vgsen1[0.5, 2.0] $vgsen2[0.5, 2.0] $vgsen3[0.5, 2.0] -svalt $svalt1[0.5, 2.0] $svalt2[0.5, 2.0] -gw $gw1[0.001, 0.3] $gw2[0.01, 0.9]"""
            else:
                cmd_proto += """ -pre $output_path -s $s1 $s2 $s3 -sv $sv1 $sv2 -vgsen $vgsen1 $vgsen2 $vgsen3 -svalt $svalt1 $svalt2 -gw $gw1 $gw2"""
        else:
            if ranges:
                cmd_proto += """ -pre $output_path -s $s1[0.01, 20.0] $s2[1.0, 150.0] $s3[0.1, 10.0] -sv $sv1[0.01, 20.0] $sv2[1.0, 150.0] -gw $gw1[0.001, 0.3] $gw2[0.01, 0.9]"""
            else:
                cmd_proto += """ -pre $output_path -s $s1 $s2 $s3 -sv $sv1 $sv2 -gw $gw1 $gw2"""
        
        return cmd_proto

    
    def _readCmdProtoFromFile(self, basedir):
        """ Read cmd.proto from $BASEDIR/cmd.proto 

            @param basedir String representing the basedir of the calibration session
            
            @return String containing the contents of the cmd.proto. Returns None is cmd.proto was not found
        """
        cmd_proto = None

        file = open(os.path.join(basedir, "cmd.proto"))
        cmd_proto = file.read()
        file.close()

        return cmd_proto

    
    def stripParameterRangesFromCmdProto(self, cmd_proto):
        """ Strip any parameter ranges (e.g. [0.01, 5.0]) from cmd_proto
        
            @param cmd_proto String representing cmd.proto from which parameter ranges are to be stripped
            @return cmd.proto with any parameter ranges removed.
        """
        return re.sub(PARAM_INTERVAL_REGEX, ' ', cmd_proto).strip()
    
    
    def parseCmdProtoForParams(self, cmd_proto, s_for_sv=False):
        """ Parses cmd.proto and determines what calibration parameters
            are specified
            
            @param cmd_proto String representing the cmd.proto
            @param s_for_sv If true use the same m and K parameters for
                               for horizontal as for vertical. Defaults to false (0).

            @return Tuple<String, CalibrationParametersProto> where the
            first element represents the cmd.proto with any parameter ranges removed,
            and the second element is a CalibrationParametersProto instance where parameters
            present in cmd.proto are set to True, and those not found
            in cmd.proto set to false, with parameter ranges set according to
            those defined in cmd.proto.

            @raise Exception if illegal parameter combinations are provided
        """
        paramsProto = CalibrationParametersProto(s_for_sv=s_for_sv)
        paramsProto.parseParameterString(cmd_proto)
        
        # Validate -s parameters
        if (paramsProto.s1 and not paramsProto.s2) or (paramsProto.s2 and not paramsProto.s1):
            raise Exception("Parameters s1 and s2 must be supplied if argument -s is specified")
        
        if paramsProto.s3 and not (paramsProto.s1 and paramsProto.s2):
            raise Exception("Parameters s1 and s2 must be supplied if s3 parameter is specified")
        
        # Validate -sv parameters
        if (paramsProto.sv1 and not paramsProto.sv2) or (paramsProto.sv2 and not paramsProto.sv1):
            raise Exception("Parameters sv1 and sv2 must be supplied if argument -sv is specified")

        # Validate -gw parameters
        if (paramsProto.gw1 and not paramsProto.gw2) or (paramsProto.gw2 and not paramsProto.gw1):
            raise Exception("Parameters gw1 and gw2 must be supplied if argument -gw is specified")

        # Validate -vgsen parameters
        if (paramsProto.vgsen1 and not paramsProto.vgsen2) or (paramsProto.vgsen2 and not paramsProto.vgsen1):
            raise Exception("Parameters vgsen1 and vgsen2 must be supplied if argument -vgsen is specified")
        
        if paramsProto.vgsen3 and not (paramsProto.vgsen1 and paramsProto.vgsen2):
            raise Exception("Parameters vgsen1 and vgsen2 must be supplied if vgsen3 parameter is specified")

        # Validate -svalt parameters
        if (paramsProto.svalt1 and not paramsProto.svalt2) or (paramsProto.svalt2 and not paramsProto.svalt1):
            raise Exception("Parameters svalt1 and svalt2 must be supplied if argument -svalt is specified")
        
        # Strip any parameter ranges from cmd.proto
        cmd_proto_noparam = self.stripParameterRangesFromCmdProto(cmd_proto)
        
        return (cmd_proto_noparam, paramsProto)


    def determineRouting(self, cmd_proto):
        """ Determine whether routing is specified in the cmd_proto (i.e. via -r flag),
              and if so, whether a separate surface flow table is to be used
            
            @param cmd_proto String representing the command proto
            
            @return Tuple (Dict, Dict), the first containing filenames of subsurface flow tables, 
            the second containing filenames of surface flow tables.
        """
        self.explicitRouting = False
        self.surfaceFlowtable = False
        flowtablePath = None
        surfaceFlowtablePath = None
        if string.count(cmd_proto, " -r $flowtable ") > 0:
            self.explicitRouting = True
            if string.count(cmd_proto, " -r $flowtable $surface_flowtable ") > 0:
                self.surfaceFlowtable = True
        
        # If we're running with explicit routing,
        #   Ensure a flow table exists foreach worldfile       
        if self.explicitRouting:
            (flowTablesOK, flowtablePath, surfaceFlowtablePath, withoutFlowTables, withoutSurfaceFlowTables) = \
              self.verifyFlowTables(self.basedir, self.worldfiles.keys(), self.surfaceFlowtable)
            if not flowTablesOK:
                for worldfile in withoutFlowTables:
                    self.logger.debug("Worldfile without flow table: %s" %
                                      worldfile)
                if self.surfaceFlowtable:
                    for worldfile in withoutSurfaceFlowTables:
                        self.logger.debug("Worldfile without surface flow table: %s" %
                                          worldfile)
                raise Exception("Flowtable or surface flowtable not found for each worldfile.  Set debug level to DEBUG or higher to see a list of worldfile without flow tables")
        return (flowtablePath, surfaceFlowtablePath)
    
    def preProcessCmdProto(self, cmd_proto, rhessys, tecfile):
        """ Pre-process cmd_proto, filling in the following values common
            to all runs and iterations: RHESSys executable, tecfile

            @param cmd_proto String representing the contents of the cmd.proto to 
                                  pre-process
            @param rhessys String representing the path to the rhessys executable
            @param tecfile String representing the path to the tecfile

            @return String representing the pre-processed cmd.proto
        """
        template = Template(cmd_proto)

#        print("preProcessCmdProto: rhessys exec: %s" % (rhessys) )
        return template.safe_substitute(rhessys=rhessys, tecfile=tecfile)
             
    def addParametersToCmdProto(self, cmd_proto, params):
        """ Add CalibrationParameters to cmd.proto.  All runs for a given
            iteration will use the same parameters, so only add these once.

            @param cmd_proto String representing the contents of the cmd.proto to add 
                                  parameters to
            @param params CalibrationParameters to 
                                  substitute into cmd.proto

            @return String representing the cmd.proto with 
            CalibrationParameters substituted into it.
        """
        template = Template(cmd_proto)
        return template.safe_substitute(params.toDict())


    def addWorldfileAndFlowtableToCmdProto(self, cmd_proto,
                                           worldfile, flowtable, surface_flowtable=None):
        """ Add items specific to a particular run to cmd.proto, these
            items include: worldfile, flowtable

            @param cmd_proto String representing the contents of the cmd.proto to add to
            @param worldfile String represetning the path of the worldfile
            @param flowtable String representing the path of the flowtable
            @param surface_flowtable String representing path of the surface flowtable

            @return String representing the cmd.proto with run-specific
            items substituted to it; a.k.a. the raw command suitable for
            launching a particular run of RHESSys.

            @raise KeyError on error.
        """
        template = Template(cmd_proto)
        if surface_flowtable is not None:
            return template.safe_substitute(worldfile=worldfile, 
                                            flowtable=flowtable,
                                            surface_flowtable=surface_flowtable)
        else:
            return template.safe_substitute(worldfile=worldfile, 
                                            flowtable=flowtable)
        
    def addWorldfileToCmdProto(self, cmd_proto, worldfile):
        """ Add items specific to a particular run to cmd.proto, these
            items include: worldfile

            @param cmd_proto String representing the contents of the cmd.proto to add to
            @param worldfile String representing the path of the worldfile

            @return String representing the cmd.proto with run-specific
            items substituted to it; a.k.a. the raw command suitable for
            launching a particular run of RHESSys.

            @raise KeyError on error.
        """
        template = Template(cmd_proto)
        return template.safe_substitute(worldfile=worldfile)


    def getCmdRawForRun(self, cmd_proto, output_path):
        """ Add output_path to cmd.proto to yield raw_cmd for a given
            run.

            @param cmd_proto String representing he contents of the cmd.proto
            @param output_path String representing the output_path where run output will be
                                    written

            @return String representing the raw command suitable for launching a
            particular run of RHESSys.
            
            @raise KeyError on error.
        """
        # Put a trailing "/rhessys" on the path so that rhessys will correctly
        #  put output in the output directory (without having the names
        #  start with "_"
        output_path_with_slash = output_path + os.sep + "rhessys"
        template = Template(cmd_proto)
        return template.substitute(output_path=output_path_with_slash)


    def createVerifyDirectoryStructure(self, basedir, cmd_proto_all=False, cmd_proto_ranges=True):
        """ Verify that calibration session directory structure is present,
            create directories as needed.

            @param basedir String representing the path to the basedir
            @param cmd_proto_all True if all possible parameters should be included in the template
            cmd.proto (only applies if cmd.proto is generated)
            @param cmd_proto_ranges True if parameter ranges should in included in the template
            cmd.proto (only applies if cmd.proto is generated)

            @raise IOError is basedir is not writeable.  
            @raise OSError if any directory creation fails.  Raises IOError if creation
            of cmd.proto fails.
        """
        # Check for write access in basedir
        if not os.access(basedir, os.W_OK):
            raise IOError(errno.EACCES, "The directory %s is not writable" 
                          % basedir)
        # Create DB directory
        try:
            os.mkdir(os.path.join(basedir, "db"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create rhessys/src directory
        try:
            os.makedirs(os.path.join(basedir, "rhessys", "src"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create rhessys/templates directory
        try:
            os.makedirs(os.path.join(basedir, "rhessys", "templates"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create rhessys/bin directory
        try:
            os.makedirs(os.path.join(basedir, "rhessys", "bin"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create rhessys/worldfiles/active directory
        try:
            os.makedirs(os.path.join(basedir, "rhessys", "worldfiles", 
                                     "active"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create rhessys/flow directory
        try:
            os.mkdir(os.path.join(basedir, "rhessys", "flow"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create rhessys/tecfiles/active directory
        try:
            os.makedirs(os.path.join(basedir, "rhessys", "tecfiles", 
                                     "active"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create rhessys/defs directory
        try:
            os.mkdir(os.path.join(basedir, "rhessys", "defs"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e
        
        # Create rhessys/clim directory
        try:
            os.mkdir(os.path.join(basedir, "rhessys", "clim"))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create output directory
        try:
            os.mkdir(RHESSysCalibrator.getOutputPath(basedir))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create obs directory
        try:
            os.mkdir(RHESSysCalibrator.getObsPath(basedir))
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e

        # Create cmd.proto file
        self._cmd_proto = os.path.join(basedir, "cmd.proto")
        if not os.path.exists(self._cmd_proto):
            try:
                file = open(self._cmd_proto, "w")
                file.write(self._generateCmdProto(ranges=cmd_proto_ranges, all_params=cmd_proto_all))
                file.close()
            except:
                raise


    def _readCmdProto(self):
        """ Read cmd.proto file

            @precondition: self._cmd_proto = os.path.join(basedir, "cmd.proto")

            @return String representing the first line of the cmd.proto file
            (or None if the file is empty)

            @raise IOError if reading cmd.proto fails
        """
        cmd_proto = None
        try:
            self.logger.debug("opening %s" % self._cmd_proto)
            file = open(self._cmd_proto, "r")
            cmd_proto = file.read()
            self.logger.debug("cmd_proto: %s" % cmd_proto)
            file.close()
        except:
            raise

        return cmd_proto


    def getWorldfiles(self, basedir):
        """ Read list of worldfiles from $BASEDIR/rhessys/worldfiles/active 
            Note: Does not check the contents of the files.

            @param basedir String representing the basedir from which to read worldfiles 

            @return Dict of {filename, filepath}.  
            An empty dictionary will be returned
            if no files were present in the worldfile directory.
            Redefine worldfiles, i.e. worldfile ending in ".Y%4dM%dD%dH%d", will
            be excluded. Filepath will be relative to $BASEDIR/rhessys
        """
        worldfiles = dict()

        worldPath = os.path.join(basedir, "rhessys", "worldfiles", "active")
        for entry in os.listdir(worldPath):
            # Only look at non dot files/dir, even though os.listdir says it will not return these
            if entry[0] != '.':
                # Make sure entry is a file
                entryPath = os.path.join(worldPath, entry)
                if os.path.isfile(entryPath):
                    # Test to see if file is readable
                    if os.access(entryPath, os.R_OK):
                        # Exclude redefine worldfiles (i.e. those that end in ".Y%4dM%dD%dH%d")
                        if not self.__worldfileFilterRe.match(entryPath) \
                               and not self.__worldfileHeaderFilterRe.match(entryPath):
                            worldfiles[entry] = os.path.join('worldfiles', 'active', entry)
        return worldfiles


    def verifyFlowTables(self, basedir, worldfiles, surfaceFlowtable=False):
        """ Ensure that one flow table exists in $BASEDIR/rhessys/flow 
            for each worldfile in the list provided.  Assumes flow tables
            are named $WORLDFILE_flow_table.dat

            @param basedir String representing the basedir from which to read flow tables
            @param worldfiles List of strings representing worldfile names
            @param surfaceFlowtable True if surface flowtable is to be used
        
            @return Tuple: (True, {}, {}, [], []) or (False, {}, {}, [], []), where
            the first {} is a dictionary mapping worldfile name to flowtable
            path, the second {} is a dictionary mapping worldfile name to surface
            flow table path, the first [] is a list of worldfiles without flow tables, and
            the second [] is a list of worldfiles without surface flowtables; the lists
            will be empty if first tuple element is True.  The second dict and list will be empty if
            surfaceFlowtable is False.  Flowtable paths will be relative to $BASEDIR/rhessys

            @raise OSError if a flow table file is not readable
        """
        flowtablePath = {}
        surfaceFlowtablePath = {}
        worldfilesWithoutFlowTables = []
        worldfilesWithoutSurfaceFlowTables = []
        flowTablesOK = True

        flowPath = os.path.join(basedir, 'rhessys', 'flow')
        for worldfile in worldfiles:
            flowTmp = worldfile + DEFAULT_FLOWTABLE_SUFFIX
            entryPath = os.path.join(flowPath, flowTmp)
            if os.path.isfile(entryPath):
                # Test to see if file is readable
                if not os.access(entryPath, os.R_OK):
                    raise OSError(errno.EACCES, 
                                  "Flow table %s is not readable" %
                                  entryPath)
                flowtablePath[worldfile] = os.path.join('flow', flowTmp)
            else:
                flowTablesOK = False
                worldfilesWithoutFlowTables.append(worldfile)
            # Check for surface flow tables (if applicable)
            if surfaceFlowtable:
                flowTmp = worldfile + SURFACE_FLOWTABLE_SUFFIX
                entryPath = os.path.join(flowPath, flowTmp)
                if os.path.isfile(entryPath):
                # Test to see if file is readable
                    if not os.access(entryPath, os.R_OK):
                        raise OSError(errno.EACCES, 
                                      "Surface flow table %s is not readable" %
                                      entryPath)
                    surfaceFlowtablePath[worldfile] = os.path.join('flow', flowTmp)
                else:
                    flowTablesOK = False
                    worldfilesWithoutSurfaceFlowTables.append(worldfile)

        return (flowTablesOK, flowtablePath, surfaceFlowtablePath, worldfilesWithoutFlowTables, worldfilesWithoutSurfaceFlowTables)


    def getTecfilePath(self, basedir):
        """ Read name of techfile from $BASEDIR/rhessys/tecfiles/active.  
            Note: Does not check the contents of the file.

            @param basedir String representing the basedir from which to read the tecfile

            @return Tuple: (True, pathtofilename) or (False, None).
            If more than one file is present, there is no guaranty made
            on what will be returned.  Returns None if no tecfile was found
            or if the file is not readable; pathtofilename will be relative to $BASEDIR/rhessys
        """
        pathToFilename = None
        ret = False

        tecPath = os.path.join(basedir, 'rhessys', 'tecfiles', 'active')
        for entry in os.listdir(tecPath):
            # Only look at non dot files/dir, even though os.listdir says it will not return these
            if entry[0] != '.':
                # Make sure entry is a file
                entryPath = os.path.join(tecPath, entry)
                if os.path.isfile(entryPath):
                    # Test to see if file is readable
                    if os.access(entryPath, os.R_OK):
                        tecfile = entry
                        ret = True
                        break

        if ret:
            pathToFilename = os.path.join('tecfiles', 'active', tecfile)
        return (ret, pathToFilename)


    def getRHESSysExecPath(self, basedir):
        """ Verify that RHESSys executable exists as an executable file
            in $BASEDIR/rhessys/bin

            @param basedir String representing the basedir from which to look for the 
                                executable

            @return Tuple: (True, filename, pathtofilename) or 
            (False, None, None).
            If more than one executable is present,
            this is no guarantee made on what will be returned; pathtofilename will be relative to $BASEDIR/rhessys
        """
        filename = None
        pathToFilename = None
        ret = False

        binPath = os.path.join(basedir, 'rhessys', 'bin')
        for entry in os.listdir(binPath):
            # Only look at non dot files/dir, even though os.listdir says it will not return these
            if entry[0] != '.':
                # Make sure entry is a file
                entryPath = os.path.join(binPath, entry)
                if os.path.isfile(entryPath):
                    # Test to see if file is executable
                    if os.access(entryPath, os.X_OK):
                        filename = entry
                        ret = True
                        break

        if ret:
            pathToFilename = os.path.join('bin')
        return (ret, filename, pathToFilename)

    
    def createOutputPath(self, basedir, session_id, worldfile, iteration):
        """ Generate output_path for a particular worldfile for a particular
            iteration.  Will create directory.

            @param basedir String representing the basedir of the calibration session
            @param session_id String representing the session_id of the session associated
                                  with a particular run
            @param worldfile String representing the name of the worldfile (not including
                                   path elements
            @param iteration Integer representing the iteration number

            @return String of the form 
            output/SESSION_$SESSION_ID_$WORLDFILE_ITR_$ITERATION relative to
            $BASEDIR/rhessys

            @raise OSError if there was a problem creating output_path
        """
        output_path = os.path.join("output",
                                   "SESSION_%d_%s_ITR_%d" %
                                   (session_id, worldfile, iteration))
        # Get path relative to $BASEDIR/.. so that we can create output
        #  path
        full_output_path = os.path.join(basedir, "rhessys", output_path)
        # Create output_path
        try:
            os.makedirs(full_output_path)
        except OSError as e:
            # If the directory exists, eat the error
            if e.errno != errno.EEXIST:
                raise e
            
        return output_path
                           
                           
    def createCalibrationSession(self, user, project, iterations,
                                 processes, basedir, notes=None, cmd_proto=None):
        """ Create calibration session for this session in the database 

            @precondition self.calibratorDB point to a valid ModelRunnerDB

            @param user String representing the user associated with the session
            @param project String representing the project associated with the session
            @param iterations Integer representing the number of times each worldfile is to be 
                                  run in the calibration session
            @param processes Integer representing the number of simultaneous jobs (runs) to run 
                                   at any given time in the calibration session
            @param basedir String representing the basedir wherein session files and 
                                   directories are stored
            @param notes String representing notes associated with the session
            @param cmd_proto String representing cmd_proto to use for runs in session.  
                     If None, cmd.proto will be read from basedir.

            @return model_runner_db.ModelSession object 
            representing the session that was created in the database.
            
            @raise IOError is reading cmd.proto fails
        """
        # Read cmd.proto
        if cmd_proto is None:
            cmd_proto = self._readCmdProto()

        # Create the CalibrationSession
        sessionID = self.calibratorDB.insertSession(user, project, notes,
                                                    iterations, processes,
                                                    basedir, cmd_proto)
        assert sessionID != None
        
        # Could eliminate DB round trip if we just pack our own object,
        #  but this tests that insertSession succeeded
        session = self.calibratorDB.getSession(sessionID)

        return session
                  

    def main(self, args):

        # Set up command line options
        parser = OptionParser(usage="""%prog --basedir=BASEDIR --project=PROJECT_NAME\n--iterations=NUM_ITERATIONS --jobs=NUM_JOBS [optional arguments ...]

Run "%prog --help" for detailed description of all options

%prog is a system for managing calibration sessions and run of
RHESSys.  A session contains many runs.  Each run represents a
distinct and uniformly random set of sensitiviy parameters.  
%prog uses a database (sqlite3) to keep track of each 
session and run.  The program handles launching and management of each
run, and will update its database as run jobs are submitted, begin
running, and finish (either with or without error).  Note that by
defaul RHESSys erroneously returns "1" when it exits without error
(counter to convention).  For job management tools to be able to
corectly sense that RHESSys has exited without error, RHESSys source
must be patched to return "0" on a normal exit.

%prog does not handle calibration session post processing (i.e.
calibrating Nash-Sutcliffe efficiency v. observed values).  This is a
done by a companion program named cluster_calibrator_results.py.

Before running %prog to perform calibrations, it is first 
necessary to create the session directory structure.  This is done by
issuing the --create argument, along with the always-required
--basedir argument.  This will create the following directory
structure within $BASEDIR:

db/                        Where the session DB will be stored (by
                            %prog)
rhessys/src/               Where you should place your RHESSys source code (optional).
rhessys/bin/               Where you will place your RHESSys binary.  (if more than one
                            executable is present, there is no guarantee as to which will
                            be used.)
rhessys/worldfiles/active/ Where you will place world files for which 
                            calibration should be performed.  (All worldfiles
                            in this directory will be calibrated as part of the
                            session)
rhessys/flow/              Where you will place flow tables associated with
                            each worldfile to be calibrated. Flow table filename must be 
                            of the form $(WORLDFILE_NAME)_flow_table.dat. Surface flow table
                            filename must be of the form $(WORLDFILE_NAME)_surface_flow_table.dat.
rhessys/tecfiles/active    Where you will place the TEC file used in your 
                            calibration runs (only one will be used, with
                            no guaranty of what will be used if there is
                            more than one file in this directory)
rhessys/defs/              Where you will place default files referenced in 
                            your worldfiles
rhessys/clim/              Where you will place climate data referenced in your
                            worldfiles
rhessys/output/            Where output for each run will be stored.  Each 
                            run's output will be stored in a directory named
                            "SESSION_$SESSION_ID$WORLDFILE_ITR_$ITR" where 
                            $SESSION_ID is the session associated with a run,
                            $ITR is the iteration and $WORLDFILE corresponds 
                            to a worldfile listed in
                            $BASEDIR/rhessys/worldfiles/active
obs/                       Where you will store observed data to be compared to
                            data from calibration model runs.
""")

        parser.add_option("-b", "--basedir", action="store", type="string", 
                          dest="basedir",
                          help="[REQUIRED] base directory for the calibration session")
        parser.add_option("-c", "--create", action="store_true", dest="create",
                          help="[OPTIONAL] create calibration session directory structure within $BASEDIR and then exit (this will not overwrite or delete any existing files or directories in $BASEDIR).  Once the session directory has been created the file $BASEDIR/cmd.proto will be created.  This file will be used by %prog as a prototype for launching each run in the calibration session.  You will need to edit cmd.proto to suit your modeling needs, however be careful to maintain the formatting conventions (i.e. for the locations of files, and sensitivity parameters) so that %prog will be able to parse cmd.proto")
        
        parser.add_option("--allparams", action="store_true", dest="allparams",
                          help="[OPTIONAL] include all possible parameters in cmd.proto generated.  Note: only applies when 'create' option is specified.")
        parser.add_option("--noparamranges", action="store_true", dest="noparamranges",
                          help="[OPTIONAL] include parameter ranges in cmd.proto generated.  Note: only applies when 'create' option is specified.")
        
        parser.add_option("-u", "--user", action="store", type="string",
                          dest="user",
                          help="[OPTIONAL] user to associate with the calibration session.  If not supplied, the value of os.getlogin() will be used.")
        
        parser.add_option("-p", "--project", action="store", type="string",
                          dest="project",
                          help="[REQUIRED] name of the project ot associate with the calibration session.")

        parser.add_option("-n", "--notes", action="store", type="string",
                          dest="notes",
                          help="[OPTIONAL] notes to associate with the calibration session.")

        parser.add_option("-i", "--iterations", action="store", type="int",
                          dest="iterations",
                          help="[REQUIRED] the number of times each worldfile is to be run in the calibration session (e.g. --iterations=5000).  Maximum value is %d" % MAX_ITERATIONS)

        parser.add_option("-j", "--jobs", action="store", type="int",
                          dest="processes",
                          help="[REQUIRED] the number of simultaneous jobs (runs) to run at any given time in the calibration session (e.g. --jobs=32).  Maximum value is %d" % MAX_PROCESSORS) 

        parser.add_option("-l", "--loglevel", action="store", type="string",
                          dest="loglevel", default="OFF",
                          help="[OPTIONAL] set logging level, one of: OFF [default], DEBUG, CRITICAL (case sensitive)")

        parser.add_option("-s", "--simulator_path", action="store", 
                          type="string", dest="simulator_path",
                          help="[ADVANCED; OPTIONAL] Set path for LSF simulator.  When supplied, jobs will be submitted to the simulator, not via actual LSF commands.  Must be the absolute path (e.g. /Users/joeuser/rhessys_calibrator/lsf-sim)")

        parser.add_option("-q", "--queue", action="store",
                          type="string", dest="queue_name",
                          help="[OPTIONAL] set queue name to submit jobs to using the underlying queue manager.  " +
                               "Applies only to non-process-based calibration runners (specified by parallel_mode option).")

        parser.add_option("--parallel_mode", action="store", 
                          type="string", dest="parallel_mode",
                          help="[OPTIONAL] set method to use for running jobs in parallel, one of: lsf [default], pbs, process")

        parser.add_option("--polling_delay", action="store",
                          type="int", dest="polling_delay",
                          help="[ADVANCED; OPTIONAL] set multiplier for how long to wait in between successive pollings of job status.  Default polling delay is 60 seconds, thus a multiplier of 5 will result in a delay of 5 minutes instead of 1 minute.")

        parser.add_option("--use_horizontal_m_and_K_for_vertical", action="store_true",
                          dest="use_horizontal_m_and_K_for_vertical",
                          help="[ADVANCED; OPTIONAL] use The same m and K parameters for horizontal (i.e. -s) as well as vertical (i.e. -sv ) directions.  Defaults to false.")
        
        parser.add_option("--bsub_exclusive_mode", action="store_true",
                          dest="bsub_exclusive_mode",
                          help="[ADVANCED; OPTIONAL] for LSF parallel mode: run bsub with arguments \"-n 1 -R 'span[hosts=1]' -x\" to ensure jobs only run exclusively (i.e. the only job on a node). This can be useful for models that use a lot of memory.")

        parser.add_option("--mem_limit", action="store",
                          type="int", dest="mem_limit",
                          default=4,
                          help="[OPTIONAL] for non-process based parallel modes: Specify memory limit for jobs.  Unit: gigabytes  Defaults to 4.")
        
        parser.add_option("--wall_time", action="store",
                          type="int", dest="wall_time",
                          help="[OPTIONAL] For PBS-based parallel mode: Specify wall time in hours that jobs should take.")

        (options, args) = parser.parse_args()

        # Enforce initial command line options rules
        if "DEBUG" == options.loglevel:
            self._initLogger(logging.DEBUG)
        elif "CRITICAL" == options.loglevel:
            self._initLogger(logging.CRITICAL)
        else:
            self._initLogger(logging.NOTSET)

        if not options.basedir:
            parser.error("Please specify the basedir for the calibration session")
        self.basedir = os.path.abspath(options.basedir)            

        # Create/verify directory structure in session directory
        try:
            self.createVerifyDirectoryStructure(self.basedir,
                                                cmd_proto_all=options.allparams,
                                                cmd_proto_ranges= not options.noparamranges)

            if options.create:
                # --create option was specified, just exit
                print "Calibration session directory structure created in %s" % self.basedir
                return 0 # exit normally
        except:
            print "Exception ocurred while creating/verifying directory structure"
            raise

        # Enforce remaining command line option rules  
        if not options.iterations:
            parser.error("""Please specify the number of times each worldfile is to be run in the calibration session""")
             
        if not options.processes:
            parser.error("""Please specify the number of simultaneous jobs (runs) to run at any given time in the calibration session""")
   
        if int(options.iterations) > MAX_ITERATIONS:
            parser.error("The maximum number of iterations is %d" % MAX_ITERATIONS)
            
        if int(options.processes) > MAX_PROCESSORS:
            parser.error("The maximum number of jobs is %d" % MAX_PROCESSORS)
        
        if not options.project:
            parser.error("""Please provide the name of the project associated
with the calibration session""")
            
        if not options.user:
            options.user = os.getlogin()

        if not options.notes:
            options.notes = None

        if not options.parallel_mode:
            options.parallel_mode = PARALLEL_MODE_LSF
        elif not options.parallel_mode in PARALLEL_MODES:
            parser.error("""Please specify a valid parallel mode.  See PARALLEL_MODES in %prog""")

        assert( (PARALLEL_MODE_LSF == options.parallel_mode) or 
                (PARALLEL_MODE_PBS == options.parallel_mode) or
                (PARALLEL_MODE_PROCESS == options.parallel_mode))

        if options.parallel_mode == PARALLEL_MODE_LSF and not options.queue_name:
            parser.error("""Please specify a queue name that is valid for your system.""")
        
        wall_time = None
        if options.parallel_mode == PARALLEL_MODE_PBS and options.wall_time:
            if options.wall_time < 1 or options.wall_time > 168:
                parser.error("Wall time must be greater than 0 and less than 169 hours")
            wall_time = options.wall_time
        
        if not options.polling_delay:
            options.polling_delay = 1
            
        if not options.use_horizontal_m_and_K_for_vertical:
            options.use_horizontal_m_and_K_for_vertical = False;

        if not options.bsub_exclusive_mode:
            options.bsub_exclusive_mode = False;

        self.logger.critical("parallel mode: %s" % options.parallel_mode)
        self.logger.debug("basedir: %s" % self.basedir)
        self.logger.debug("user: %s" % options.user)
        self.logger.debug("project: %s" % options.project)
        if None != options.notes:
            self.logger.debug("notes: %s" % options.notes)
        self.logger.debug("iterations: %d" % options.iterations)
        self.logger.debug("jobs: %d" % options.processes)

        # Main events take place herein ...
        try:
            # Make sure we have everything we need to run calibrations        
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
            
            # Read cmd.proto
            cmd_proto = self._readCmdProtoFromFile(self.basedir)
            if None == cmd_proto:
                raise Exception("cmd.proto file not found")
            elif '' == cmd_proto:
                raise Exception("cmd.proto is an empty file")

            # Parse calibrations parameters out of cmd.proto
            (cmd_proto_noparam, paramsProto) = self.parseCmdProtoForParams(cmd_proto, options.use_horizontal_m_and_K_for_vertical)

            # Pre-process cmd.proto to add rhessys exec and tecfile path
            cmd_proto_pre = self.preProcessCmdProto(cmd_proto_noparam,
                                                    os.path.join(rhessysExecPath, rhessysExec),
                                                    tecfilePath)

            # Check for explicit routing and surface flowtable in cmd_proto, get dicts of
            # flowtables from basedir
            (self.flowtablePath, self.surfaceFlowtablePath) = self.determineRouting(cmd_proto_noparam)

            self.logger.debug("DB path: %s" % 
                              RHESSysCalibrator.getDBPath(self.basedir))

            self.calibratorDB = \
                ModelRunnerDB2(RHESSysCalibrator.getDBPath(self.basedir))

            # Create session
            self.session = self.createCalibrationSession(options.user, 
                                                         options.project,
                                                         options.iterations,
                                                         options.processes,
                                                         self.basedir,
                                                         options.notes)

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
            # For each iteration (from 1 to options.iterations+1)
            iterations = options.iterations + 1 # make sure we get all N
            for itr in range(1, iterations):
                # Generate parameter values to use for all worldfiles in 
                #  this iteration
                parameterValues = paramsProto.generateParameterValues()
                itr_cmd_proto = self.addParametersToCmdProto(cmd_proto_pre,
                                                             parameterValues)
                # For each world file
                for worldfile in self.worldfiles.keys():
                    self.logger.critical("Iteration %d, worldfile: %s" %
                                         (itr, worldfile))
                    # Create new ModelRun object for this run
                    run = ModelRun2()
                    run.session_id = self.session.id
                    run.worldfile = worldfile
                    run.setCalibrationParameters(parameterValues)
                    
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
                    run.output_path = self.createOutputPath(self.basedir,
                                                            self.session.id,
                                                            worldfile,
                                                            itr)
                    run.cmd_raw = self.getCmdRawForRun(cmd_raw_proto,
                                                       run.output_path)
        
                    if PARALLEL_MODE_PROCESS == options.parallel_mode:
                        # Set job ID if we are in process parallel mode
                        #   (in non-process mode, we will use the job number given back by the queueing system instead of itr)
                        run.job_id = str(itr)
        
                    # Dispatch to consumer
                    runQueue.put(run)

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
                        

        except:
            raise

        else:
            self.logger.debug("exited normally")
            return 0 # exit normally
        finally:
            # Decrement reference count, this will (hopefully) allow __del__
            #  to be called on the once referenced object
            self.calibratorDB = None

class RHESSysCalibratorRestart(RHESSysCalibrator):
    
    def main(self, args):
        
        # Set up command line options
        parser = argparse.ArgumentParser(description='Restart calibration run')
        parser.add_argument("-b", "--basedir", 
                            dest="basedir", required=True,
                            help="The base directory for the calibration session")
        parser.add_argument("-s", "--session", type=int,
                            dest="session_id", required=True,
                            help="The ID of the session for which model fitness statistics should be calculated")
        parser.add_argument("-l", "--loglevel",
                            dest="loglevel", default="OFF", choices=['OFF', 'DEBUG', 'CRITICAL'],
                            help="Set logging level, one of: OFF [default], DEBUG, CRITICAL (case sensitive)")
        parser.add_argument("-j", "--jobs", type=int,
                            dest="processes",
                            help="The number of simultaneous jobs (runs) to run at any given time in the calibration session (e.g. --jobs=32).  Maximum value is %d" % MAX_PROCESSORS) 
        parser.add_argument("--simulator_path",
                            dest="simulator_path",
                            help="[ADVANCED] set path for LSF simulator.  When supplied, jobs will be submitted to the simulator, not via actual LSF commands.  Must be the absolute path (e.g. /Users/joeuser/rhessys_calibrator/lsf-sim)")
        parser.add_argument("-q", "--queue",
                            dest="queue_name", required=False,
                            help="Set queue name to submit jobs to using the underlying queue manager.  " +
                               "Applies only to non-process-based calibration runners (specified by parallel_mode option).")
        parser.add_argument("--parallel_mode",
                            dest="parallel_mode", choices=PARALLEL_MODES, default=DEFAULT_PARALLEL_MODE,
                            help="Set method to use for running jobs in parallel, one of: lsf [default], pbs, process")
        parser.add_argument("--polling_delay", default=1,
                            type=int, dest="polling_delay",
                            help="[ADVANCED] set multiplier for how long to wait in between successive pollings of job status.  Default polling delay is 60 seconds, thus a multiplier of 5 will result in a delay of 5 minutes instead of 1 minute.")
        parser.add_argument("--use_horizontal_m_and_K_for_vertical", action="store_true",
                            dest="use_horizontal_m_and_K_for_vertical",
                            help="[ADVANCED] use The same m and K parameters for horizontal (i.e. -s) as well as vertical (i.e. -sv ) directions.  Defaults to false.")
        parser.add_argument("--bsub_exclusive_mode", action="store_true",
                            dest="bsub_exclusive_mode",
                            help="[ADVANCED] For LSF parallel mode: run bsub with arguments \"-n 1 -R 'span[hosts=1]' -x\" to ensure jobs only run exclusively (i.e. the only job on a node). This can be useful for models that use a lot of memory.")
        parser.add_argument("--mem_limit",
                            type=int, dest="mem_limit",
                            default=4,
                            help="For non-process based parallel modes: Specify memory limit for jobs.  Unit: gigabytes  Defaults to 4.")
        parser.add_argument("--wall_time", action="store",
                            type=int, dest="wall_time",
                            help="For PBS-based parallel mode: Specify wall time in hours that jobs should take.")


        args = parser.parse_args()
        
        if args.processes > MAX_PROCESSORS:
            sys.exit("The maximum number of jobs is %d" % MAX_PROCESSORS)
        
        # Set up logger
        if "DEBUG" == args.loglevel:
            self._initLogger(logging.DEBUG)
        elif "CRITICAL" == args.loglevel:
            self._initLogger(logging.CRITICAL)
        else:
            self._initLogger(logging.INFO)
        
        if args.parallel_mode != PARALLEL_MODE_PROCESS and not args.queue_name:
            sys.exit("""Please specify a queue name that is valid for your system.""")
    
        wall_time = None
        if args.parallel_mode == PARALLEL_MODE_PBS and args.wall_time:
            if args.wall_time < 1 or args.wall_time > 168:
                sys.exit("Wall time must be greater than 0 and less than 169 hours")
            wall_time = args.wall_time
        
        if not os.path.isdir(args.basedir) or not os.access(args.basedir, os.W_OK):
            sys.exit("Unable to write to basedir %s" % (args.basedir,) )
        self.basedir = os.path.abspath(args.basedir)
        
        dbPath = RHESSysCalibrator.getDBPath(self.basedir)
        if not os.access(dbPath, os.R_OK):
            raise IOError(errno.EACCES, "The database at %s is not readable" %
                          dbPath)
        self.logger.debug("DB path: %s" % dbPath)
        
        # Make sure we have everything we need to run calibrations        
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
        
        # Read cmd.proto
        cmd_proto = self._readCmdProtoFromFile(self.basedir)
        if None == cmd_proto:
            raise Exception("cmd.proto file not found")
        elif '' == cmd_proto:
            raise Exception("cmd.proto is an empty file")

        # Parse calibrations parameters out of cmd.proto
        (cmd_proto_noparam, paramsProto) = self.parseCmdProtoForParams(cmd_proto, args.use_horizontal_m_and_K_for_vertical)

        # Pre-process cmd.proto to add rhessys exec and tecfile path
        cmd_proto_pre = self.preProcessCmdProto(cmd_proto_noparam,
                                                os.path.join(rhessysExecPath, rhessysExec),
                                                tecfilePath)

        # Check for explicit routing and surface flowtable in cmd_proto, get dicts of
        # flowtables from basedir
        (self.flowtablePath, self.surfaceFlowtablePath) = self.determineRouting(cmd_proto_noparam)
        
        try:
            calibratorDB = \
                ModelRunnerDB2(RHESSysCalibrator.getDBPath(self.basedir))
        
            # Make sure the session exists
            self.session = calibratorDB.getSession(args.session_id)
            if None == self.session:
                raise Exception("Session %d was not found in the calibration database %s" % (args.session_id, dbPath))
            if self.session.status != "submitted":
                print( "WARNING: session status is: %s.  Can only restart runs with %s status." % (self.session.status, 'submitted') )
            else:
                self.logger.debug("Session status is: %s" % (self.session.status,))
                
            # Get runs in session
            runs = calibratorDB.getRunsInSession(self.session.id)
            self.numRuns = len(runs) 
            if self.numRuns == 0:
                raise Exception("No runs found for session %d" % (self.session.id,))  
            
            numRunsDone = 0
            minDoneRunId = sys.maxint
            runsDone = []
            runsToRestart = []
            existingRunIds = []
            for run in runs:
                existingRunIds.append(run.id)
                if "DONE" == run.status:
                    if run.id < minDoneRunId:
                        minDoneRunId = run.id
                    runsDone.append(run)
                    numRunsDone += 1
                else:
                    # If it's not done, run it again
                    runsToRestart.append(run)
            existingRunIds = set(existingRunIds)
            # List of available run IDs (will be used for new runs)
            # We have to do it this way as new run IDs won't necessarily be
            # contiguous
            # All possible run IDs
            allRunIds = range(minDoneRunId, minDoneRunId + self.session.iterations)
            allRunIds = set(allRunIds)
            print("All run IDs:")
            print(allRunIds)
            freeRunIds = allRunIds - existingRunIds
            
            print("\nRuns done")
            print([r.id for r in runsDone])
            
            print("\nRuns to restart")
            print([r.id for r in runsToRestart])
            
            print("\nFree run IDs")
            print(freeRunIds)
            
            numToRestart = len(runsToRestart)
            print("Total runs in session: %s" % (self.session.iterations,) )
            print("Completed runs: %s" % (numRunsDone,) )
            print("Runs to be restarted: %s" % (numToRestart,) )
            numNewRuns = self.session.iterations - numRunsDone - numToRestart
            print("New runs: %s" % (numNewRuns,) )
            
            if len(freeRunIds) < numNewRuns:
                sys.exit("The number of free Run IDs (%d) is less than the needed number of new runs (%d)" % (len(freeRunIds), numNewRuns) )
            
            response = raw_input("Continue? [yes | no] " )
            response = response.lower()
            if response != 'y' and response != 'yes':
                # Exit normally
                return(0)
            
            # Dispatch restarted runs to consumer
            print("Restarting %d runs..." % (numToRestart,) )
            # Initialize CalibrationRunner consumers for executing jobs
            (runQueue, consumers) = \
                RHESSysCalibrator.initializeCalibrationRunnerConsumers(self.basedir, self.logger,
                                                                       self.session.id, args.parallel_mode, args.processes, args.polling_delay,
                                                                       args.queue_name, 
                                                                       mem_limit=args.mem_limit, 
                                                                       wall_time=wall_time,
                                                                       bsub_exclusive_mode=args.bsub_exclusive_mode,
                                                                       simulator_path=args.simulator_path,
                                                                       restart_runs=True)
            for run in runsToRestart:
                # Dispatch to consumer
                runQueue.put(run)
        
            time.sleep(5)

            # Wait for all jobs to finish
            self.logger.critical("calling runQueue.join() ...")
            runQueue.join()
            for consumerProcess in consumers:
                consumerProcess.join()
            
            # Dispatch new runs to consumer
            print("Launching %d new runs..." % (numNewRuns,) )
            # TODO: refactor as this code is duplicated from RHESSysCalibrator
            # Initialize CalibrationRunner consumers for executing jobs
            (runQueue, consumers) = \
                RHESSysCalibrator.initializeCalibrationRunnerConsumers(self.basedir, self.logger,
                                                                       self.session.id, args.parallel_mode, args.processes, args.polling_delay,
                                                                       args.queue_name, 
                                                                       mem_limit=args.mem_limit,
                                                                       wall_time=wall_time,
                                                                       bsub_exclusive_mode=args.bsub_exclusive_mode,
                                                                       simulator_path=args.simulator_path)
            
            # For each new run (from 1 to numNewRuns+1)
            iterations = numNewRuns + 1 # make sure we get all N
            for itr in range(1, iterations):
                runId = freeRunIds.pop()
                # Generate parameter values to use for all worldfiles in 
                #  this iteration
                parameterValues = paramsProto.generateParameterValues()
                itr_cmd_proto = self.addParametersToCmdProto(cmd_proto_pre,
                                                             parameterValues)
                # For each world file
                for worldfile in self.worldfiles.keys():
                    self.logger.critical("run.id %d, worldfile: %s" %
                                         (runId, worldfile))
                    # Create new ModelRun object for this run
                    run = ModelRun2()
                    run.session_id = self.session.id
                    run.worldfile = worldfile
                    run.setCalibrationParameters(parameterValues)
                    
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
                    run.output_path = self.createOutputPath(self.basedir,
                                                            self.session.id,
                                                            worldfile,
                                                            runId)
                    run.cmd_raw = self.getCmdRawForRun(cmd_raw_proto,
                                                       run.output_path)
        
                    if PARALLEL_MODE_PROCESS == args.parallel_mode:
                        # Set job ID if we are in process parallel mode
                        #   (in lsf mode, we will use the LSF job number instead of runId)
                        run.job_id = runId
        
                    # Dispatch to consumer
                    runQueue.put(run)

            time.sleep(5)

            # Wait for all jobs to finish
            self.logger.critical("calling runQueue.join() ...")
            runQueue.join()
            for consumerProcess in consumers:
                consumerProcess.join()

            # Update session endtime and status
            calibratorDB.updateSessionEndtime(self.session.id,
                                              datetime.utcnow(),
                                              "complete")
            
        except:
            raise
        else:
            self.logger.debug("exiting normally")
            return(0)
        finally:
            # Decrement reference count, this will (hopefully) allow __del__
            #  to be called on the once referenced object
            calibratorDB = None 