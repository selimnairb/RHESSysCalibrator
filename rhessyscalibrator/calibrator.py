"""@package rhessyscalibrator.calibrator

@brief A system for managing calibration sessions and run of RHESSys.
@brief Can by run on laptop/workstation using multiple processors or
on a cluster that runs LSF (by Platform Computing, Inc.) for job 
management.

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
import sys
import os
import errno
from optparse import OptionParser
import logging
from string import Template
import multiprocessing
#import string

#import re
#from subprocess import *

#from random import *
#import thread # _thread in Python 3
#import Queue  # queue in Python 3
#import multiprocessing
#import time
#from datetime import datetime

from rhessyscalibrator.model_runner_db import *
from rhessyscalibrator.calibration_runner import *
from rhessyscalibrator.calibration_parameters import *

# Constants
LSF_QUEUES = ["day", "debug", "hour", "week", "bigmem"]
PARALLEL_MODES = ["lsf", "process"]

MAX_ITERATIONS = 5120
MAX_PROCESSORS = 512

FILE_READ_BUFF_SIZE = 4096

class RHESSysCalibrator(object):

    _dbPath = None
    __obsPath = None
    __rhessysPath = None
    __outputPath = None

    ## Main driver class for cluster_calibrator tool
    def __init__(self):
        # RE filter used to exclude redefine worldfiles (i.e. those that end in ".Y%4dM%dD%dH%d")
        self.__worldfileFilterRe = re.compile("^.*\.Y[0-9]{4}M[1-9][1-2]{0,1}D[1-9][0-9]{0,1}H[0-9][1-4]{0,1}$")


    ## Define some class methods that are useful for other classes
    @classmethod
    def getDBPath(cls, basedir):
        """ Returns the path to the DB file relative to basedir's parent
            directory

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path to the DB file relative to basedir's parent
            directory
        """
        if not cls._dbPath:
            cls._dbPath = os.path.join(basedir,
                                        "db",
                                        "calibration.db")
        return cls._dbPath

    @classmethod
    def getObsPath(cls, basedir):
        """ Returns the path to the directory that stores observed data for 
            a calibration session.  This path is relative to basedir's parent
            directory.

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path to the directory that stores observed data for 
            a calibration session.
        """
        if not cls.__obsPath:
            cls.__obsPath = os.path.join(basedir,
                                         "obs")
        return cls.__obsPath

    @classmethod
    def getRhessysPath(cls, basedir):
        """ Returns the path to the directory that stores RHESSys components,
            e.g.: $BASEDIR/rhessys

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path
        """
        if not cls.__rhessysPath:
            cls.__rhessysPath = os.path.join(basedir,
                                             "rhessys")
        return cls.__rhessysPath

    @classmethod
    def getOutputPath(cls, basedir):
        """ Returns the path to the directory that stores model output
            data for a calibration session.  This path is relative to 
            basedir's parent directory.

            @param basedir String representing the basedir of the calibration session
            
            @return A string representing the path
        """
        if not cls.__outputPath:
            cls.__outputPath = os.path.join(basedir, 
                                            "rhessys", 
                                            "output")
        return cls.__outputPath

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
        

    def _generateCmdProto(self):
        """ Generates a cmd.proto string to be written to a file.

            @return String representing the prototype command
        """
        #cmd_proto = """$rhessys -st 2003 10 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable -pre $output_path -s $s1 $s2 $s3 -sv $sv1 $sv2 -gw $gw1 $gw2 -vgsen $vgsen1 $vgsen2 $vgsen3 -svalt $svalt1 $svalt2"""
        cmd_proto = """$rhessys -st 2003 10 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable -pre $output_path -s $s1 $s2 -sv $sv1 $sv2 -gw $gw1 $gw2"""
        
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


    def parseCmdProtoForParams(self, cmd_proto, s_for_sv=False):
        """ Parses cmd.proto and determines what calibration parameters
            are specified
            
            @param cmd_proto String representing the cmd.proto
            @param s_for_sv If true use the same m and K parameters for
                               for horizontal as for vertical. Defaults to false (0).

            @return CalibrationParameters instance where parameters
            present in cmd.proto are set to True, and those not found
            in cmd.proto set to false.

            @raise Exception if illegal parameter combinations are provided
        """
        paramsProto = CalibrationParametersProto(s_for_sv=s_for_sv)

        # Check for -s argument
        if string.count(cmd_proto, " -s ") > 0:
            if string.count(cmd_proto, "$s1") > 0:
                if string.count(cmd_proto, "$s2") > 0:
                    paramsProto.s1 = True
                    paramsProto.s2 = True
                else:
                    raise Exception("Parameter s2 must be supplied if parameter s1 is specified")
                if string.count(cmd_proto, "$s3") > 0:
                    # param s3 is optional
                    paramsProto.s3 = True
            else:
                raise Exception("Parameters s1 and s2 must be supplied if -s argument is specified")

        # Check for -sv argument
        if string.count(cmd_proto, " -sv ") > 0:
            if string.count(cmd_proto, "$sv1") > 0:
                if string.count(cmd_proto, "$sv2") > 0:
                    paramsProto.sv1 = True
                    paramsProto.sv2 = True
                else:
                    raise Exception("Parameter sv2 must be supplied if parameter sv1 is specified")
            else:
                raise Exception("Parameters sv1 and sv2 must be supplied if -sv argument is specified")

        # Check for -gw arument
        if string.count(cmd_proto, " -gw ") > 0:
            if string.count(cmd_proto, "$gw1") > 0:
                if string.count(cmd_proto, "$gw2") > 0:
                    paramsProto.gw1 = True
                    paramsProto.gw2 = True
                else:
                    raise Exception("Parameter gw2 must be supplied if parameter gw1 is specified")
            else:
                raise Exception("Parameters gw1 and gw2 must be supplied if -gw argument is specified")

        # Check for -vgsen arguments
        if string.count(cmd_proto, " -vgsen ") > 0:
            if string.count(cmd_proto, "$vgsen1") > 0:
                if string.count(cmd_proto, "$vgsen2") > 0:
                    paramsProto.vgsen1 = True
                    paramsProto.vgsen2 = True
                else:
                    raise Exception("Parameter vgsen2 must be supplied if parameter vgsen1 is specified")
                if string.count(cmd_proto, "$vgsen3") > 0:
                    # param vgsen3 is optional
                    paramsProto.vgsen3 = True
            else:
                raise Exception("Parameters vgsen1 and vgsen2 must be supplied if -vgsen argument is specified")

        # Check for -svalt arguments
        if string.count(cmd_proto, " -svalt ") > 0:
            if string.count(cmd_proto, "$svalt1") > 0:
                if string.count(cmd_proto, "$svalt2") > 0:
                    paramsProto.svalt1 = True
                    paramsProto.svalt2 = True
                else:
                    raise Exception("Parameter svalt2 must be supplied if parameter if svalt1 is specified")
            else:
                raise Exception("Parameters svalt1 and svalt2 must be supplied is -svalt argument is specified")
        

        return paramsProto

    
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
                                           worldfile, flowtable):
        """ Add items specific to a particular run to cmd.proto, these
            items include: worldfile, flowtable

            @param cmd_proto String representing the contents of the cmd.proto to add to
            @param worldfile String represetning the path of the worldfile
            @flowtable String representing the path of the flowtable

            @return String representing the cmd.proto with run-specific
            items substituted to it; a.k.a. the raw command suitable for
            launching a particular run of RHESSys.

            @raise KeyError on error.
        """
        template = Template(cmd_proto)
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


    def createVerifyDirectoryStructure(self, basedir):
        """ Verify that calibration session directory structure is present,
            create directories as needed.

            @param basedir String representing the path to the basedir

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
                file.write(self._generateCmdProto())
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
                        if not self.__worldfileFilterRe.match(entryPath):
                            # Strip $BASEDIR and "rhessys" off of front of worldfile
                            #  path before storing
                            entryPathElem = entryPath.split(os.sep)
                            worldfiles[entry] = os.path.join(entryPathElem[2],
                                                             entryPathElem[3],
                                                             entryPathElem[4])
        return worldfiles


    def verifyFlowTables(self, basedir, worldfiles):
        """ Ensure that one flow table exists in $BASEDIR/rhessys/flow 
            for each worldfile in the list provided.  Assumes flow tables
            are named $WORLDFILE_flow_table.dat

            @param basedir String representing the basedir from which to read flow tables
            @param worldfiles List of strings representing worldfile names
        
            @return Tuple: (True, {}, []) or (False, {}, []), where
            {} is a dictionary mapping worldfile name to flowtable
            path, and [] is a list of worldfiles without flow tables,
            which will be empty if True. Flowtable path will be relative to $BASEDIR/rhessys

            @raise OSError if a flow table file is not readable
        """
        flowtablePath = dict()
        worldfilesWithoutFlowTables = []
        flowTablesOK = True

        flowPath = os.path.join(basedir, "rhessys", "flow")
        for worldfile in worldfiles:
            flowTmp = worldfile + "_flow_table.dat"
            entryPath = os.path.join(flowPath, flowTmp)
            if os.path.isfile(entryPath):
                # Test to see if file is readable
                if not os.access(entryPath, os.R_OK):
                    raise OSError(errno.EACCES, 
                                  "Flow table %s is not readable" %
                                  entryPath)
                # Strip $BASEDIR and "rhessys" off of front of flow table
                #  path before storing                                      
                entryPathElem = entryPath.split(os.sep)
                flowtablePath[worldfile] = os.path.join(entryPathElem[2],
                                                        entryPathElem[3])
            else:
                flowTablesOK = False
                worldfilesWithoutFlowTables.append(worldfile)

        return (flowTablesOK, flowtablePath, worldfilesWithoutFlowTables)


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

        tecPath = os.path.join(basedir, "rhessys", "tecfiles", "active")
        for entry in os.listdir(tecPath):
            # Only look at non dot files/dir, even though os.listdir says it will not return these
            if entry[0] != '.':
                # Make sure entry is a file
                entryPath = os.path.join(tecPath, entry)
                if os.path.isfile(entryPath):
                    # Test to see if file is readable
                    if os.access(entryPath, os.R_OK):
                        # Strip $BASEDIR and "rhessys" off of front of tecfile
                        #  path before storing
                        entryPathElem = entryPath.split(os.sep)
                        pathToFilename = os.path.join(entryPathElem[2],
                                                      entryPathElem[3],
                                                      entryPathElem[4])
                        ret = True
                        break

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

        binPath = os.path.join(basedir, "rhessys", "bin")
        for entry in os.listdir(binPath):
            # Only look at non dot files/dir, even though os.listdir says it will not return these
            if entry[0] != '.':
                # Make sure entry is a file
                entryPath = os.path.join(binPath, entry)
                if os.path.isfile(entryPath):
                    # Test to see if file is executable
                    if os.access(entryPath, os.X_OK):
                        filename = entry
                        # Strip $BASEDIR and "rhessys" off of front of executable
                        #  path before storing
                        entryPathElem = entryPath.split(os.sep)
                        pathToFilename = os.path.join(entryPathElem[2],
                                                      entryPathElem[3])
                        ret = True
                        break

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
                                 processes, basedir, notes=None):
        """ Create calibration session for this session in the database 

            @precondition self._cmd_proto = os.path.join(basedir, "cmd.proto")
            @precondition self._calibratorDB point to a valid ModelRunnerDB

            @param user String representing the user associated with the session
            @param project String representing the project associated with the session
            @param iterations Integer representing the number of times each worldfile is to be 
                                  run in the calibration session
            @param processes Integer representing the number of simultaneous jobs (runs) to run 
                                   at any given time in the calibration session
            @param basedir String representing the basedir wherein session files and 
                                   directories are stored
            @param notes String representing notes associated with the session

            @return model_runner_db.ModelSession object 
            representing the session that was created in the database.
            
            @raise IOError is reading cmd.proto fails
        """
        # Read cmd.proto
        cmd_proto = self._readCmdProto()

        # Create the CalibrationSession
        sessionID = self._calibratorDB.insertSession(user, project, notes,
                                                      iterations, processes,
                                                      basedir, cmd_proto)
        assert sessionID != None
        
        # Could eliminate DB round trip if we just pack our own object,
        #  but this tests that insertSession succeeded
        session = self._calibratorDB.getSession(sessionID)

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
                            each worldfile to be calibrated. Flow table file name must be 
                            of the form $(WORLDFILE_NAME)_flow_table.dat
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
                          help="[OPTIONAL] set path for LSF simulator.  when supplied, jobs will be submitted to the simulator, not via actual LSF commands.  Must be the absolute path (e.g. /Users/joeuser/cluster_calibrator/lsf-sim)")

        parser.add_option("-q", "--queue", action="store",
                          type="string", dest="lsf_queue",
                          help="[OPTIONAL] set queue name to pass to LSF job submission command.  UNC's KillDevil supports the following for general usage: day, debug, hour, week (default), bigmem.  If queue option is not supplied the 'day' queue will be used.")

        parser.add_option("--parallel_mode", action="store", 
                          type="string", dest="parallel_mode",
                          help="[OPTIONAL] set method to use for running jobs in parallel, one of: lsf [default], process")

        parser.add_option("--polling_delay", action="store",
                          type="int", dest="polling_delay",
                          help="[ADVANCED; OPTIONAL] set multiplier for how long to wait in between successive pollings of job status.  Default polling delay is 60 seconds, thus a multiplier of 5 will result in a delay of 5 minutes instead of 1 minute.")

        parser.add_option("--use_horizontal_m_and_K_for_vertical", action="store_true",
                          dest="use_horizontal_m_and_K_for_vertical",
                          help="[ADVANCED; OPTIONAL] use The same m and K parameters for horizontal (i.e. -s) as well as vertical (i.e. -sv ) directions.  Defaults to false.")
        
        parser.add_option("--bsub_exclusive_mode", action="store_true",
                          dest="bsub_exclusive_mode",
                          help="[ADVANCED; OPTIONAL] run bsub with arguments \"-n 1 -R 'span[hosts=1]' -x\" to ensure jobs only run exclusively (i.e. the only job on a node). This can be useful for models that use a lot of memory.")

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

        # Create/verify directory structure in session directory
        try:
            self.createVerifyDirectoryStructure(options.basedir)

            if options.create:
                # --create option was specified, just exit
                print "Calibration session directory structure created in %s" % options.basedir
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
            options.parallel_mode = "lsf"
        elif not options.parallel_mode in PARALLEL_MODES:
            parser.error("""Please specify a valid parallel mode.  See PARALLEL_MODES in %prog""")

        assert( ("lsf" == options.parallel_mode) or ("process" == options.parallel_mode))

        if not options.lsf_queue:
            options.lsf_queue = "day"
        elif not options.lsf_queue in LSF_QUEUES:
            parser.error("""Please specify a valid queue.  See LSF_QUEUES in %prog""")
        
        if not options.polling_delay:
            options.polling_delay = 1
            
        if not options.use_horizontal_m_and_K_for_vertical:
            options.use_horizontal_m_and_K_for_vertical = 0;

        if not options.bsub_exclusive_mode:
            options.bsub_exclusive_mode = 0;

        self.logger.critical("parallel mode: %s" % options.parallel_mode)
        self.logger.debug("basedir: %s" % options.basedir)
        self.logger.debug("user: %s" % options.user)
        self.logger.debug("project: %s" % options.project)
        if None != options.notes:
            self.logger.debug("notes: %s" % options.notes)
        self.logger.debug("iterations: %d" % options.iterations)
        self.logger.debug("jobs: %d" % options.processes)

        # Check for simulator_path, setup job commands accordingly
        if options.simulator_path:
            run_cmd = os.path.join(options.simulator_path, "bsub.py")
            run_status_cmd = os.path.join(options.simulator_path,
                                          "bjobs.py")
        else:
            if options.bsub_exclusive_mode:
                run_cmd = """bsub -n 1,1 -R "span[hosts=1]" -x"""
            else:
                run_cmd = "bsub"
            
            run_status_cmd = "bjobs"

        # Set number of consumer threads
        if "lsf" == options.parallel_mode:
            # LSF will run our jobs us, so there is only one comsumer
            num_consumers = 1
        elif "process" == options.parallel_mode:
            # We will run our jobs, so we need options.process consumer threads
            num_consumers = options.processes
        consumers = []

        # Main events take place herein ...
        try:
            # Make sure we have everything we need to run calibrations
            
            # Get list of worldfiles
            worldfiles = self.getWorldfiles(options.basedir)
            if len(worldfiles) < 1:
                raise Exception("No worldfiles found")
            self.logger.debug("worldfiles: %s" % worldfiles)
            
            # Get tecfile name
            (res, tecfilePath) = self.getTecfilePath(options.basedir)
            if not res:
                raise Exception("No tecfile found")
            
            # Get RHESSys executable path
            (rhessysExecFound, rhessysExec, rhessysExecPath) = \
                self.getRHESSysExecPath(options.basedir)
            if not rhessysExecFound:
                raise Exception("RHESSys executable not found")

            # Read cmd.proto
            cmd_proto = self._readCmdProtoFromFile(options.basedir)
            if None == cmd_proto:
                raise Exception("cmd.proto file not found")
            elif '' == cmd_proto:
                raise Exception("cmd.proto is an empty file")

            # Parse calibrations parameters out of cmd.proto
            paramsProto = self.parseCmdProtoForParams(cmd_proto, options.use_horizontal_m_and_K_for_vertical)

            # Pre-process cmd.proto to add rhessys exec and tecfile path
            cmd_proto_pre = self.preProcessCmdProto(cmd_proto,
                                                    rhessysExecPath,
                                                    tecfilePath)

            # Check for explicit routing in cmd_proto
            explicitRouting = False
            if string.count(cmd_proto, " -r $flowtable ") > 0:
                explicitRouting = True

            # If we're running with explicit routing,
            #   Ensure a flow table exists foreach worldfile
            if explicitRouting:
                (flowTablesOK, flowtablePath, worldfilesWithoutFlowTables) = \
                  self.verifyFlowTables(options.basedir, worldfiles.keys())
                if not flowTablesOK:
                    for worldfile in worldfilesWithoutFlowTables:
                        self.logger.debug("Worldfile without flow table: %s" %
                                          worldfile)
                    raise Exception("Flowtable not found for each worldfile.  Set debug level to DEBUG or higher to see a list of worldfile without flow tables")
            

            self.logger.debug("DB path: %s" % 
                              RHESSysCalibrator.getDBPath(options.basedir))

            self._calibratorDB = \
                ModelRunnerDB(RHESSysCalibrator.getDBPath(
                    options.basedir))

            # Create session
            self.session = self.createCalibrationSession(options.user, 
                                                         options.project,
                                                         options.iterations,
                                                         options.processes,
                                                         options.basedir,
                                                         options.notes)

            # Allocate dispatch queue/pipe with of size options.processes
#            if "queue" == options.ipc:
            runQueue = multiprocessing.JoinableQueue(options.processes)
            
            for i in range(1, num_consumers + 1):
                # Create CalibrationRunner object (consumer)
                if "lsf" == options.parallel_mode:
                    consumer = CalibrationRunnerQueueLSF(options.basedir,
                                                         self.session.id,
                                                         runQueue,
                                                         options.processes,
                                                         RHESSysCalibrator.getDBPath(options.basedir),
                                                         options.lsf_queue,
                                                         options.polling_delay,
                                                         run_cmd,
                                                         run_status_cmd,
                                                         self.logger)
                elif "process" == options.parallel_mode:
                    consumer = CalibrationRunnerSubprocess(options.basedir,
                                                           self.session.id,
                                                           runQueue,
                                                           RHESSysCalibrator.getDBPath(options.basedir),
                                                           self.logger)
                # Create process for consumer
                proc = multiprocessing.Process(target=consumer.run,
                                               args=())
                consumers.append(proc)
                # Start the consumer
                proc.start()

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
                for worldfile in worldfiles.keys():
                    self.logger.critical("Iteration %d, worldfile: %s" %
                                         (itr, worldfile))
                    # Create new ModelRun object for this run
                    run = ModelRun()
                    run.session_id = self.session.id
                    run.worldfile = worldfile
                    run.setCalibrationParameters(parameterValues)
                    
                    # Add worldfile and flowtable paths to command
                    if explicitRouting:
                        cmd_raw_proto = self.addWorldfileAndFlowtableToCmdProto(\
                            itr_cmd_proto, worldfiles[worldfile], 
                            flowtablePath[worldfile])
                    else:
                        cmd_raw_proto = self.addWorldfileToCmdProto(\
                            itr_cmd_proto, worldfiles[worldfile])

                    # Finally, create output_path and generate cmd_raw
                    run.output_path = self.createOutputPath(options.basedir,
                                                            self.session.id,
                                                            worldfile,
                                                            itr)
                    run.cmd_raw = self.getCmdRawForRun(cmd_raw_proto,
                                                       run.output_path)
        
                    if "process" == options.parallel_mode:
                        # Set job ID if we are in process parallel mode
                        #   (in lsf mode, we will use the LSF job number instead of itr)
                        run.job_id = itr
        
                    # Dispatch to consumer
                    runQueue.put(run)


            time.sleep(5)

            # Wait for all jobs to finish
            self.logger.critical("calling runQueue.join() ...")
            runQueue.join()
            for consumerProcess in consumers:
                consumerProcess.join()

            # Update session endtime and status
            self._calibratorDB.updateSessionEndtime(self.session.id,
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
            self._calibratorDB = None