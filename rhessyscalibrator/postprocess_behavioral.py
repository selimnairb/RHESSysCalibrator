"""@package rhessyscalibrator.postprocess_behavioral

@brief Tool for analyzing model run results generated by rhessys_calibrator_behavioral.py and 
stored in a database format managed by rhessyscalibrator.model_runner_db.py.  Currently
plots 95% uncertainty bounds around observed streamflow using NSE as a likelihood function.

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
import os, sys, errno
import argparse
import logging

import math
import numpy
import matplotlib.pyplot as plt

from rhessyscalibrator.postprocess import RHESSysCalibratorPostprocess
from rhessyscalibrator.calibrator import RHESSysCalibrator
from rhessyscalibrator.model_runner_db import *

class RHESSysCalibratorPostprocessBehavioral(object):
    """ Main driver class for rhessys_calibrator_postprocess_behavioral tool
    """
    
    def saveUncertaintyBoundsPlot(self, outDir, filename, lowerBound, upperBound,
                                  format='PDF', log=False, xlabel=None, ylabel=None,
                                  title=None):
        """ Save uncertainty bounds plot to outDir
        
            @param lowerBound Float <100.0, >0.0, <upperBound
            @param upperBound Float <100.0, >0.0, >lowerBound
        """
        assert( format in ['PDF', 'PNG'] )
        if format == 'PDF':
            plotFilename = "%s.pdf" % (filename,)
        elif format == 'PNG':
            plotFilename = "%s.png" % (filename,)
        plotFilepath = os.path.join(outDir, plotFilename)
        
        assert( lowerBound < 100.0 and lowerBound > 0.0)
        assert( upperBound < 100.0 and upperBound > 0.0)
        assert( lowerBound < upperBound )
        assert( self.x is not None )
        assert( self.ysim is not None )
        
        # Line up start of observed and modeled timeseries
        # self.obs_datetime, self.obs_data
        
        # Normalize likelihood to have values from 0 to 1
        normLH = self.likelihood / numpy.sum(self.likelihood)
        
        lower = lowerBound / 100.0
        upper = upperBound / 100.0
        
        # Get the uncertainty boundary
        nIters = numpy.shape(self.ysim)[1]
        minYsim = numpy.zeros(nIters)
        maxYsim = numpy.zeros(nIters)

        # Generate uncertainty interval bounded by lower bound and upper bound
        for i in xrange(0, nIters):
            ys = self.ysim[:,i]
            # Use CDF of likelihood values as basis for interval
            sortedIdx = numpy.argsort(ys)
            sortYsim = ys[sortedIdx]
            sortLH = normLH[sortedIdx]
            cumLH = numpy.cumsum(sortLH)
            cond = (cumLH > lower) & (cumLH < upper)
            
            validYsim = sortYsim[cond]
            minYsim[i] = validYsim[0]
            maxYsim[i] = validYsim[-1]
            
        # Plot it up
        fig = plt.figure()
        plt.xticks(rotation=45)
        # Draw lower and upper uncertainty lines
        if log:
            Y1 = numpy.log10(minYsim)
            Y2 = numpy.log10(maxYsim)
            Y3 = numpy.log10(self.obs_data)
        else:
            Y1 = minYsim
            Y2 = maxYsim
            Y3 = self.obs_data
        plt.plot(self.x, Y1, color='0.5', linestyle='solid')
        plt.plot(self.x, Y2, color='0.5', linestyle='solid')
        # Draw observed line
        plt.plot(self.obs_datetime, Y3, color='black', linestyle='solid')
        # Draw shaded uncertainty envelope
        plt.fill_between(self.x, Y1, Y2, color='0.8')
        # Set labels and title
        if xlabel:
            plt.xlabel(xlabel)
        if ylabel:
            plt.ylabel(ylabel)
        if title:
            plt.suptitle(title)
        # Save the figure
        fig.savefig(plotFilepath, format=format, bbox_inches='tight', pad_inches=0.25)
        
    
    def _initLogger(self, level):
        """ Setup logger.  Log to the console for now """
        self.logger = logging.getLogger("cluster_calibrator")
        self.logger.setLevel(level)
        # console handler and set it to level
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(level)
        # create formatter
        formatter = \
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to console handler
        consoleHandler.setFormatter(formatter)
        # add consoleHandler to logger
        self.logger.addHandler(consoleHandler)
        
    
    def main(self, args):
        # Set up command line options
        parser = argparse.ArgumentParser(description="Tool for visualizing output from behavioral model runs for RHESSys")
        parser.add_argument("-b", "--basedir", action="store", 
                            dest="basedir", required=True,
                            help="Base directory for the calibration session")
        
        parser.add_argument("-s", "--behavioral_session", action="store", type=int,
                            dest="session_id", required=True,
                            help="Session to use for behavioral runs.")
        
        parser.add_argument("-t", "--title", action="store",
                            dest="title",
                            help="Title to add to output figures")
        
        parser.add_argument("-o", "--outdir", action="store",
                            dest="outdir",
                            help="Output directory in which to place dotty plot figures.  If not specified, basedir will be used.")

        parser.add_argument("-f", "--file", action="store",
                            dest="observed_file",
                            help="The name of the observed file to use for calculating model fitness statistics.  Filename will be interpreted as being relative to $BASEDIR/obs. If not supplied observation file from calibration model run will be used.")

        parser.add_argument("-of", "--outputFormat", action="store",
                            dest="outputFormat", default="PDF", choices=["PDF", "PNG"],
                            help="Output format to save figures in.")

        parser.add_argument("-l", "--loglevel", action="store",
                            dest="loglevel", default="OFF",
                            help="Set logging level, one of: OFF [default], DEBUG, CRITICAL (case sensitive)")
        
        options = parser.parse_args()
        
        # Enforce initial command line options rules
        if "DEBUG" == options.loglevel:
            self._initLogger(logging.DEBUG)
        elif "CRITICAL" == options.loglevel:
            self._initLogger(logging.CRITICAL)
        else:
            self._initLogger(logging.NOTSET)
        
        if not os.path.isdir(options.basedir) or not os.access(options.basedir, os.W_OK):
            sys.exit("Unable to write to basedir %s" % (options.basedir,) )
        basedir = os.path.abspath(options.basedir)
        
        if not options.outdir:
            options.outdir = basedir
            
        if not os.path.isdir(options.outdir) and os.access(options.outdir, os.W_OK):
            parser.error("Figure output directory %s must be a writable directory" % (options.outdir,) )
        outdirPath = os.path.abspath(options.outdir)

        dbPath = RHESSysCalibrator.getDBPath(basedir)
        if not os.access(dbPath, os.R_OK):
            raise IOError(errno.EACCES, "The database at %s is not readable" %
                          dbPath)
        self.logger.debug("DB path: %s" % dbPath)
        
        outputPath = RHESSysCalibrator.getOutputPath(basedir)
        if not os.access(outputPath, os.R_OK):
            raise IOError(errno.EACCES, "The output directory %s is  not readable" % outputPath)
        self.logger.debug("Output path: %s" % outputPath)

        rhessysPath = RHESSysCalibrator.getRhessysPath(basedir)
        
        try:
            calibratorDB = \
                ModelRunnerDB(RHESSysCalibrator.getDBPath(
                    basedir))
        
            # Make sure the session exists
            session = calibratorDB.getSession(options.session_id)
            if None == session:
                raise Exception("Session %d was not found in the calibration database %s" % (options.session_id, dbPath))
            if session.status != "complete":
                print "WARNING: session status is: %s.  Some model runs may not have completed." % (session.status,)
            else:
                self.logger.debug("Session status is: %s" % (session.status,))
        
            # Deterine observation file path
            if options.observed_file:
                obs_file = options.observed_file
            else:
                # Get observered file from session
                assert( session.obs_filename != None )
                obs_file = session.obs_filename
            obsPath = RHESSysCalibrator.getObsPath(basedir)
            obsFilePath = os.path.join(obsPath, obs_file)
            if not os.access(obsFilePath, os.R_OK):
                raise IOError(errno.EACCES, "The observed data file %s is  not readable" % obsFilePath)
            self.logger.debug("Obs path: %s" % obsFilePath)
            
            # Get runs in session
            runs = calibratorDB.getRunsInSession(session.id)
            numRuns = len(runs) 
            if numRuns == 0:
                raise Exception("No runs found for session %d" 
                                % (session.id,))
            self.logger.debug("%d behavioral runs" % (numRuns,) )
            
            # Read observed data from file
            obsFile = open(obsFilePath, 'r')
            (self.obs_datetime, self.obs_data) = \
                RHESSysCalibratorPostprocess.readObservedDataFromFile(obsFile)
            obsFile.close()

            self.logger.debug("Observed data: %s" % self.obs_data)
            
            self.likelihood = numpy.empty(numRuns)
            self.ysim = None
            self.x = None
            
            runsProcessed = False
            for (i, run) in enumerate(runs):
                if "DONE" == run.status:
                    runOutput = os.path.join(rhessysPath, run.output_path)
                    self.logger.debug(">>>\nOutput dir of run %d is %s" %
                                         (run.id, runOutput))
                    tmpOutfile = \
                        RHESSysCalibrator.getRunOutputFilePath(runOutput)
                    if not os.access(tmpOutfile, os.R_OK):
                        print "Output file %s for run %d not found or not readable, unable to calculate fitness statistics for this run" % (tmpOutfile, run.id)
                        continue
                    
                    tmpFile = open(tmpOutfile, 'r')
                    
                    (model_datetime, model_data) = \
                            RHESSysCalibratorPostprocess.readColumnFromFile(tmpFile,
                                                                            "streamflow")
                    # Stash date for X values (assume they are the same for all runs
                    if self.x == None:
                        self.x = numpy.array(model_datetime)
                    # Put data in matrix
                    dataLen = len(model_data)
                    if self.ysim == None:
                        # Allocate matrix for results
                        self.ysim = numpy.empty( (numRuns, dataLen) )
                    assert( numpy.shape(self.ysim)[1] == dataLen )
                    self.ysim[i,] = model_data
                    
                    # Store fitness parameter
                    self.likelihood[i] = run.nse
                            
                    tmpFile.close()                
                    runsProcessed = True
                    
            if runsProcessed:
                # Generate visualization
                behavioralFilename = "behavioral_SESSION_%s" % ( options.session_id, ) 
                self.saveUncertaintyBoundsPlot(outdirPath, behavioralFilename, 
                                               2.5, 97.5, format=options.outputFormat, log=False,
                                               ylabel=r'Streamflow (mm)',
                                               title=options.title)
                
                behavioralFilename = "behavioral-log_SESSION_%s" % ( options.session_id, ) 
                self.saveUncertaintyBoundsPlot(outdirPath, behavioralFilename, 
                                               2.5, 97.5, format=options.outputFormat, log=True,
                                               ylabel=r'$Log_{10}$(Streamflow) (mm)',
                                               title=options.title)
        except:
            raise
        else:
            self.logger.debug("exiting normally")
            return 0
        finally:
            # Decrement reference count, this will (hopefully) allow __del__
            #  to be called on the once referenced object
            calibratorDB = None