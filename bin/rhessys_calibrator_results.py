#!/usr/bin/env python
"""@package rhessys_calibrator_results

@brief Tool for analyzing model run results generated
by rhessys_calibrator.py and stored in a database format managed by 
rhessyscalibrator.model_runner_db.py

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
import string

import time
from datetime import datetime

import wx
import wx.aui
import wx.grid

from rhessys_calibrator import RHESSysCalibrator
from rhessyscalibrator.model_runner_db import *


class ClusterCalibratorResults(wx.Frame):
    """ Main driver class for cluster_calibrator_results tool
    """
    def __init__(self, args):
        wx.Frame.__init__(self, None, -1, "Calibration Results", size=(900,700))

        # Set up command line options
        parser = OptionParser(usage="""%prog --basedir=BASEDIR --file=OBS_FILE --session=SESSION_ID [optional arguments ...]

Run "%prog --help" for detailed description of all options
""")
        parser.add_option("-b", "--basedir", action="store", type="string", 
                          dest="basedir",
                          help="[REQUIRED] base directory for the calibration session")
        
        (options, args) = parser.parse_args()

        if not options.basedir:
            parser.error("Please specify the basedir for the calibration session")

        dbPath = RHESSysCalibrator.getDBPath(options.basedir)
        if not os.access(dbPath, os.R_OK):
            raise IOError(errno.EACCES, "The database at %s is not readable" %
                          dbPath)

        self.calibratorDB = \
            ModelRunnerDB(dbPath)

        self.sessions = None
        self.runs = dict()
        self.selectedSession = None

        # Initialize GUI components
        self.mgr = wx.aui.AuiManager(self)

        sessionPanel = wx.Panel(self, size=(200,320))
        sessionSizer = wx.BoxSizer(wx.VERTICAL)

        # Place to display session IDs
        self.sessionGrid = wx.grid.Grid(sessionPanel, style=wx.VSCROLL)
        self.sessionGrid.CreateGrid(1, 1)
        self.sessionGrid.SetRowLabelSize(0)
        self.sessionGrid.SetColLabelValue(0, "Session ID")
        self.sessionGrid.EnableEditing(False)
        self.sessionGrid.SetDefaultRenderer(wx.grid.GridCellAutoWrapStringRenderer())
        self.addSessionsToGrid(self.sessionGrid, 0, 0)
        self.sessionGrid.ClearSelection()
        self.sessionGrid.Bind(wx.grid.EVT_GRID_CELL_LEFT_CLICK, 
                              self.OnSessionGridCellClick)

        # Place to display session details
        #self.sessionDetail = wx.TextCtrl(sessionPanel, size=wx.Size(160, 120),
        #                                 style=wx.TE_MULTILINE)
        #self.sessionDetail.SetEditable(False)
        self.sessionDetail = wx.grid.Grid(sessionPanel, size=(320, 320))
        self.sessionDetail.CreateGrid(9, 1)
        self.sessionDetail.AutoSizeColumns()
        self.sessionDetail.AutoSizeRows()
        self.sessionDetail.SetColLabelSize(0)
        self.sessionDetail.SetRowLabelValue(0, "Start time")
        self.sessionDetail.SetRowLabelValue(1, "End time")
        self.sessionDetail.SetRowLabelValue(2, "User")
        self.sessionDetail.SetRowLabelValue(3, "Project")
        self.sessionDetail.SetRowLabelValue(4, "Iterations")
        self.sessionDetail.SetRowLabelValue(5, "Num. CPUs")
        self.sessionDetail.SetRowLabelValue(6, "Status")
        self.sessionDetail.SetRowLabelValue(7, "Obs. file")
        self.sessionDetail.SetRowLabelValue(8, "Notes")
        self.sessionDetail.EnableEditing(False)
        self.sessionDetail.SetDefaultRenderer(wx.grid.GridCellAutoWrapStringRenderer())

        sessionSizer.Add(self.sessionGrid, 0, wx.LEFT, 4)
        sessionSizer.Add(self.sessionDetail, 0, wx.LEFT, 4)
        sessionPanel.SetSizer(sessionSizer)

        # Place to display summary of runs
        runsSummaryPanel = wx.Panel(self)
        runSizer = wx.BoxSizer(wx.VERTICAL)

        self.runGrid = wx.grid.Grid(runsSummaryPanel, 
                                    style=wx.VSCROLL | wx.HSCROLL)
        self.runGrid.CreateGrid(5, 8)
        self.runGrid.SetRowLabelSize(0)
        self.runGrid.SetColLabelValue(0, "Run ID")
        self.runGrid.SetColLabelValue(1, "NSE")
        self.runGrid.SetColLabelValue(2, "NSE-log")
        self.runGrid.SetColLabelValue(3, "PBIAS")
        self.runGrid.SetColLabelValue(4, "RSR")
        self.runGrid.SetColLabelValue(5, "user1")
        self.runGrid.SetColLabelValue(6, "user2")
        self.runGrid.SetColLabelValue(7, "user3")
        self.runGrid.EnableEditing(False)
        self.runGrid.SetDefaultRenderer(wx.grid.GridCellAutoWrapStringRenderer())
        self.runGrid.Bind(wx.grid.EVT_GRID_CELL_LEFT_CLICK,
                          self.OnRunGridCellClick)

        runSizer.Add(self.runGrid, 0, wx.TOP, 4)
        runsSummaryPanel.SetSizer(runSizer)

        # Place to display details of a run
        runDetailPanel = wx.Panel(self, size=(890, 328))
        runDetailSizer = wx.BoxSizer(wx.VERTICAL)
        
        self.runDetail = wx.grid.Grid(runDetailPanel, 
                                      style=wx.VSCROLL | wx.HSCROLL,
                                      size=(882, 320))
        self.runDetail.AutoSize()
        self.runDetail.CreateGrid(13, 1)
        self.runDetail.SetColLabelSize(0)
        self.runDetail.SetRowLabelValue(0, "Worldfile")
        self.runDetail.SetRowLabelValue(1, "Output path")
        self.runDetail.SetRowLabelValue(2, "RHESSys command")
        self.runDetail.SetRowLabelValue(3, "s1")
        self.runDetail.SetRowLabelValue(4, "s2")
        self.runDetail.SetRowLabelValue(5, "s3")
        self.runDetail.SetRowLabelValue(6, "sv1")
        self.runDetail.SetRowLabelValue(7, "sv2")
        self.runDetail.SetRowLabelValue(8, "gw1")
        self.runDetail.SetRowLabelValue(9, "gw2")
        self.runDetail.SetRowLabelValue(10, "vgsen1")
        self.runDetail.SetRowLabelValue(11, "vgsen2")
        self.runDetail.SetRowLabelValue(12, "vgsen3")
        self.runDetail.EnableEditing(False)
        self.runDetail.SetDefaultRenderer(wx.grid.GridCellAutoWrapStringRenderer())

        runDetailSizer.Add(self.runDetail, 0, wx.TOP, 4)
        runDetailPanel.SetSizer(runDetailSizer)

        # Add components to UI manager
        self.mgr.AddPane(sessionPanel, wx.LEFT)
        self.mgr.AddPane(runsSummaryPanel, wx.CENTER)
        self.mgr.AddPane(runDetailPanel, wx.BOTTOM)
        self.mgr.Update()

        self.Bind(wx.EVT_CLOSE, self.OnCloseWindow)

    def OnSessionGridCellClick(self, event):
        """ Display session details and runs for session
            
            Precondition: self.sessions is initialized

            Arguments:
            event -- wx.GridEvent   The event
        """
        assert(self.sessions != None)

        #print "Row %s clicked on" % event.GetRow()
        sessionID = self.sessionGrid.GetCellValue(event.GetRow(),
                                                  event.GetCol())
        #print "\tClicked value: %s" % sessionID
        try:
            # Update session detail
            idx = self.findIndexByID(self.sessions, sessionID)
            session = self.sessions[idx]
            self.sessionGrid.SelectRow(event.GetRow(), False)
            self.setSessionDetail(self.sessionDetail, session, 0, 0)
            self.selectedSession = sessionID

            # Load data for runs in the selected session
            if not sessionID in self.runs.keys():
                self.runs[sessionID] = \
                    self.calibratorDB.getRunsInSession(sessionID)
            self.addRunsToGrid(self.runGrid, self.runs[sessionID], 0, 0)

            # Clear run details
            self.runDetail.ClearGrid()
            
        except ValueError as e:
            print "OnSessionGridCellClick: %s" % e

    def OnRunGridCellClick(self, event):
        """ Display run details for run

            Precondition: self.selectedSession is initialized to an
            integer representing the session currently selected
            
            Precondition: self.runs[self.selectedSession] is a list
            of runs in the selected session

            Assumption: runGrid stores run IDs in column 0

            Arguments:
            event -- wx.GridEvent  The event
        """
        if self.selectedSession:
            runsInSession = self.runs[self.selectedSession]

            runID = self.runGrid.GetCellValue(event.GetRow(),
                                              0)
            try:
                idx = self.findIndexByID(runsInSession, runID)
                run = runsInSession[idx]
                self.runGrid.SelectRow(event.GetRow(), False)
                self.addRunDetailsToGrid(self.runDetail, run, 
                                         0, 0)
            except ValueError as e:
                print "OnRunGridCellClick: %s" % e


    def addSessionsToGrid(self, grid, begin_row, begin_col):
        """ Add session.id to each row of the grid

            Arguments:
            grid -- wx.grid.Grid    The grid to add session IDs to
            begin_row -- int        The row to begin adding values to
            begin_col -- int        The column to begin adding values to
            
        """
        if None == self.sessions:
            self.sessions = self.calibratorDB.getSessions()

        grid.ClearGrid()

        # Add rows to the grid if necessary
        numSessions = len(self.sessions)
        rowsNeeded = numSessions - grid.NumberRows
        if rowsNeeded > 0:
            grid.AppendRows(rowsNeeded)

        myRow = begin_row
        for session in self.sessions:
            grid.SetCellValue(myRow, begin_col, str(session.id))
            myRow += 1
        grid.AutoSizeColumns()
        grid.AutoSizeRows()

    def addRunsToGrid(self, grid, runs, begin_row, begin_col):
        """ Add run summary information for self.runGrid

            Arguments:
            grid -- wx.grid.Grid   Grid to add run information to
            runs -- list<model_runner_db.CalibrationRun>
            begin_row -- int       Row at which to begin writing information
            begin_col -- int       Column at which to begin writing information
        """
        grid.ClearGrid()

        # Add rows to the grid if necessary
        numRuns = len(runs)
        rowsNeeded = numRuns - grid.NumberRows
        if rowsNeeded > 0:
            grid.AppendRows(rowsNeeded)

        myRow = begin_row
        myCol = begin_col
        for run in runs:
            grid.SetCellValue(myRow, myCol, str(run.id))
            if run.nse:
                grid.SetCellValue(myRow, myCol+1, "%.2f" % run.nse)
            if run.nse_log:
                grid.SetCellValue(myRow, myCol+2, "%.2f" % run.nse_log)
            if run.pbias:
                grid.SetCellValue(myRow, myCol+3, "%.2f" % run.pbias)
            if run.rsr:
                grid.SetCellValue(myRow, myCol+4, "%.2f" % run.rsr)
            if run.user1:
                grid.SetCellValue(myRow, myCol+5, "%.2f" % run.user1)
            if run.user2:
                grid.SetCellValue(myRow, myCol+6, "%.2f" % run.user2)
            if run.user3:
                grid.SetCellValue(myRow, myCol+7, "%.2f" % run.user3)
            myRow += 1

    def setSessionDetail(self, grid, session, begin_row, begin_col):
        """ Sets session detail in GUI
        
            Arguments:
            grid -- wx.grid.Grid    The grid to add session details t
            session -- model_runner_db.CalibrationSession
            begin_row -- int        The row to begin adding values to
            begin_col -- int        The column to begin adding values to
        """
        #self.sessionDetail.Clear()
        #self.sessionDetail.WriteText("id: %s\nobs_filename: %s" %
        #                             (session.id, session.obs_filename))

        grid.ClearGrid()
        
        grid.SetCellValue(begin_row, begin_col, str(session.starttime))
        if session.endtime:
            grid.SetCellValue(begin_row+1, begin_col, str(session.endtime))
        grid.SetCellValue(begin_row+2, begin_col, session.user)
        grid.SetCellValue(begin_row+3, begin_col, session.project)
        grid.SetCellValue(begin_row+4, begin_col, str(session.iterations))
        grid.SetCellValue(begin_row+5, begin_col, str(session.processes))
        grid.SetCellValue(begin_row+6, begin_col, session.status)
        if session.obs_filename:
            grid.SetCellValue(begin_row+7, begin_col, session.obs_filename)
        if session.notes:
            grid.SetCellValue(begin_row+8, begin_col, session.notes)
        grid.AutoSizeColumns()
        grid.AutoSizeRows()

    def addRunDetailsToGrid(self, grid, run, begin_row, begin_col):
        """ Sets run detail in GUI

            Arguments:
            grid -- wx.grid.Grid    The grid to add run details to
            run -- model_runner_db.CalibrationRun
            begin_row -- int        The row to begin adding values to
            begin_col -- int        The column to begin adding values to 
        """
        grid.ClearGrid()

        grid.SetCellValue(begin_row, begin_col, run.worldfile)
        grid.SetCellValue(begin_row+1, begin_col, run.output_path)
        grid.SetCellValue(begin_row+2, begin_col, run.cmd_raw)
        if run.param_s1:
            grid.SetCellValue(begin_row+3, begin_col, "%.4f" % run.param_s1)
        if run.param_s2:
            grid.SetCellValue(begin_row+4, begin_col, "%.4f" % run.param_s2)
        if run.param_s3:
            grid.SetCellValue(begin_row+5, begin_col, "%.4f" % run.param_s3)
        if run.param_sv1:
            grid.SetCellValue(begin_row+6, begin_col, "%.4f" % run.param_sv1)
        if run.param_sv2:
            grid.SetCellValue(begin_row+7, begin_col, "%.4f" % run.param_sv2)
        if run.param_gw1:
            grid.SetCellValue(begin_row+8, begin_col, "%.4f" % run.param_gw1)
        if run.param_gw2:
            grid.SetCellValue(begin_row+9, begin_col, "%.4f" % run.param_gw2)
        if run.param_vgsen1:
            grid.SetCellValue(begin_row+10, begin_col, 
                              "%.4f" % run.param_vgsen1)
        if run.param_vgsen2:
            grid.SetCellValue(begin_row+11, begin_col, 
                              "%.4f" % run.param_vgsen2)
        if run.param_vgsen3:
            grid.SetCellValue(begin_row+12, begin_col, 
                              "%.4f" % run.param_vgsen3)
        grid.AutoSizeColumns()
        grid.AutoSizeRows()
        
    @classmethod
    def findIndexByID(cls, collection, id):
        """ Finds the index of the session in sessions with session_id
        
            If more than one item in the collection has the same ID,
            the index of the first item will be returned

            Arguments:
            collection -- A collection of objects that have an "id" attribute
            id -- int  The ID to search for

            Raises ValueError if a session with session_id is not in sessions
        """
        id = int(id)
        idx = 0
        for item in collection:
            if id == item.id:
                return idx
            idx += 1
        raise ValueError("Item with ID %s not found in collection %s" %
                         (id, collection))

    def OnCloseWindow(self, event):
        self.Destroy()

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

    def __del__(self):
        self.calibratorDB = None

if __name__ == "__main__":
    app = wx.App(False)

    calibratorResults = ClusterCalibratorResults(sys.argv)
    calibratorResults.Center()
    calibratorResults.Show(True)

    app.MainLoop()
