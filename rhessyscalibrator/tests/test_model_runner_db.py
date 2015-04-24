#!/usr/bin/env python
"""@package rhessyscalibrator.tests.test_model_runner_db

@brief Unit tests for rhessyscalibrator.model_runner_db

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

@note To run: PYTHONPATH=../EcohydroLib:../RHESSysWorkflows python -m unittest discover
@note Assumes EcohydroLib and RHESSysWorkflows source are in ".."

@author Brian Miles <brian_miles@unc.edu>
"""
import unittest
from datetime import datetime

from rhessyscalibrator.model_runner_db import *


class TestClusterCalibrator(unittest.TestCase):

    def setUp(self):
        self.calibratorDB = ModelRunnerDB("./rhessyscalibrator/tests/data/test.db")

    def testSessionAndRunCreationAndUpdate(self):

        # Test session creation
        insertedSessionID = self.calibratorDB.insertSession('user2','proj1','notes1',5000,32,'./rhessyscalibrator/tests/data','touch')
        
        mostRecentSessionID = self.calibratorDB.getMostRecentSessionID()

        self.assertEqual(mostRecentSessionID, insertedSessionID)

        # Test updating the end time
        self.calibratorDB.updateSessionEndtime(insertedSessionID, 
                                               datetime.utcnow(),
                                               "complete")

        # Test run creation
        insertedRunID = self.calibratorDB.insertRun(insertedSessionID,
                                                    "worldfile1",
                                                    0.11, 0.12, 0.13,
                                                    0.21, 0.22,
                                                    0.31, 0.32,
                                                    0.41, 0.42, 0.43,
                                                    0.51, 0.52,
                                                    "rhessys -w worldfile1 ..",
                                                    "run_12345_worlfile1",
                                                    12345)
        
        mostRecentRunID = self.calibratorDB.getMostRecentRunID(insertedSessionID)

        self.assertEqual(mostRecentRunID, insertedRunID)

        runStatus_pre = self.calibratorDB.getRunStatus(insertedRunID)

        self.calibratorDB.updateRunStatus(insertedRunID, "RUN")

        runStatus_post = self.calibratorDB.getRunStatus(insertedRunID)

        self.assertNotEqual(runStatus_pre, runStatus_post)

        self.calibratorDB.updateRunEndtime(insertedRunID,
                                           datetime.utcnow(),
                                           "DONE")

        runStatus_final = self.calibratorDB.getRunStatus(insertedRunID)
        self.assertNotEqual(runStatus_post, runStatus_final)

        self.calibratorDB.updateRunFitnessResults(insertedRunID, 'daily', 1.0, 0.9)

        self.calibratorDB.updateSessionObservationFilename(insertedSessionID,
                                                           "obsfile.csv")

        # Test fetching a stored run
        fetchedRun = self.calibratorDB.getRun(insertedRunID)
        self.assertFalse(fetchedRun == None)
        self.assertEqual(fetchedRun.id, insertedRunID)
        self.assertEqual(fetchedRun.session_id, insertedSessionID)
        # Just try to parse to make sure the date returned is valid
        fetchedRun.starttime.strftime("%Y-%m-%d %H:%M:%S")
        if fetchedRun.endtime != None:
            fetchedRun.endtime.strftime("%Y-%m-%d %H:%M:%S")
        self.assertEqual(fetchedRun.worldfile, "worldfile1")
        self.assertEqual(fetchedRun.param_s1, 0.11)
        self.assertEqual(fetchedRun.param_s2, 0.12)
        self.assertEqual(fetchedRun.param_s3, 0.13)
        self.assertEqual(fetchedRun.param_sv1, 0.21)
        self.assertEqual(fetchedRun.param_sv2, 0.22)
        self.assertEqual(fetchedRun.param_gw1, 0.31)
        self.assertEqual(fetchedRun.param_gw2, 0.32)
        self.assertEqual(fetchedRun.param_vgsen1, 0.41)
        self.assertEqual(fetchedRun.param_vgsen2, 0.42)
        self.assertEqual(fetchedRun.param_vgsen3, 0.43)
        self.assertEqual(fetchedRun.cmd_raw, "rhessys -w worldfile1 ..")
        self.assertEqual(fetchedRun.output_path, "run_12345_worlfile1")
        self.assertEqual(fetchedRun.job_id, '12345')
        self.assertEqual(fetchedRun.status, "DONE")
        self.assertEqual(fetchedRun.nse, 1.0)
        self.assertEqual(fetchedRun.nse_log, 0.9)
        self.assertEqual(fetchedRun.rsr, None)

        # Test getRunsInSession
        # First, add another run (so there will be more than one)
        newRunID = self.calibratorDB.insertRun(insertedSessionID,
                                               "worldfile2",
                                               0.11, 0.12, 0.13,
                                               0.21, 0.22,
                                               0.31, 0.32,
                                               0.41, 0.42, 0.43,
                                               0.51, 0.52,
                                               "rhessys -w worldfile2 ..",
                                               "run_12346_worlfile2",
                                               12346)

        runs = self.calibratorDB.getRunsInSession(insertedSessionID)
        self.assertTrue(runs[0].id < runs[1].id)
                         
        # Test getSession
        fetchedSession = self.calibratorDB.getSession(insertedSessionID)
        self.assertFalse(fetchedSession == None)
        self.assertEqual(fetchedSession.id, insertedSessionID)
        # Just try to parse to make sure the date returned is valid
        fetchedSession.starttime.strftime("%Y-%m-%d %H:%M:%S")
        if fetchedSession.endtime != None:
            fetchedSession.endtime.strftime("%Y-%m-%d %H:%M:%S")
        self.assertEqual(fetchedSession.user, "user2")
        self.assertEqual(fetchedSession.project, "proj1")
        self.assertEqual(fetchedSession.notes, "notes1")
        self.assertEqual(fetchedSession.iterations, 5000)
        self.assertEqual(fetchedSession.processes, 32)
        self.assertEqual(fetchedSession.basedir, "./rhessyscalibrator/tests/data")
        self.assertEqual(fetchedSession.cmd_proto, "touch")
        self.assertEqual(fetchedSession.status, "complete")
        self.assertEqual(fetchedSession.obs_filename, "obsfile.csv")

        # Test getSessions
        # First, add another session (so there will be more than one)
        newSessionID = self.calibratorDB.insertSession('user2','proj1','notes2',5000,32,'./test2','touch2')

        sessions = self.calibratorDB.getSessions()
        self.assertTrue(sessions[0].id < sessions[1].id)

        # Test getRunInSession
        run = self.calibratorDB.getRunInSession(insertedSessionID, 12346)

        self.assertFalse(run == None)

if __name__ == "__main__":
    unittest.main()
