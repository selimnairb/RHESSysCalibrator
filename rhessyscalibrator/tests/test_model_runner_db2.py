#!/usr/bin/env python
"""@package rhessyscalibrator.tests.test_model_runner_db2

@brief Unit tests for rhessyscalibrator.model_runner_db2

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
import tempfile
import shutil
from datetime import datetime

from rhessyscalibrator.model_runner_db2 import *


class TestClusterCalibrator(unittest.TestCase):

    def setUp(self):
        # Copy test DB to temp directory
        self.dbDir = tempfile.mkdtemp()
        shutil.copy("./rhessyscalibrator/tests/data/test.db", self.dbDir)
        self.dbOrigPath = os.path.join(self.dbDir, 'test.db')
        # Upgrade from 1.0 to 2.0
        db = ModelRunnerDB2(self.dbOrigPath)
        self.dbPath = os.path.join(self.dbDir, 'test.sqlite')
        shutil.copyfile(self.dbOrigPath, self.dbPath)
        
        # Keep original 1.0 database separate so that we can test upgrading within a test case
        shutil.copyfile("./rhessyscalibrator/tests/data/test.db", self.dbOrigPath)
        
    def testVersion1ToVersion2Migration(self):
        version = ModelRunnerDB2.getDbVersion(self.dbOrigPath)
        self.assertEqual(version, 1.0)
        
        db = ModelRunnerDB2(self.dbOrigPath)
        
        version = ModelRunnerDB2.getDbVersion(self.dbOrigPath)
        self.assertEqual(version, 2.0)
    
    def testSessionAndRunCreationAndUpdate(self):
         
        db = ModelRunnerDB2(self.dbPath)
 
        # Test session creation
        insertedSessionID = db.insertSession('user2','proj1','notes1',5000,32,'./rhessyscalibrator/tests/data','touch')
         
        mostRecentSessionID = db.getMostRecentSessionID()
 
        self.assertEqual(mostRecentSessionID, insertedSessionID)
 
        # Test updating the end time
        db.updateSessionEndtime(insertedSessionID, 
                                datetime.utcnow(),
                                "complete")
 
        # Test run creation
        insertedRunID = db.insertRun(insertedSessionID,
                                     "worldfile1",
                                     0.11, 0.12, 0.13,
                                     0.21, 0.22,
                                     0.31, 0.32,
                                     0.41, 0.42, 0.43,
                                     0.51, 0.52,
                                     "rhessys -w worldfile1 ..",
                                     "run_12345_worlfile1",
                                     12345)
         
        mostRecentRunID = db.getMostRecentRunID(insertedSessionID)
 
        self.assertEqual(mostRecentRunID, insertedRunID)
 
        runStatus_pre = db.getRunStatus(insertedRunID)
 
        db.updateRunStatus(insertedRunID, "RUN")
 
        runStatus_post = db.getRunStatus(insertedRunID)
 
        self.assertNotEqual(runStatus_pre, runStatus_post)
 
        db.updateRunEndtime(insertedRunID,
                            datetime.utcnow(),
                            "DONE")
 
        runStatus_final = db.getRunStatus(insertedRunID)
        self.assertNotEqual(runStatus_post, runStatus_final)
 
        retPostProcID = db.insertPostProcess(insertedSessionID, "obsfile.csv", 
                                          "daily", None, 0.25, {'foo': 'bar'})
        insertedPostproc = db.getPostProcessForSession(insertedSessionID)
        postproc = insertedPostproc[0]
        postProcID = postproc.id
        self.assertEqual(retPostProcID, postProcID)
        
        self.assertEqual(postproc.session_id, insertedSessionID)
        self.assertEqual(postproc.obs_filename, "obsfile.csv")
        self.assertEqual(postproc.fitness_period, "daily")
        #self.assertEqual(postproc.exclude_date_ranges, ...)
        self.assertEqual(postproc.obs_runoff_ratio, 0.25)
        self.assertEqual(postproc.options['foo'], 'bar')
        
        postproc2 = db.getPostProcess(postProcID)
        self.assertEqual(postproc.session_id, postproc2.session_id)
        self.assertEqual(postproc.obs_filename, postproc2.obs_filename)
        self.assertEqual(postproc.fitness_period, postproc2.fitness_period)
        #self.assertEqual(postproc.exclude_date_ranges, ...)
        self.assertEqual(postproc.obs_runoff_ratio, postproc2.obs_runoff_ratio)
        self.assertEqual(postproc.options['foo'], postproc2.options['foo'])
 
        retRunfitID = db.insertRunFitnessResults(postProcID, insertedRunID, 1.0, 0.9)
 
        # Test fetching a stored run
        fetchedRun = db.getRun(insertedRunID)
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
        
        runfit = db.getRunFitnessForPostProcess(postProcID)
        self.assertEqual(len(runfit), 1)
        runfit = runfit[0]
        self.assertEqual(runfit.id, retRunfitID)
        self.assertEqual(runfit.nse, 1.0)
        self.assertEqual(runfit.nse_log, 0.9)
        self.assertEqual(runfit.rsr, None)
 
        # Test getRunsInSession
        # First, add another run (so there will be more than one)
        newRunID = db.insertRun(insertedSessionID,
                                               "worldfile2",
                                               0.11, 0.12, 0.13,
                                               0.21, 0.22,
                                               0.31, 0.32,
                                               0.41, 0.42, 0.43,
                                               0.51, 0.52,
                                               "rhessys -w worldfile2 ..",
                                               "run_12346_worlfile2",
                                               12346)
 
        runs = db.getRunsInSession(insertedSessionID)
        self.assertTrue(runs[0].id < runs[1].id)
                          
        # Test getSession
        fetchedSession = db.getSession(insertedSessionID)
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
 
        # Test getSessions
        # First, add another session (so there will be more than one)
        newSessionID = db.insertSession('user2','proj1','notes2',5000,32,'./test2','touch2')
 
        sessions = db.getSessions()
        self.assertTrue(sessions[0].id < sessions[1].id)
 
        # Test getRunInSession
        run = db.getRunInSession(insertedSessionID, 12346)
        
        # Test getRunsInPostProcess
        runs = db.getRunsInPostProcess(postProcID)
        self.assertTrue(len(runs) == 1)
        fetchedRun = runs[0]
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
        self.assertEqual(fetchedRun.run_fitness.nse, 1.0)
        self.assertEqual(fetchedRun.run_fitness.nse_log, 0.9)
        
        # Test getRunInSession with where clause
        runs = db.getRunsInPostProcess(postProcID, where_clause='nse > 0.5 AND nse_log > 0.5')
        self.assertTrue(len(runs) == 1)
        fetchedRun = runs[0]
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
        self.assertEqual(fetchedRun.run_fitness.nse, 1.0)
        self.assertEqual(fetchedRun.run_fitness.nse_log, 0.9)
 
        self.assertFalse(run == None)

    def tearDown(self):
        shutil.rmtree(self.dbDir)
        unittest.TestCase.tearDown(self)

if __name__ == "__main__":
    unittest.main()
