"""@package rhessyscalibrator.model_runner_db

@brief Classes that store data about calibration runs to an SQLite3 database.
Version 2 of the database.

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
import sqlite3
from datetime import datetime
import time
import shutil
import errno

from calibration_parameters import CalibrationParameters

class ModelRunnerDB2(object):
    """ Class for setting up, and storing modeling session and run
        information in a sqlite-base DB.  A model runner DB stores
        information about modeling sessions.  A session is composed
        of one or more modeling runs.  There is a one-to-many
        relationship between sessions and runs (i.e. a session can be
        associated with many runs, but a run can only be associated
        with a single session).  Details about calibration run 
        post-processing are stored in the postprocess table.
    """
    DB_VERSION = 2.0
    
    @classmethod
    def _createTables(cls, conn):
        cursor = conn.cursor()
        
        cls._createSessionTable(cursor)
        cls._createRunTable(cursor)
        cls._createPostprocessTable(cursor)
        cls._createRunfitnessTable(cursor)
        cls._createUserfitnessTable(cursor)
        
        cursor.close()

    @classmethod
    def _createSessionTable(cls, cursor):
        cursor.execute("""CREATE TABLE IF NOT EXISTS version
(version REAL)""")
        cursor.execute("""INSERT INTO version (version) VALUES (?)""", (cls.DB_VERSION,))
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS session 
(id INTEGER PRIMARY KEY ASC,
starttime DATETIME DEFAULT CURRENT_TIMESTAMP,
endtime DATETIME,
user TEXT NOT NULL,
project TEXT NOT NULL,
notes TEXT,
iterations INTEGER NOT NULL,
processes INTEGER NOT NULL,
basedir TEXT NOT NULL,
cmd_proto TEXT NOT NULL,
status TEXT NOT NULL,
CHECK (status="submitted" OR status="complete" OR status="aborted")
)
""")

    @classmethod
    def _createRunTable(cls, cursor):
        cursor.execute("""CREATE TABLE IF NOT EXISTS run
(id INTEGER PRIMARY KEY ASC,
session_id INTEGER NOT NULL REFERENCES session (id) ON DELETE CASCADE,
starttime TEXT DEFAULT CURRENT_TIMESTAMP,
endtime TEXT,
worldfile TEXT NOT NULL,
param_s1 REAL,
param_s2 REAL,
param_s3 REAL,
param_sv1 REAL,
param_sv2 REAL,
param_gw1 REAL,
param_gw2 REAL,
param_vgsen1 REAL,
param_vgsen2 REAL,
param_vgsen3 REAL,
param_svalt1 REAL,
param_svalt2 REAL,
cmd_raw TEXT NOT NULL,
output_path TEXT NOT NULL,
job_id TEXT NOT NULL,
status TEXT NOT NULL
CHECK (status="PEND" OR status="RUN" OR status="PUSP" OR 
status="USUSP" OR status="SSUSP" OR status="DONE" OR 
status="EXIT" OR status="UNKWN" OR status="WAIT" OR status="ZOMBI")
)
""")
        # Create indices on run table
        cursor.execute("""CREATE INDEX IF NOT EXISTS run_sess_idx ON 
run (session_id)""")
    
    @classmethod
    def _createPostprocessTable(cls, cursor):
        cursor.execute("""CREATE TABLE IF NOT EXISTS postprocess
(id INTEGER PRIMARY KEY ASC,
session_id INTEGER NOT NULL REFERENCES session (id) ON DELETE CASCADE,
obs_filename TEXT,
fitness_period TEXT,
exclude_date_ranges TEXT,
obs_runoff_ratio REAL
CHECK (fitness_period="daily" OR fitness_period="weekly" OR fitness_period="monthly" OR fitness_period="yearly" OR fitness_period=NULL)
)
""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS postprocess_option
(id INTEGER PRIMARY KEY ASC,
postprocess_id INTEGER NOT NULL REFERENCES postprocess (id) ON DELETE CASCADE,
attr TEXT,
value TEXT
)
""")
        # Create indices
        cursor.execute("""CREATE INDEX IF NOT EXISTS postproc_sess_idx ON 
postprocess (session_id)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS postproc_opt_idx ON 
postprocess_option (postprocess_id)""")
    
    @classmethod
    def _createRunfitnessTable(cls, cursor):
        cursor.execute("""CREATE TABLE IF NOT EXISTS runfitness
(id INTEGER PRIMARY KEY ASC,
postprocess_id INTEGER NOT NULL REFERENCES postprocess (id) ON DELETE CASCADE,
run_id INTEGER NOT NULL REFERENCES run (id) ON DELETE CASCADE,
nse REAL,
nse_log REAL,
pbias REAL,
rsr REAL,
runoff_ratio REAL
)
""")
        # Create indices on runfitness table
        cursor.execute("""CREATE INDEX IF NOT EXISTS runfit_postproc_idx ON 
runfitness (postprocess_id)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS runfit_run_idx ON 
runfitness (run_id)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS nse_idx ON
runfitness (nse)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS nse_log_idx ON
runfitness (nse_log)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS pbias_idx ON
runfitness (pbias)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS rsr_idx ON
runfitness (rsr)""")
    
    @classmethod
    def _createUserfitnessTable(cls, cursor):
        cursor.execute("""CREATE TABLE IF NOT EXISTS userfitness
(id INTEGER PRIMARY KEY ASC,
runfitness_id INTEGER NOT NULL REFERENCES runfitness (id) ON DELETE CASCADE,
attr TEXT,
value TEXT
)
""")
        # Create indices on userfitness table
        cursor.execute("""CREATE INDEX IF NOT EXISTS userfit_runfit_idx ON 
userfitness (runfitness_id)""")
    
    @classmethod
    def migrateDbToCurrentVersion(cls, db_path):
        """ DO NOT CALL THIS FUNCTION UNLESS YOU KNOW WHAT YOU ARE DOING """
        if not os.path.exists(db_path):
            raise IOError("Database {0} does not exist".format(db_path), errno.ENOENT)
        
        version = cls.getDbVersion(db_path)
        
        if version is None:
            raise Exception("Database {0} appears to be empty and does not need to be migrated".format(db_path))
        if version == cls.DB_VERSION:
            raise Exception("Database {0} already is version {1} and does not need to be migrated".format(db_path, cls.DB_VERSION))
        if version == 1.0:
            # Migrate DB
            # Back-up old db
            backup_path = "{0}_backup_{1}".format(db_path, int(time.time()))
            print("Backing up existing database to {0}".format(backup_path))
            shutil.move(db_path, backup_path)
            
            # Make new database stored in db_path
            conn = sqlite3.connect(db_path, check_same_thread = False)
            # Give us access to fields by name
            conn.row_factory = sqlite3.Row 
            cls._createTables(conn)
            
            # Copy data from old DB to new
            cursor = conn.cursor()
            
            #import pdb; pdb.set_trace()
            
            from model_runner_db import ModelRunnerDB
            old_db = ModelRunnerDB(backup_path)
            sessions = old_db.getSessions()
            for s in sessions:
                cursor.execute("""INSERT INTO session
(id, starttime, endtime, user, project, notes, iterations, processes, basedir, cmd_proto, status)
VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
(s.id, s.starttime, s.endtime, s.user, s.project, s.notes, s.iterations, s.processes,
s.basedir, s.cmd_proto, s.status))
                cursor.execute("""SELECT LAST_INSERT_ROWID() from session""")
                mostRecentSessionID = cursor.fetchone()[0]
                assert(mostRecentSessionID == s.id)
                
                post_proc_id = None
                need_postproc_tbl = False
                if s.obs_filename:
                    # Flag that we need to create a postprocess table for this session later
                    need_postproc_tbl = True
                runs = old_db.getRunsInSession(s.id)
                for r in runs:
                    cursor.execute("""INSERT INTO run
(id, session_id, starttime, endtime, worldfile, 
param_s1, param_s2, param_s3, param_sv1, param_sv2, param_gw1, param_gw2, 
param_vgsen1, param_vgsen2, param_vgsen3, param_svalt1, param_svalt2,
cmd_raw, output_path, job_id, status)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
(r.id, mostRecentSessionID, r.starttime, r.endtime, r.worldfile,
r.param_s1, r.param_s2, r.param_s3, r.param_sv1, r.param_sv2, r.param_gw1, r.param_gw2,
r.param_vgsen1, r.param_vgsen2, r.param_vgsen3, r.param_svalt1, r.param_svalt2,
r.cmd_raw, r.output_path, r.job_id, r.status))
                    cursor.execute("""SELECT LAST_INSERT_ROWID() from run""")
                    mostRecentRunID = cursor.fetchone()[0]
                    assert(mostRecentRunID == r.id)
                    
                    if need_postproc_tbl:
                        if r.fitness_period:
                            if post_proc_id is None:
                                # Create postprocess entry
                                cursor.execute("""INSERT into postprocess
    (session_id, obs_filename, fitness_period)
    VALUES (?,?,?)""",
                                               (mostRecentSessionID, s.obs_filename, r.fitness_period))
                                cursor.execute("""SELECT LAST_INSERT_ROWID() from postprocess""")
                                post_proc_id = cursor.fetchone()[0]
                            # Add run fitness entry
                            cursor.execute("""INSERT into runfitness
(postprocess_id, run_id, nse, nse_log, pbias, rsr)
VALUES (?,?,?,?,?,?)""",
                            (post_proc_id, r.id, r.nse, r.nse_log, r.pbias, r.rsr))
             
            conn.commit()
            cursor.close()
            conn.close()        
                        
        else:
            raise Exception("Database {0} is version {1}, which cannot be migrated by this version of the code".format(db_path, version))
             
    @classmethod
    def getDbVersion(cls, db_path):
        version = None
        try:
            conn = sqlite3.connect(db_path, check_same_thread = False)
            # Give us access to fields by name
            conn.row_factory = sqlite3.Row 
            cursor = conn.cursor()
            cursor.execute("""SELECT * FROM version""")
            version = cursor.fetchone()[0]
        
        except sqlite3.Error as e:
            if e.args[0] == "no such table: version":
                # See if the DB is empty
                cursor.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
                res = cursor.fetchall()
                if len(res) > 0: 
                    version = 1.0
        finally:
            conn.close() 
           
        return version
    
    def __init__(self, db_path):
        """ Initialize the DB. Will create a connection to the sqlite DB
            pointed to by db_path
            
            @raise Exception if database needs to be migrated to latest version.
        """
        self._dbPath = db_path
        
        self.version = self.getDbVersion(db_path)
        if self.version is not None and self.version != self.DB_VERSION:
            #raise Exception("Database version is {0}, database must be migrated before continuing".format(self.version))
            # Migrate database
            print("Database version is {0}, attempting to migrate to version {1}...".format(self.version, self.DB_VERSION))
            self.migrateDbToCurrentVersion(db_path)
        
        self._conn = sqlite3.connect(db_path, check_same_thread = False)
        # Give us access to fields by name
        self._conn.row_factory = sqlite3.Row 
        
        if self.version is None:
            self._createTables(self._conn)

    def __del__(self):
        self._conn.close()

    def close(self):
        self._conn.close()
        
    def getMostRecentSessionID(self):
        """ Returns the ID of the session most recently inserted by the given
            instance of ModelRunnerDB.  Note: if another process has active
            connections to the underlying sqlite database, the session ID
            returned will not be the same as the the session ID of the last
            session inserted by a given instance of ModelRunnerDB, as the other
            processes may have done in insert into the session table after the 
            most recent insert done by a given instance of ModelRunnerDB.
        """
        cursor = self._conn.cursor()

        cursor.execute("""SELECT id,MAX(starttime) from session""")
        res = cursor.fetchone()[0]

        cursor.close()

        return res


    def insertSession(self, user, project, notes, iterations, processes,
                      basedir, cmd_proto):
        """ Creates a new session with a starttime of the current time,
            and a status of 'submitted'

            @param user String representing the username of the user who is submitting jobs
            @param project String representing a short description of the project this 
                                 modeling session is associated with
            @param notes String representing a description of the calibraton session
            @param iterations Integer representing the number of iterations to calibrate each 
                                 world file
            @param processes Integer representing the number of simultaneous processes (jobs) to
                                 submit
            @param basedir String representing the basedir where run results are stored
            @param cmd_proto String representing the command template for running RHESSys

            @return The ID of the session
        """
        cursor = self._conn.cursor()

        # Create a new session
        cursor.execute("""INSERT INTO session 
(user,project,notes,iterations,processes,basedir,cmd_proto,status)
VALUES (?,?,?,?,?,?,?,?)""", 
(user, project, notes, iterations, processes, basedir, cmd_proto, 'submitted'))

        self._conn.commit()

        # Get the rowid just inserted
        cursor.execute("""SELECT LAST_INSERT_ROWID() from session""")
        #self.__mostRecentSessionID = cursor.fetchone()[0]
        mostRecentSessionID = cursor.fetchone()[0]

        cursor.close()

        #return self.__mostRecentSessionID
        return mostRecentSessionID

    def updateSessionEndtime(self, id, endtime, status):
        """ Updates the end time and the status of the given session

            @param id Integer representing the ID of the session to update
            @param endtime datetime representing the end time of the session (in UTC not 
                                  local time)
            @param status String: one of: submitted, complete, aborted
        """
        cursor = self._conn.cursor()

        cursor.execute("""UPDATE session SET endtime=?, status=? WHERE
id=?""", (endtime.strftime("%Y-%m-%d %H:%M:%S"), status, id))

        self._conn.commit()

        cursor.close()

    def updateSessionObservationFilename(self, id, obs_filename):
        """ Updates the obs_filename used for calculating model
            fitness statistics for runs associated with the session

            @param id Integer representing the ID of the session to update
            @param obs_filename String representing the name of the observation file
                                    used to calculate fitness statistics
                                    for model runs
        """
        cursor = self._conn.cursor()

        cursor.execute("""UPDATE session SET obs_filename=? WHERE id=?""", 
                       (obs_filename, id))

        self._conn.commit()

        cursor.close()

    def getMostRecentRunID(self, session_id):
        """ Returns the ID of the run most recently inserted by the given
            instance of ModelRunnerDB.  Note: if another process has active
            connections to the underlying sqlite database, the run ID
            returned will not be the same as the the run ID of the last
            run inserted by a given instance of ModelRunnerDB, as the other
            processes may have done in insert into the run table after the 
            most recent insert done by a given instance of ModelRunnerDB.

            @param session_id Integer representing the session associated with this run
        """
        cursor = self._conn.cursor()

        cursor.execute("""SELECT id,MAX(starttime) FROM run
WHERE session_id=?""", (session_id,))

        res = cursor.fetchone()[0]

        cursor.close()

        return res

    def insertRun(self, session_id, worldfile,
                  param_s1, param_s2, param_s3,
                  param_sv1, param_sv2,
                  param_gw1, param_gw2,
                  param_vgsen1, param_vgsen2, param_vgsen3,
                  param_svalt1, param_svalt2,
                  cmd_raw, output_path,
                  job_id,
                  fitness_period=None,
                  nse=None, nse_log=None, 
                  pbias=None, rsr=None,
                  user1=None, user2=None, user3=None):
        """ Creates a new run with a starttime of the current time,
            and a status of 'PEND'.  Note: you must specify as null (None)
            any of the param_* arguments that are no used in the run.

            Arguments:
            @param session_id Integer representing session to associate this run with
            @param worldfile String representing name of worldfile associated with this run
            @param s1 Double: decay of hydraulic conductivity with depth (m)
            @param s2 Double: surface hydraulic conductivity (K)
            @param s3 Double: soil depth (OPTIONAL; default=None)
            @param sv1 Double: m to vertical drainage
            @param sv2 Double: K to vertical drainage
            @param gw1 Double: hillslope sat_to_gw_coeff
                               (amount of water moving from saturated to
                               groundwater store)
            @param gw2 Double: hillslope gw_loss_coeff 
                               (amount of water moving from groundwater
                               to the stream)
            @param vgsen1 Double: Multiplies specific leaf area 
                               (SLA -- ratio of leaf area to dry weight)
            @param vgsen2 Double: Multiplies ratio of shaded to 
                               sunlit leaf area
            @param vgsen3 Double: changes allocation of net 
                               photosynthate based on current LAI
                               (only appl. if using Dickenson C 
                               allocation if not using Dickenson,
                               set to 1.0)
            @param svalt1 Double: Multiplies psi air entry pressure
            @param svalt2 Double: Multiplies pore size
            @param cmd_raw String representing the raw command that invoked this run
                                     of RHESSys
            @param output_path String representing the dir where results for this run are 
                                     stored
            @param job_id String representing the job ID associated with this run
            @param fitness_period String indicating time step over which 
                                      all fitness results were calculated.
                                      One of: daily, monthly, yearly
            @param nse Double: Nash-Sutcliffe efficiency
            @param nse_log Double: Nash-Sutcliffe efficiency (log)
            @param pbias Double: Percent bias
            @param rsr Double: RMSE / STDEV_obs
            @param user1 Double: User-defined run fitness result 
            @param user2 Double: User-defined run fitness result 
            @param user3 Double: User-defined run fitness result 

            @return The ID of the run created
        """
        cursor = self._conn.cursor()

        # Create a new run
        cursor.execute("""INSERT INTO run 
(session_id,worldfile,
param_s1,param_s2,param_s3,
param_sv1,param_sv2,
param_gw1,param_gw2,
param_vgsen1,param_vgsen2,param_vgsen3,
param_svalt1,param_svalt2,
cmd_raw,output_path,
job_id,status,
nse,nse_log,pbias,rsr,user1,user2,user3,fitness_period)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                       (session_id, worldfile,
                        param_s1, param_s2, param_s3,
                        param_sv1, param_sv2,
                        param_gw1, param_gw2,
                        param_vgsen1, param_vgsen2, param_vgsen3,
                        param_svalt1, param_svalt2,
                        cmd_raw, output_path,
                        job_id, "PEND",
                        nse, nse_log, pbias, rsr, user1, user2, user3, fitness_period))

        self._conn.commit()

        # Get the rowid just inserted
        cursor.execute("""SELECT LAST_INSERT_ROWID() from run""")
        res = cursor.fetchone()[0]
        
        cursor.close()

        return res

    def updateRunEndtime(self, id, endtime, status): 
        """ Updates the end time and the status of the given run

            @param id Integer representing the ID of the run to update
            @param endtime datetime representing the end time of the run (in UTC not 
                                  local time)
            @param status String: one of: PEND, RUN, PUSP, USUSP, SSUSP, DONE,
                                  EXIT, UNKWN, WAIT, ZOMBI
        """
        cursor = self._conn.cursor()

        cursor.execute("""UPDATE run SET endtime=?, status=? WHERE
id=?""", (endtime.strftime("%Y-%m-%d %H:%M:%S"), status, id))

        self._conn.commit()

        cursor.close()

    def getRunStatus(self, id):
        """ Gets the status of the given run

            @param id  Integer representing the ID of the run to update
            
            @return String representing the the status of the run, 
            One of: PEND, RUN, PUSP, USUSP, SSUSP, DONE,   
            EXIT, UNKWN, WAIT, ZOMBI
        """
        cursor = self._conn.cursor()

        cursor.execute("""SELECT status FROM run WHERE id=?""", (id,))
        res = cursor.fetchone()[0]

        cursor.close()

        return res

    def updateRunStatus(self, id, status, endtime=None):
        """ Updates the status of the given run.  If endtime is not None, will 
            update endtime as well as status

            @param id Integer representing the ID of the run to update
            @param status String: one of: PEND, RUN, PUSP, USUSP, SSUSP, DONE,
                                  EXIT, UNKWN, WAIT, ZOMBI
            @param endtime datetime representing the end time of the run (in UTC not 
                                  local time)
            
        """
        cursor = self._conn.cursor()

        if None == endtime:
            cursor.execute("""UPDATE run SET status=? WHERE id=?""", 
                           (status, id))
        else:
            cursor.execute("""UPDATE run SET status=?, endtime=? where id=?""",
                           (status, endtime.strftime("%Y-%m-%d %H:%M:%S"), id))

        self._conn.commit()

        cursor.close()
        
    def updateRunJobId(self, id, job_id):
        """ Updates the job_id of the given run.  

            @param id Integer representing the ID of the run to update
            @param job_id String representing the job ID of the run
            
        """
        cursor = self._conn.cursor()

        cursor.execute("""UPDATE run SET job_id=? where id=?""",
                       (job_id, id))

        self._conn.commit()

        cursor.close()

    def updateRunFitnessResults(self, id, fitness_period, nse, nse_log, 
                                pbias=None, rsr=None,
                                user1=None, user2=None, user3=None):
        """ Updates fitness results of a model run

            @param id Integer representing The ID of the run to update
            @param fitness_period String indicating time step over which 
                                      all fitness results were calculated.
                                      One of: daily, monthly, yearly
            @param nse Double: Nash-Sutcliffe efficiency
            @param nse_log Double: Nash-Sutcliffe efficiency (log)
            @param pbias Double: Percent bias
            @param rsr Double: RMSE / STDEV_obs
            @param user1 Double: User-defined run fitness result 
            @param user2 Double: User-defined run fitness result 
            @param user3 Double: User-defined run fitness result 
        """
        cursor = self._conn.cursor()
        
        cursor.execute("""UPDATE run SET 
nse=?, nse_log=?, pbias=?, rsr=?, user1=?, user2=?, user3=?, fitness_period=?
WHERE id=?""", (nse, nse_log, pbias, rsr, user1, user2, user3, fitness_period, id))

        self._conn.commit()

        cursor.close()

    def _runRecordToObject(self, row):
        """ Translate a record from the "run" table into a ModelRun2
            object

            @param row sqlite3.Row: row record to be copied into the
                                  CalibratioRun object

            @return ModelRun2 representing the run record
        """
        run = ModelRun2()
        run.id = row["id"]
        run.session_id = row["session_id"]
        run.starttime = datetime.strptime(row["starttime"], 
                                          "%Y-%m-%d %H:%M:%S")
        if row["endtime"] != None:
            run.endtime = datetime.strptime(row["endtime"],
                                            "%Y-%m-%d %H:%M:%S")
        run.worldfile = row["worldfile"]
        run.param_s1 = row["param_s1"]
        run.param_s2 = row["param_s2"]
        run.param_s3 = row["param_s3"]
        run.param_sv1 = row["param_sv1"]
        run.param_sv2 = row["param_sv2"]
        run.param_gw1 = row["param_gw1"]
        run.param_gw2 = row["param_gw2"]
        run.param_vgsen1 = row["param_vgsen1"]
        run.param_vgsen2 = row["param_vgsen2"]
        run.param_vgsen3 = row["param_vgsen3"]
        run.param_svalt1 = row["param_svalt1"]
        run.param_svalt2 = row["param_svalt2"]
        run.cmd_raw = row["cmd_raw"]
        run.output_path = row["output_path"]
        run.job_id = row["job_id"]
        run.status = row["status"]
        run.nse = row["nse"]
        run.nse_log = row["nse_log"]
        run.pbias = row["pbias"]
        run.rsr = row["rsr"]
        run.user1 = row["user1"]
        run.user2 = row["user2"]
        run.user3 = row["user3"]
        run.fitness_period = row["fitness_period"]

        return run

    def getRun(self, run_id):
        """ Get the run with the supplied ID

            @param run_id Integer representing ID of the run to fetch from the DB

            @return ModelRun object.  Returns None if the run_id
            does not exist
        """
        cursor = self._conn.cursor()

        cursor.execute("""SELECT * from run WHERE id=?""", (run_id,))
        row = cursor.fetchone()

        if row == None:
            cursor.close()
            return None

        run = self._runRecordToObject(row)

        cursor.close()

        return run

    def getRunsInSession(self, session_id, where_clause=None):
        """ Get all runs associated with a session

            @param session_id Integer representing the session whose list of runs we want

            @return An array of ModelRun objects
        """
        cursor = self._conn.cursor()
        runs = []

        queryProto = "SELECT * from run WHERE session_id=?"
        if where_clause:
            queryProto += " AND " + where_clause

        cursor.execute(queryProto, (session_id,))
        for row in cursor:
            runs.append(self._runRecordToObject(row))

        cursor.close()

        return runs


    def getRunInSession(self, session_id, job_id):
        """ Get all runs associated with a session

            @param session_id Integer representing the session whose list of runs we want
            @param job_id String representing job ID of run to retrieve

            @return ModelRun object
            
            @todo Debug and write test case
        """
        cursor = self._conn.cursor()

        run = None

        cursor.execute("""SELECT * from run WHERE session_id=? AND
job_id=?""", 
                       (session_id, job_id))
        row = cursor.fetchone()
        if row != None:
            run = self._runRecordToObject(row)

        cursor.close()

        return run


    def _sessionRecordToObject(self, row):
        """ Translate a record from the "session" table into a 
            ModelSession2 object

            @param row sqlite3.Row: The row record to be copied into the
                                  CalibratioSession object

            @return ModelSession representing the run record
        """
        session = ModelSession2()
        session.id = row["id"]
        session.starttime = datetime.strptime(row["starttime"], 
                                          "%Y-%m-%d %H:%M:%S")
        if row["endtime"] != None:
            session.endtime = datetime.strptime(row["endtime"],
                                            "%Y-%m-%d %H:%M:%S")
        session.user = row["user"]
        session.project = row["project"]
        session.notes = row["notes"]
        session.iterations = row["iterations"]
        session.processes = row["processes"]
        session.basedir = row["basedir"]
        session.cmd_proto = row["cmd_proto"]
        session.status = row["status"]
        session.obs_filename = row["obs_filename"]

        return session


    def getSession(self, session_id):
        """ Get the session with the supplied ID

            @param session_id Integer representing the ID of the session to fetch from the DB

            @return ModelSession object.  Returns None if the 
            session_id does not exist
        """
        cursor = self._conn.cursor()

        cursor.execute("""SELECT * from session WHERE id=?""", (session_id,))
        row = cursor.fetchone()

        if row == None:
            cursor.close()
            return None

        session = self._sessionRecordToObject(row)

        cursor.close()

        return session


    def getSessions(self):
        """ Get all sessions in the DB

            @return An array of ModelSession objects
        """
        cursor = self._conn.cursor()

        sessions = []

        cursor.execute("""SELECT * from session""")
        for row in cursor:
            sessions.append(self._sessionRecordToObject(row))

        cursor.close()

        return sessions



class ModelSession2(object):
    """ Class for representing a modeling session record stored within a
        model runner DB
    """
    def __init__(self):
        self.id = None
        self.starttime = None
        self.endtime = None
        self.user = None
        self.project = None
        self.notes = None
        self.iterations = None
        self.processes = None
        self.basedir = None
        self.cmd_proto = None
        self.status = None
        self.obs_filename = None

    #def __eq__(self, other):
    #    if not isinstance(other, ModelSession): raise NotImplementedError
    #    return self.id == other.id

class ModelRun2(object):
    """ Class for representing a modeling run with a modeling session
        stored in a model runner DB.  
    """
    def __init__(self):
        self.id = None
        self.session_id = None
        self.starttime = None
        self.endtime = None
        self.worldfile = None
        self.param_s1 = None 
        self.param_s2 = None
        self.param_s3 = None
        self.param_sv1 = None
        self.param_sv2 = None
        self.param_gw1 = None
        self.param_gw2 = None
        self.param_vgsen1 = None
        self.param_vgsen2 = None
        self.param_vgsen3 = None
        self.param_svalt1 = None
        self.param_svalt2 = None
        self.cmd_raw = None
        self.output_path = None
        self.job_id = None
        self.status = None
        self.nse = None
        self.nse_log = None
        self.pbias = None
        self.rsr = None
        self.user1 = None
        self.user2 = None
        self.user3 = None
        self.fitness_period = None
    
    def setCalibrationParameters(self, params):
        """ Set calibration parameters as supplied by parameter class.
            Parameters not supplied in params will be left set to their
            previous valus (i.e. None if no other assignment has ocurred)

            @param params cluster_parameters.CalibrationParameters
        """
        if params.s1 != None:
            self.param_s1 = params.s1
        if params.s2 != None:
            self.param_s2 = params.s2
        if params.s3 != None:
            self.param_s3 = params.s3
        if params.sv1 != None:
            self.param_sv1 = params.sv1
        if params.sv2 != None:
            self.param_sv2 = params.sv2
        if params.gw1 != None:
            self.param_gw1 = params.gw1
        if params.gw2 != None:
            self.param_gw2 = params.gw2
        if params.vgsen1 != None:
            self.param_vgsen1 = params.vgsen1
        if params.vgsen2 != None:
            self.param_vgsen2 = params.vgsen2
        if params.vgsen3 != None:
            self.param_vgsen3 = params.vgsen3
        if params.svalt1 != None:
            self.param_svalt1 = params.svalt1
        if params.svalt2 != None:
            self.param_svalt2 = params.svalt2
            
    def getCalibrationParameters(self):
        """ Get calibration parameters as a 
            cluster_parameters.CalibrationParametersparameter object.
           
            @return cluster_parameters.CalibrationParameters
        """
        params = CalibrationParameters(self.param_s1, self.param_s2, self.param_s3,
                                       self.param_sv1, self.param_sv2, 
                                       self.param_gw1, self.param_gw2,
                                       self.param_vgsen1, self.param_vgsen2, self.param_vgsen3,
                                       self.param_svalt1, self.param_svalt2)
        return params

