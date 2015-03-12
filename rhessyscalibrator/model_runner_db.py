"""@package rhessyscalibrator.model_runner_db

@brief Classes that store data about calibration runs to an SQLite3 database

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

Old revision history:
---------------------
 Revisions: 20110607: Add observation file field to session table
            20110626: Refactor to rename "cluster_calibrator_db" to "model_runner_db"
            20120327: Added support for svalt calibration parameters; Added support for fitness period for runs: daily, monthly, yearly


"""
import sqlite3
from datetime import datetime

from calibration_parameters import CalibrationParameters

class ModelRunnerDB(object):
    """ Class for setting up, and storing modeling session and run
        information in a sqlite-base DB.  A model runner DB stores
        information about modeling sessions.  A session is composed
        of one or more modeling runs.  There is a one-to-many
        relationship between sessions and runs (i.e. a session can be
        associated with many runs, but a run can only be associated
        with a single session)
    """
    def __init__(self, db_path):
        """ Initialize the DB. Will create a connection to the sqlite DB
            pointed to by db_path
        """
        self._dbPath = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread = False)
        # Give us access to fields by name
        self._conn.row_factory = sqlite3.Row 
        self._createSessionTable()
        self._createRunTable()

    def __del__(self):
        self._conn.close()

    def close(self):
        self._conn.close()

    def _createSessionTable(self):
        cursor = self._conn.cursor()

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
obs_filename TEXT
CHECK (status="submitted" OR status="complete" OR status="aborted")
)
""")

        cursor.close()

    def _createRunTable(self):
        cursor = self._conn.cursor()

        # Create session table
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
status="EXIT" OR status="UNKWN" OR status="WAIT" OR status="ZOMBI"),
nse REAL,
nse_log REAL,
pbias REAL,
rsr REAL,
user1 REAL,
user2 REAL,
user3 REAL,
fitness_period TEXT
CHECK (fitness_period="daily" OR fitness_period="weekly" OR fitness_period="monthly" OR fitness_period="yearly" OR fitness_period=NULL)
)
""")

        # Create indices on run table
        cursor.execute("""CREATE INDEX IF NOT EXISTS job_idx ON 
run (job_id)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS nse_idx ON
run (nse)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS nse_log_idx ON
run (nse_log)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS pbias_idx ON
run (pbias)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS rsr_idx ON
run (rsr)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS user1_idx ON
run (user1)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS user2_idx ON
run (user2)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS user3_idx ON
run (user3)""")

        cursor.close()

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
        """ Translate a record from the "run" table into a ModelRun
            object

            @param row sqlite3.Row: row record to be copied into the
                                  CalibratioRun object

            @return ModelRun representing the run record
        """
        run = ModelRun()
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
            ModelSession object

            @param row sqlite3.Row: The row record to be copied into the
                                  CalibratioSession object

            @return ModelSession representing the run record
        """
        session = ModelSession()
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



class ModelSession(object):
    """ Class for representing a modeling session record stored within a
        model runner DB
    """
    def __init__(self):
        self.id = None
        self.startime = None
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

class ModelRun(object):
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

