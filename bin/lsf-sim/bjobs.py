#!/usr/bin/env python
#
# bjobs.py Simulator for LSB bjobs command.  Only supports being invoked as:
#          "bjobs -a" (i.e. show jobs of all statuses).
#
#          Note: The companion bsub.py must be run at least once prior to 
#                running bjobs.py (bsub.py creates the sqlite database
#                needed for both)
#
# Author(s): Brian Miles
#  Revision: 0.01
#      Date: 20110530
#

import sys
from random import *
import sqlite3
import string

conn = sqlite3.connect("lsf-sim.db")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

if "-a" == sys.argv[1]:
    cursor.execute("""SELECT * FROM jobs""")
    rows = cursor.fetchall()
    if len(rows) > 0:
        # Print header
        print "%s%s%s%s%s%s%s%s" % \
            (string.ljust("JOBID", 10),
             string.ljust("USER", 8),
             string.ljust("STAT", 6),
             string.ljust("QUEUE", 11),
             string.ljust("FROM_HOST", 12),
             string.ljust("EXEC_HOST", 12),
             string.ljust("JOB_NAME", 11),
             string.ljust("SUBMIT_TIME", 12))

        for row in rows:
            print "%s%s%s%s%s%s%s%s" % \
                (string.ljust("%d" % row["lsf_job_id"], 10),
                 string.ljust("joeuser", 8),
                 string.ljust(row["lsf_stat"], 6),
                 string.ljust("week", 11),
                 string.ljust("login-node", 12),
                 string.ljust("compute1", 12),
                 string.ljust(row["raw_cmd"][:10], 11),
                 string.ljust("MON DD HH:MM", 12))

            if "PEND" == row["lsf_stat"]:
                if random() > 0.10:
                    # Make the job go from PEND to run
                    cursor.execute("""UPDATE jobs SET lsf_stat=?
WHERE lsf_job_id=?""", ("RUN", row["lsf_job_id"]))

            if "RUN" == row["lsf_stat"]:
                if random() > 0.75:
                    # Randomly decide to make this job finish
                    cursor.execute("""UPDATE jobs SET lsf_stat=? 
WHERE lsf_job_id=?""", ("DONE", row["lsf_job_id"]))
        

conn.commit()
conn.close()

sys.exit(0)
