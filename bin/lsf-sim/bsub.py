#!/usr/bin/env python
#
# bsub.py Simulator for LSB bsub command.  Only supports being invoked as:
#         "bjobs command [arguments]" (i.e. run "command" with [arguments])
#
# Author(s): Brian Miles
#  Revision: 0.01: Original
#            0.02: Simulate pending job threshold 
#      Date: 20110530, 20110610
#

# Force use of print function instead of stupid print statement
#  (necessary only for Python 2.6 and prior)
from __future__ import print_function

import sys
from random import *
import sqlite3
import time

conn = sqlite3.connect("lsf-sim.db")
cursor = conn.cursor()

# Create jobs database
cursor.execute("""CREATE TABLE IF NOT EXISTS jobs
(lsf_job_id INTEGER NOT NULL,
raw_cmd TEXT NOT NULL,
lsf_stat TEXT NOT NULL
CHECK (lsf_stat="PEND" OR lsf_stat="RUN" OR lsf_stat="PUSP" OR
lsf_stat="USUSP" OR lsf_stat="SSUSP" OR lsf_stat="DONE" OR
lsf_stat="EXIT" OR lsf_stat="UNKWN" OR lsf_stat="WAIT" OR lsf_stat="ZOMBI")
)
""")
cursor.close()

cursor = conn.cursor()

queue = "chunk"
if "-q" == sys.argv[1]:
    # Get queue
    queue = sys.argv[2]
    # Skip argv[3] "-o"
    # Skip argv[4] $OUTPUT_PATH
    raw_cmd = sys.argv[5]
    for arg in sys.argv[6:]:
        raw_cmd += " " + arg
elif "-o" == sys.argv[1]:
    # Skip argv[1] "-o"
    # Skip argv[2] $OUTPUT_PATH
    raw_cmd = sys.argv[3]
    for arg in sys.argv[4:]:
        raw_cmd += " " + arg
else:
    # No arguments
    raw_cmd = sys.argv[1]
    for arg in sys.argv[2:]:
        raw_cmd += " " + arg 

# Simulate pending job threshold
cursor.execute("""SELECT count(*) FROM jobs""")
row = cursor.fetchone()

# Experimental code to simulate pending job threshold on cluster
#while row[0] > 4:
#    print("User <joeuser>: Pending job threshold reached. Retrying in 60 seconds...\n", sys.stderr)
#    time.sleep(15)
#    cursor.execute("""SELECT count(*) FROM jobs""")
#    row = cursor.fetchone()

# A random integer
job_number = randrange(1,100000000, 1)

# Insert new job into DB
cursor.execute("""INSERT INTO jobs VALUES (?, ?, ?)""", 
               (job_number, raw_cmd, "PEND"))

print("Job <%d> is submitted to the default queue <%s>." % \
    (job_number, queue))

cursor.close()
conn.commit()
conn.close()

sys.exit(0)
