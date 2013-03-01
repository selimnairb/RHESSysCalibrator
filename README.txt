RHESSysCalibrator			{#index}
=======================

This software is provided free of charge under the New BSD License. Please see
the following license information:

Copyright (c) 2013, University of North Carolina at Chapel Hill
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name of the University of North Carolina at Chapel Hill nor 
      the names of its contributors may be used to endorse or promote products
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


Authors
-------
Brian Miles <brian_miles@unc.edu>


Introduction
------------
RHESSysCalibrator is a system for managing calibration sessions and run of
RHESSys.  A session contains many runs.  Each run represents a
distinct and uniformly random set of sensitiviy parameters.  
RHESSysCalibrator uses a database (sqlite3) to keep track of each 
session and run.  The program handles launching and management of each
run, and will update its database as run jobs are submitted, begin
running, and finish (either with or without error).  Note that by
defaul RHESSys erroneously returns "1" when it exits without error
(counter to convention).  For job management tools to be able to
corectly sense that RHESSys has exited without error, RHESSys source
must be patched to return "0" on a normal exit.

RHESSysCalibrator does not handle calibration session post processing (i.e.
calibrating Nash-Sutcliffe efficiency v. observed values).  This is a
done by a companion program named rhessys_calibrator_postprocess.py.


Installation
------------
Using Python PyPi:

easy_install --script-dir /path/to/install/scripts rhessyscalibrator

It is recommended that you install the calibrator scripts in a location distinct from
where the Python package will be installed (i.e. ~/bin or /usr/local/bin).  This is 
accomplished by specifying the --script-dir option to easy install (see above).  


Model directory structure
-------------------------
Before running rhessys_calibrator.py to perform calibrations, it is first 
necessary to create the session directory structure.  This is done by
issuing the --create argument, along with the always-required
--basedir argument.  This will create the following directory
structure within $BASEDIR:

db/                        Where the session DB will be stored (by
                            RHESSysCalibrator)
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



