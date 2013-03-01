#!/usr/bin/env python
"""@package rhessys_calibrator

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

Old revision history:
---------------------
 Revisions: 20110606: fixed queue IPC implementation, refactored 
                      CalibrationRunner class and descendants
            20110626: 1.0.3: refactored CalibrationRunner to ModelRunner for generalization
                             only update accounting of active jobs after verifying that
                             job returned by bjobs belongs to us (is in current session
                             record)
            20110628: fixed bug where --create option required iterations and jobs options
                      to be specified
            20110710: 1.0.4: Added subprocess parallelization method; Removed pipe-based IPC method.
            20120105: 1.0.5: Added support for running RHESSys in TOPMODEL model (i.e. no flow tables);
                             Added support for using the same calibration parameters for both horizontal
                             and vertical m parameter
                             Changed LSF queues to reflect those on KillDevil
            20120114: 1.0.6: Added support for running bsub with exclusive mode parameter
                             Removed old PIPE-based CalibrationRunner implementation
             20120215: 1.0.7: Updated --use_horizontal_m_for_vertical to apply also to the K parameter
                              (new option is --use_horizontal_m_and_K_for_vertical)
            20120327: 1.0.8: Added support for vgsen and svalt calibration parameters
            20120328: 1.0.9: Fixed bug where third -s parameter was not set by CalibrationParameters
            20120529: 1.0.10: Added filter for worldfile lookup that excludes redefine worldfiles.
                            

"""
import sys

from rhessyscalibrator.calibrator import RHESSysCalibrator

if __name__ == "__main__":
    RHESSysCalibrator = RHESSysCalibrator()
    # main's return value will be the exit code
    sys.exit(RHESSysCalibrator.main(sys.argv))
