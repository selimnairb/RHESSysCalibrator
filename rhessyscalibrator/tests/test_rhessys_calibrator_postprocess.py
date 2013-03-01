#!/usr/bin/env python
"""@package rhessyscalibrator.tests.test_rhessys_calibrator_postprocess

@brief Unit tests for rhessyscalibrator.postprocess

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
import unittest
import os
from subprocess import *
import re
import string

from rhessyscalibrator.postprocess import RHESSysCalibratorPostprocess

class TestClusterCalibratorPostProcess(unittest.TestCase):

    def setUp(self):
        #self.postProcessor = \
        #    RHESSysCalibratorPostprocess()
        pass

    def testLogTransform(self):
        list1 = [1,10,100,0,10000]
        list2 = [1,10,0,1000,10000]
        
        (log_list1, log_list2) = \
            RHESSysCalibratorPostprocess.logTransform(list1, list2)

        self.assertTrue(len(log_list1) > 0)
        self.assertTrue(len(log_list2) > 0)
        self.assertTrue(len(log_list1) == len(log_list2))
        self.assertTrue(len(list1) > len(log_list1))
        self.assertTrue(len(list2) > len(log_list2))

        self.assertTrue(0 == log_list1[0] and 0 == log_list2[0])
        self.assertTrue(1 == log_list1[1] and 1 == log_list2[1])
        self.assertTrue(4 == log_list1[2] and 4 == log_list2[2])
        

if __name__ == "__main__":
    unittest.main()
