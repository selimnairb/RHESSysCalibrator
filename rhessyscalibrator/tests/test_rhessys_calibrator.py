#!/usr/bin/env python
"""@package rhessyscalibrator.tests.test_rhessys_calibrator

@brief Unit tests for rhessyscalibrator.calibrator

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
import os
import stat
import subprocess
from subprocess import *
import re
import string
from shutil import rmtree
from zipfile import ZipFile
import tempfile

from rhessyscalibrator.calibrator import RHESSysCalibrator
from rhessyscalibrator.model_runner_db import *
from rhessyscalibrator.calibration_parameters import *

class TestClusterCalibrator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # We zip the test RHESSys mode to be nice to GitHub, unzip it
        cls.basedir = os.path.abspath('./rhessyscalibrator/tests/data/DR5')
        basedirZip = "%s.zip" % (cls.basedir,)
        if not os.access(basedirZip, os.R_OK):
            raise IOError(errno.EACCES, "Unable to read test RHESSys model zip %s" %
                      basedirZip)
        basedirParent = os.path.split(basedirZip)[0]    
        if not os.access(basedirParent, os.W_OK):
            raise IOError(errno.EACCES, "Unable to write to test RHESSys model parent dir %s" %
                          basedirParent)
        zip = ZipFile(basedirZip, 'r')
        extractDir = os.path.split(cls.basedir)[0]
        zip.extractall(path=extractDir)
            
        # Build RHESSys
        currDir = os.getcwd()
        os.chdir('./rhessyscalibrator/tests/data/DR5/rhessys/src/rhessys')
        res = subprocess.call('make install RHESSYS_BIN=../../bin', shell=True)
        # Make sure binary is executable else some tests will fail
        os.chmod('../../bin/rhessys5.15', stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        os.chdir(currDir)    
        
        cls.clusterCalibrator = RHESSysCalibrator()
    
    @classmethod
    def tearDownClass(cls):
        rmtree(cls.basedir)

    def testCreateVerifyDirectoryStructure(self):
        basedir = "./rhessyscalibrator/tests/data/.test"
    
        os.mkdir(basedir)

        self.clusterCalibrator.createVerifyDirectoryStructure(basedir)

        file = open(os.path.join(basedir, "cmd.proto"))
        cmd_proto_str = file.read()
        file.close()

        # Remove basedir and contents
        for root, dirs, files in os.walk(basedir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

        os.rmdir(basedir)

        self.assertNotEqual(cmd_proto_str, '')

    def testGetRHESSysExecPath(self):
        
        (res, filename, pathToFilename) = \
            self.clusterCalibrator.getRHESSysExecPath(self.basedir)

        self.assertTrue(res)
        if res:
            print("Filename: %s, path of filename: %s", 
                  filename, pathToFilename)
        else:
            print "no executable found in %s" % self.basedir

    def testGetTecfilePath(self):
        
        (res, pathToFilename) = \
            self.clusterCalibrator.getTecfilePath(self.basedir)

        self.assertTrue(res)
#        if res:
#            print("path of filename: %s", 
#                  pathToFilename)
#        else:
#            print "no tecfile found in %s" % basedir

    def testGetWorldFiles(self):
        
        worldfiles = self.clusterCalibrator.getWorldfiles(self.basedir)

        self.assertTrue("world5m_dr5" in worldfiles)
        #self.assertTrue("world_dr6_20110426d_10m-v3" in worldfiles)

    def testVerifyFlowTables(self):
        
        worldfiles = self.clusterCalibrator.getWorldfiles(self.basedir)

        (res, flowtablePath, surfaceFlowtablePath, worldfilesWithoutFlowTables, worldfilesWithoutSurfaceFlowTables) = \
            self.clusterCalibrator.verifyFlowTables(self.basedir, worldfiles.keys())

        self.assertTrue(res)
#        if res:
#            print "All worldfiles have flow tables"
#        else:
#            for worldfile in worldfilesWithoutFlowTables:
#                print "%s has no flow table" % worldfile 

    def testParseCmdProtoCustomRangesDefaultParams(self):
        cmd_proto = """$rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable"""
        cmd_proto += """ -pre $output_path -s $s1[0.02, 30.0] $s2[0.5, 100.0] $s3[0.2, 20.0] -sv $sv1[0.03, 10.0] $sv2[0.1, 250.0] -gw $gw1[0.005, 0.5] $gw2[0.05, 0.75]"""
        
        paramsProtoRef = CalibrationParametersProto()
        
        paramsProto = CalibrationParametersProto()
        paramsProto.parseParameterString(cmd_proto)
        
        self.assertTrue(paramsProto.s1)
        self.assertNotEqual(paramsProto.parameterRanges['s1'][0], paramsProtoRef.parameterRanges['s1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['s1'][1], paramsProtoRef.parameterRanges['s1'][1])
        self.assertEqual(paramsProto.parameterRanges['s1'][0], 0.02)
        self.assertEqual(paramsProto.parameterRanges['s1'][1], 30)
        
        self.assertTrue(paramsProto.s2)
        self.assertNotEqual(paramsProto.parameterRanges['s2'][0], paramsProtoRef.parameterRanges['s2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['s2'][1], paramsProtoRef.parameterRanges['s2'][1])
        self.assertEqual(paramsProto.parameterRanges['s2'][0], 0.5)
        self.assertEqual(paramsProto.parameterRanges['s2'][1], 100)
        
        self.assertTrue(paramsProto.s3)
        self.assertNotEqual(paramsProto.parameterRanges['s3'][0], paramsProtoRef.parameterRanges['s3'][0])
        self.assertNotEqual(paramsProto.parameterRanges['s3'][1], paramsProtoRef.parameterRanges['s3'][1])
        self.assertEqual(paramsProto.parameterRanges['s3'][0], 0.2)
        self.assertEqual(paramsProto.parameterRanges['s3'][1], 20)
        
        self.assertTrue(paramsProto.sv1)
        self.assertNotEqual(paramsProto.parameterRanges['sv1'][0], paramsProtoRef.parameterRanges['sv1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['sv1'][1], paramsProtoRef.parameterRanges['sv1'][1])
        self.assertEqual(paramsProto.parameterRanges['sv1'][0], 0.03)
        self.assertEqual(paramsProto.parameterRanges['sv1'][1], 10)
        
        self.assertTrue(paramsProto.sv2)
        self.assertNotEqual(paramsProto.parameterRanges['sv2'][0], paramsProtoRef.parameterRanges['sv2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['sv2'][1], paramsProtoRef.parameterRanges['sv2'][1])
        self.assertEqual(paramsProto.parameterRanges['sv2'][0], 0.1)
        self.assertEqual(paramsProto.parameterRanges['sv2'][1], 250)
        
        self.assertTrue(paramsProto.gw1)
        self.assertNotEqual(paramsProto.parameterRanges['gw1'][0], paramsProtoRef.parameterRanges['gw1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['gw1'][1], paramsProtoRef.parameterRanges['gw1'][1])
        self.assertEqual(paramsProto.parameterRanges['gw1'][0], 0.005)
        self.assertEqual(paramsProto.parameterRanges['gw1'][1], 0.5)
        
        self.assertTrue(paramsProto.gw2)
        self.assertNotEqual(paramsProto.parameterRanges['gw2'][0], paramsProtoRef.parameterRanges['gw2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['gw2'][1], paramsProtoRef.parameterRanges['gw2'][1])
        self.assertEqual(paramsProto.parameterRanges['gw2'][0], 0.05)
        self.assertEqual(paramsProto.parameterRanges['gw2'][1], 0.75)
        
        self.assertFalse(paramsProto.vgsen1)
        self.assertFalse(paramsProto.vgsen2)
        self.assertFalse(paramsProto.vgsen3)
        self.assertFalse(paramsProto.svalt1)
        self.assertFalse(paramsProto.svalt2)
    
    def testParseCmdProtoCustomRangesAllParams(self):
        cmd_proto = """$rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable"""
        cmd_proto += """ -pre $output_path -s $s1[0.02, 30.0] $s2[0.5, 100.0] $s3[0.2, 20.0] -sv $sv1[0.03, 10.0] $sv2[0.1, 250.0] -vgsen $vgsen1[0.25, 4.0] $vgsen2[0.23, 4.5] $vgsen3[0.6, 5.0] -svalt $svalt1[0.7, 6.0] $svalt2[0.8, 7.0] -gw $gw1[0.005, 0.5] $gw2[0.05, 0.75]"""
        
        paramsProtoRef = CalibrationParametersProto()
        
        paramsProto = CalibrationParametersProto()
        paramsProto.parseParameterString(cmd_proto)
        
        self.assertTrue(paramsProto.s1)
        self.assertNotEqual(paramsProto.parameterRanges['s1'][0], paramsProtoRef.parameterRanges['s1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['s1'][1], paramsProtoRef.parameterRanges['s1'][1])
        self.assertEqual(paramsProto.parameterRanges['s1'][0], 0.02)
        self.assertEqual(paramsProto.parameterRanges['s1'][1], 30)
        
        self.assertTrue(paramsProto.s2)
        self.assertNotEqual(paramsProto.parameterRanges['s2'][0], paramsProtoRef.parameterRanges['s2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['s2'][1], paramsProtoRef.parameterRanges['s2'][1])
        self.assertEqual(paramsProto.parameterRanges['s2'][0], 0.5)
        self.assertEqual(paramsProto.parameterRanges['s2'][1], 100)
        
        self.assertTrue(paramsProto.s3)
        self.assertNotEqual(paramsProto.parameterRanges['s3'][0], paramsProtoRef.parameterRanges['s3'][0])
        self.assertNotEqual(paramsProto.parameterRanges['s3'][1], paramsProtoRef.parameterRanges['s3'][1])
        self.assertEqual(paramsProto.parameterRanges['s3'][0], 0.2)
        self.assertEqual(paramsProto.parameterRanges['s3'][1], 20)
        
        self.assertTrue(paramsProto.sv1)
        self.assertNotEqual(paramsProto.parameterRanges['sv1'][0], paramsProtoRef.parameterRanges['sv1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['sv1'][1], paramsProtoRef.parameterRanges['sv1'][1])
        self.assertEqual(paramsProto.parameterRanges['sv1'][0], 0.03)
        self.assertEqual(paramsProto.parameterRanges['sv1'][1], 10)
        
        self.assertTrue(paramsProto.sv2)
        self.assertNotEqual(paramsProto.parameterRanges['sv2'][0], paramsProtoRef.parameterRanges['sv2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['sv2'][1], paramsProtoRef.parameterRanges['sv2'][1])
        self.assertEqual(paramsProto.parameterRanges['sv2'][0], 0.1)
        self.assertEqual(paramsProto.parameterRanges['sv2'][1], 250)
        
        self.assertTrue(paramsProto.gw1)
        self.assertNotEqual(paramsProto.parameterRanges['gw1'][0], paramsProtoRef.parameterRanges['gw1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['gw1'][1], paramsProtoRef.parameterRanges['gw1'][1])
        self.assertEqual(paramsProto.parameterRanges['gw1'][0], 0.005)
        self.assertEqual(paramsProto.parameterRanges['gw1'][1], 0.5)
        
        self.assertTrue(paramsProto.gw2)
        self.assertNotEqual(paramsProto.parameterRanges['gw2'][0], paramsProtoRef.parameterRanges['gw2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['gw2'][1], paramsProtoRef.parameterRanges['gw2'][1])
        self.assertEqual(paramsProto.parameterRanges['gw2'][0], 0.05)
        self.assertEqual(paramsProto.parameterRanges['gw2'][1], 0.75)
        
        self.assertTrue(paramsProto.vgsen1)
        self.assertNotEqual(paramsProto.parameterRanges['vgsen1'][0], paramsProtoRef.parameterRanges['vgsen1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['vgsen1'][1], paramsProtoRef.parameterRanges['vgsen1'][1])
        self.assertEqual(paramsProto.parameterRanges['vgsen1'][0], 0.25)
        self.assertEqual(paramsProto.parameterRanges['vgsen1'][1], 4)
        
        self.assertTrue(paramsProto.vgsen2)
        self.assertNotEqual(paramsProto.parameterRanges['vgsen2'][0], paramsProtoRef.parameterRanges['vgsen2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['vgsen2'][1], paramsProtoRef.parameterRanges['vgsen2'][1])
        self.assertEqual(paramsProto.parameterRanges['vgsen2'][0], 0.23)
        self.assertEqual(paramsProto.parameterRanges['vgsen2'][1], 4.5)
        
        self.assertTrue(paramsProto.vgsen3)
        self.assertNotEqual(paramsProto.parameterRanges['vgsen3'][0], paramsProtoRef.parameterRanges['vgsen3'][0])
        self.assertNotEqual(paramsProto.parameterRanges['vgsen3'][1], paramsProtoRef.parameterRanges['vgsen3'][1])
        self.assertEqual(paramsProto.parameterRanges['vgsen3'][0], 0.6)
        self.assertEqual(paramsProto.parameterRanges['vgsen3'][1], 5)
        
        self.assertTrue(paramsProto.svalt1)
        self.assertNotEqual(paramsProto.parameterRanges['svalt1'][0], paramsProtoRef.parameterRanges['svalt1'][0])
        self.assertNotEqual(paramsProto.parameterRanges['svalt1'][1], paramsProtoRef.parameterRanges['svalt1'][1])
        self.assertEqual(paramsProto.parameterRanges['svalt1'][0], 0.7)
        self.assertEqual(paramsProto.parameterRanges['svalt1'][1], 6)
        
        self.assertTrue(paramsProto.svalt2)
        self.assertNotEqual(paramsProto.parameterRanges['svalt2'][0], paramsProtoRef.parameterRanges['svalt2'][0])
        self.assertNotEqual(paramsProto.parameterRanges['svalt2'][1], paramsProtoRef.parameterRanges['svalt2'][1])
        self.assertEqual(paramsProto.parameterRanges['svalt2'][0], 0.8)
        self.assertEqual(paramsProto.parameterRanges['svalt2'][1], 7)
    
    def testParseCmdProtoNoRangesDefaultParams(self):
        cmd_proto = """$rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable"""
        cmd_proto += """ -pre $output_path -s $s1 $s2 $s3 -sv $sv1 $sv2 -gw $gw1 $gw2"""
        
        paramsProtoRef = CalibrationParametersProto()
        
        paramsProto = CalibrationParametersProto()
        paramsProto.parseParameterString(cmd_proto)
        
        self.assertTrue(paramsProto.s1)
        self.assertEqual(paramsProto.parameterRanges['s1'][0], paramsProtoRef.parameterRanges['s1'][0])
        self.assertEqual(paramsProto.parameterRanges['s1'][1], paramsProtoRef.parameterRanges['s1'][1])
        
        self.assertTrue(paramsProto.s2)
        self.assertEqual(paramsProto.parameterRanges['s2'][0], paramsProtoRef.parameterRanges['s2'][0])
        self.assertEqual(paramsProto.parameterRanges['s2'][1], paramsProtoRef.parameterRanges['s2'][1])
        
        self.assertTrue(paramsProto.s3)
        self.assertEqual(paramsProto.parameterRanges['s3'][0], paramsProtoRef.parameterRanges['s3'][0])
        self.assertEqual(paramsProto.parameterRanges['s3'][1], paramsProtoRef.parameterRanges['s3'][1])
        
        self.assertTrue(paramsProto.sv1)
        self.assertEqual(paramsProto.parameterRanges['sv1'][0], paramsProtoRef.parameterRanges['sv1'][0])
        self.assertEqual(paramsProto.parameterRanges['sv1'][1], paramsProtoRef.parameterRanges['sv1'][1])
        
        self.assertTrue(paramsProto.sv2)
        self.assertEqual(paramsProto.parameterRanges['sv2'][0], paramsProtoRef.parameterRanges['sv2'][0])
        self.assertEqual(paramsProto.parameterRanges['sv2'][1], paramsProtoRef.parameterRanges['sv2'][1])
        
        self.assertTrue(paramsProto.gw1)
        self.assertEqual(paramsProto.parameterRanges['gw1'][0], paramsProtoRef.parameterRanges['gw1'][0])
        self.assertEqual(paramsProto.parameterRanges['gw1'][1], paramsProtoRef.parameterRanges['gw1'][1])
        
        self.assertTrue(paramsProto.gw2)
        self.assertEqual(paramsProto.parameterRanges['gw2'][0], paramsProtoRef.parameterRanges['gw2'][0])
        self.assertEqual(paramsProto.parameterRanges['gw2'][1], paramsProtoRef.parameterRanges['gw2'][1])
        
        self.assertFalse(paramsProto.vgsen1)
        self.assertFalse(paramsProto.vgsen2)
        self.assertFalse(paramsProto.vgsen3)
        self.assertFalse(paramsProto.svalt1)
        self.assertFalse(paramsProto.svalt2)
    
    def testParseCmdProtoNoRangesAllParams(self):
        cmd_proto = """$rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable"""
        cmd_proto += """ -pre $output_path -s $s1 $s2 $s3 -sv $sv1 $sv2 -vgsen $vgsen1 $vgsen2 $vgsen3 -svalt $svalt1 $svalt2 -gw $gw1 $gw2"""
        
        paramsProtoRef = CalibrationParametersProto()
        
        paramsProto = CalibrationParametersProto()
        paramsProto.parseParameterString(cmd_proto)
        
        self.assertTrue(paramsProto.s1)
        self.assertEqual(paramsProto.parameterRanges['s1'][0], paramsProtoRef.parameterRanges['s1'][0])
        self.assertEqual(paramsProto.parameterRanges['s1'][1], paramsProtoRef.parameterRanges['s1'][1])
        
        self.assertTrue(paramsProto.s2)
        self.assertEqual(paramsProto.parameterRanges['s2'][0], paramsProtoRef.parameterRanges['s2'][0])
        self.assertEqual(paramsProto.parameterRanges['s2'][1], paramsProtoRef.parameterRanges['s2'][1])
        
        self.assertTrue(paramsProto.s3)
        self.assertEqual(paramsProto.parameterRanges['s3'][0], paramsProtoRef.parameterRanges['s3'][0])
        self.assertEqual(paramsProto.parameterRanges['s3'][1], paramsProtoRef.parameterRanges['s3'][1])
        
        self.assertTrue(paramsProto.sv1)
        self.assertEqual(paramsProto.parameterRanges['sv1'][0], paramsProtoRef.parameterRanges['sv1'][0])
        self.assertEqual(paramsProto.parameterRanges['sv1'][1], paramsProtoRef.parameterRanges['sv1'][1])
        
        self.assertTrue(paramsProto.sv2)
        self.assertEqual(paramsProto.parameterRanges['sv2'][0], paramsProtoRef.parameterRanges['sv2'][0])
        self.assertEqual(paramsProto.parameterRanges['sv2'][1], paramsProtoRef.parameterRanges['sv2'][1])
        
        self.assertTrue(paramsProto.gw1)
        self.assertEqual(paramsProto.parameterRanges['gw1'][0], paramsProtoRef.parameterRanges['gw1'][0])
        self.assertEqual(paramsProto.parameterRanges['gw1'][1], paramsProtoRef.parameterRanges['gw1'][1])
        
        self.assertTrue(paramsProto.gw2)
        self.assertEqual(paramsProto.parameterRanges['gw2'][0], paramsProtoRef.parameterRanges['gw2'][0])
        self.assertEqual(paramsProto.parameterRanges['gw2'][1], paramsProtoRef.parameterRanges['gw2'][1])
        
        self.assertTrue(paramsProto.vgsen1)
        self.assertEqual(paramsProto.parameterRanges['vgsen1'][0], paramsProtoRef.parameterRanges['vgsen1'][0])
        self.assertEqual(paramsProto.parameterRanges['vgsen1'][1], paramsProtoRef.parameterRanges['vgsen1'][1])
        
        self.assertTrue(paramsProto.vgsen2)
        self.assertEqual(paramsProto.parameterRanges['vgsen2'][0], paramsProtoRef.parameterRanges['vgsen2'][0])
        self.assertEqual(paramsProto.parameterRanges['vgsen2'][1], paramsProtoRef.parameterRanges['vgsen2'][1])
        
        self.assertTrue(paramsProto.vgsen3)
        self.assertEqual(paramsProto.parameterRanges['vgsen3'][0], paramsProtoRef.parameterRanges['vgsen3'][0])
        self.assertEqual(paramsProto.parameterRanges['vgsen3'][1], paramsProtoRef.parameterRanges['vgsen3'][1])
        
        self.assertTrue(paramsProto.svalt1)
        self.assertEqual(paramsProto.parameterRanges['svalt1'][0], paramsProtoRef.parameterRanges['svalt1'][0])
        self.assertEqual(paramsProto.parameterRanges['svalt1'][1], paramsProtoRef.parameterRanges['svalt1'][1])
        
        self.assertTrue(paramsProto.svalt2)
        self.assertEqual(paramsProto.parameterRanges['svalt1'][0], paramsProtoRef.parameterRanges['svalt1'][0])
        self.assertEqual(paramsProto.parameterRanges['svalt1'][1], paramsProtoRef.parameterRanges['svalt1'][1])
    

    def testPreProcessCmdProto(self):

        file = open(os.path.join(self.basedir, "cmd.proto"))
        cmd_proto = file.read()
        file.close()

        # Get rhessys executable
        (res, filename, pathToRHESSys) = \
            self.clusterCalibrator.getRHESSysExecPath(self.basedir)

        # Get tecfile
        (res, pathToTecfile) = \
            self.clusterCalibrator.getTecfilePath(self.basedir)

#        print "\ncmd_proto: %s" % cmd_proto

        cmd_proto_pre = \
            self.clusterCalibrator.preProcessCmdProto(cmd_proto,
                                                      pathToRHESSys,
                                                      pathToTecfile)

#        print "\ncmd_proto_pre: %s\n" % cmd_proto_pre

        paramsProto = self.clusterCalibrator.parseCmdProtoForParams(cmd_proto)
        self.assertTrue(paramsProto.s1)
        self.assertTrue(paramsProto.s2)
        self.assertFalse(paramsProto.s3)
        self.assertTrue(paramsProto.sv1)
        self.assertTrue(paramsProto.sv2)
        self.assertTrue(paramsProto.gw1)
        self.assertTrue(paramsProto.gw2)
        self.assertFalse(paramsProto.vgsen1)
        self.assertFalse(paramsProto.vgsen2)
        self.assertFalse(paramsProto.vgsen3)

        params = paramsProto.generateParameterValues()
        self.assertNotEqual(params.s1, None)
        self.assertNotEqual(params.s2, None)
        self.assertEqual(params.s3, None)
        self.assertNotEqual(params.sv1, None)
        self.assertNotEqual(params.sv2, None)
        self.assertNotEqual(params.gw1, None)
        self.assertNotEqual(params.gw2, None)
        self.assertEqual(params.vgsen1, None)
        self.assertEqual(params.vgsen2, None)
        self.assertEqual(params.vgsen3, None)

        itr_cmd_proto = self.clusterCalibrator.addParametersToCmdProto(\
            cmd_proto_pre, params)

#        print "\nitr_cmd_proto: %s\n" % itr_cmd_proto

        worldfiles = self.clusterCalibrator.getWorldfiles(self.basedir)

        (res, flowtablePath, surfaceFlowtablePath, worldfilesWithoutFlowTables, worldfilesWithoutSurfaceFlowTables) = \
            self.clusterCalibrator.verifyFlowTables(self.basedir, worldfiles.keys())

        itr = 1
        for worldfile in worldfiles.keys():
            raw_cmd_proto = \
                self.clusterCalibrator.addWorldfileAndFlowtableToCmdProto(\
                itr_cmd_proto, worldfiles[worldfile], flowtablePath[worldfile])

#            print "\nraw_cmd_proto: %s\n" % raw_cmd_proto

            output_path = \
                self.clusterCalibrator.createOutputPath(self.basedir,
                                                          1,
                                                          worldfile,
                                                          itr)
#            print "output path: %s" % output_path
            
            raw_cmd = \
                self.clusterCalibrator.getCmdRawForRun(raw_cmd_proto,
                                                       output_path)
#            print "\nraw_cmd: %s\n" % raw_cmd


    def testLSF_sim(self):
        
        bsub_cmd = "./bin/lsf-sim/bsub.py uname -a"
        # run bsub.py
        bsub_process = Popen(bsub_cmd, shell=True, stdout=PIPE)
        (stdout_str, stderr_str) = bsub_process.communicate()
        print "bsub returned:\n%s" % stdout_str
        
        bsub_regex = re.compile("^Job\s<([0-9]+)>\s.+$")
        match = bsub_regex.match(stdout_str)

        if None == match:
            print "!!!MATCH IS None"

        job_id = match.group(1)
        print "\tjob_id %s" % job_id
        
        bjobs_cmd = "./bin/lsf-sim/bjobs.py -a"
        # run bjobs.py
        bjobs_process = Popen(bjobs_cmd, shell=True, stdout=PIPE)
        (stdout_str, stderr_str) = bjobs_process.communicate()
        print "bjobs returned:\n%s" % stdout_str

        bjobs_regex = re.compile("^([0-9]+)\s+\w+\s+(\w+)\s+.+$")
        for line in string.split(stdout_str, '\n'):
            print line
            match = bjobs_regex.match(line)
            if match != None:
                bsub_job_id = match.group(1)
                bsub_stat = match.group(2)
                print "\tjob_id: %s, status: %s" % (bsub_job_id, bsub_stat)
        
        
        

#if __name__ == "__main__":
#    unittest.main()
