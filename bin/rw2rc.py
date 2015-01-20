#!/usr/bin/env python

import os
import sys
import argparse
import shutil

if __name__ == "__main__":
    # Handle command line options
    parser = argparse.ArgumentParser(description='Copy RHESSysWorkflows project to RHESSysCalibrator project')
    parser.add_argument('-p', '--projectDir', dest='projectDir', required=True,
                        help='The RHESSysWorkflows project directory to convert to RHESSysCalibrator project')
    parser.add_argument('-b', '--baseDir', dest='baseDir', required=True,
                        help='RHESSysCalibrator base directory to which RHESSysWorkflow project to be copied')
    args = parser.parse_args()

    if not os.access(args.projectDir, os.R_OK):
        sys.exit("Unable to read RHESSysWorkflows project directory {0}".format(args.projectDir))
    src = os.path.abspath(args.projectDir)

    if not os.access(args.baseDir, os.W_OK):
        sys.exit("Unable to write to RHESSysCalibrator project directory {0}".format(args.baseDir))
    dest = os.path.abspath(args.baseDir)

    # Copy RHESSys binary
    print('Copying RHESSys binary')
    srcBin = os.path.join(src, 'rhessys/bin/rhessys5.18.r2')
    destBinDir = os.path.join(dest, 'rhessys/bin')
    shutil.copy(srcBin, destBinDir)

    # Copy climate data
    print('Copying climate data')
    srcClimDir = os.path.join(src, 'rhessys/clim')
    destClimDir = os.path.join(dest, 'rhessys/clim')
    srcClimFiles = os.listdir(srcClimDir)
    for f in srcClimFiles:
        srcClimFile = os.path.join(srcClimDir, f)
        shutil.copy(srcClimFile, destClimDir)

    # Copy parameter definitions
    print('Copying parameter definitions')
    srcDefDir = os.path.join(src, 'rhessys/defs')
    destDefDir = os.path.join(dest, 'rhessys/defs')
    srcDefFiles = os.listdir(srcDefDir)
    for f in srcDefFiles:
        srcDefFile = os.path.join(srcDefDir, f)
        shutil.copy(srcDefFile, destDefDir)

    # Copy flow table
    print('Copying flow table')
    srcFlow = os.path.join(src, 'rhessys/flow/world.flow')
    destFlowDir = os.path.join(dest, 'rhessys/flow/world_flow_table.dat')
    shutil.copy(srcFlow, destFlowDir)

    # Copy src code
    print('Copying source code')
    srcSrcDir = os.path.join(src, 'rhessys/src')
    destSrcDir = os.path.join(dest, 'rhessys/src')
    shutil.rmtree(destSrcDir)
    shutil.copytree(srcSrcDir, destSrcDir)

    # Copy tecfile
    print('Copying TEC file')
    srcTecFile = os.path.join(src, 'rhessys/tecfiles/tec_daily.txt')
    destTecDir = os.path.join(dest, 'rhessys/tecfiles/active')
    shutil.copy(srcTecFile, destTecDir)

    # Copy template
    print('Copying worldfile template')
    srcTemplateFile = os.path.join(src, 'rhessys/templates/template')
    destTemplateDir = os.path.join(dest, 'rhessys/templates')
    shutil.copy(srcTemplateFile, destTemplateDir)

    # Copy worldfile
    print('Copying worldfile')
    destWorldfileDir = os.path.join(dest, 'rhessys/worldfiles/active')
    srcWorldfileHdrFile = os.path.join(src, 'rhessys/worldfiles/world.hdr')
    shutil.copy(srcWorldfileHdrFile, destWorldfileDir)
    srcWorldfileFile = os.path.join(src, 'rhessys/worldfiles/world')
    shutil.copy(srcWorldfileFile, destWorldfileDir)

    print('Done!')

