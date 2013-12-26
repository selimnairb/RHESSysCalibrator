"""@package rhessyscalibrator.calibration_parameters

@brief Classes that support generating calibration parameters.

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
from random import *

class CalibrationParametersProto(object):
    """ Represents whether particular calibration parameters have been
        specified using boolean values
    """

    parameterRanges = {'s1':[0.01, 20],
                       's2':[1, 150],
                       's3':[0.1, 10],
                       'sv1':[0.01, 20],
                       'sv2':[1, 150],
                       'gw1':[0.001, 0.3],
                       'gw2':[0.01, 0.9],
                       'vgsen1':[0.5, 2],
                       'vgsen2':[0.5, 2], 
                       'vgsen3':[1, 1],
                       'svalt1':[0.5, 2],
                       'svalt2':[0.5, 2]}

    def __init__(self, s_for_sv=False):
        self.s1 = False
        self.s2 = False
        self.s3 = False
        self.sv1 = False
        self.sv2 = False
        self.gw1 = False
        self.gw2 = False
        self.vgsen1 = False
        self.vgsen2 = False
        self.vgsen3 = False
        self.svalt1 = False
        self.svalt2 = False
        self.s_for_sv = s_for_sv

        
    def generateParameterValues(self):
        """ Generate random values for parameters specified as True

            Note: Current implementation only supports the following 
            parameters: s1, s2, s3, sv1, sv2, gw1, gw2, vgsen1, vgsen2, vgsen3, svalt1, svalt2

            @return CalibrationParameters object containing random
            values for each enabled parameter.
        """
        calibrationParameters = \
            CalibrationParameters.newCalibrationParameters()

        if self.s1:
            paramRange = self.parameterRanges['s1']
            calibrationParameters.s1 = uniform(paramRange[0], 
                                               paramRange[1])
        if self.s2:
            paramRange = self.parameterRanges['s2']
            calibrationParameters.s2 = uniform(paramRange[0], 
                                               paramRange[1])

        if self.s3:
            paramRange = self.parameterRanges['s3']
            calibrationParameters.s3 = uniform(paramRange[0], 
                                               paramRange[1])

        if self.sv1:
            if self.s_for_sv:
                assert(calibrationParameters.s1)
                calibrationParameters.sv1 = calibrationParameters.s1
            else:
                paramRange = self.parameterRanges['sv1']
                calibrationParameters.sv1 = uniform(paramRange[0], 
                                                    paramRange[1])

        if self.sv2:
            if self.s_for_sv:
                assert(calibrationParameters.s2)
                calibrationParameters.sv2 = calibrationParameters.s2
            else:
                paramRange = self.parameterRanges['sv2']
                calibrationParameters.sv2 = uniform(paramRange[0], 
                                                    paramRange[1])
            
        if self.gw1:
            paramRange = self.parameterRanges['gw1']
            calibrationParameters.gw1 = uniform(paramRange[0], 
                                                paramRange[1])

        if self.gw2:
            paramRange = self.parameterRanges['gw2']
            calibrationParameters.gw2 = uniform(paramRange[0], 
                                                paramRange[1])
            
        if self.vgsen1:
            paramRange = self.parameterRanges['vgsen1']
            calibrationParameters.vgsen1 = uniform(paramRange[0],
                                                   paramRange[1])
        
        if self.vgsen2:
            paramRange = self.parameterRanges['vgsen2']
            calibrationParameters.vgsen2 = uniform(paramRange[0],
                                                   paramRange[1])
            
        if self.vgsen3:
            paramRange = self.parameterRanges['vgsen3']
            calibrationParameters.vgsen3 = uniform(paramRange[0],
                                                   paramRange[1])
            
        if self.svalt1:
            paramRange = self.parameterRanges['svalt1']
            calibrationParameters.svalt1 = uniform(paramRange[0],
                                                   paramRange[1])
        
        if self.svalt2:
            paramRange = self.parameterRanges['svalt2']
            calibrationParameters.svalt2 = uniform(paramRange[0],
                                                   paramRange[1])
            
        return calibrationParameters


class CalibrationParameters(CalibrationParametersProto):
    """ Represents a set of calibration parameters for a particular run """

    def __init__(self, s1, s2, s3, sv1, sv2, gw1, gw2, 
                 vgsen1, vgsen2, vgsen3, svalt1, svalt2, s_for_sv = False):
        """
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
            @param s_for_sv True to use the same m parameter for
                               for horizontal as for vertical. Defaults to false (0).
        """
        self.s1 = s1
        self.s2 = s2
        self.s3 = s3
        self.sv1 = sv1
        self.sv2 = sv2
        self.gw1 = gw1
        self.gw2 = gw2
        self.vgsen1 = vgsen1
        self.vgsen2 = vgsen2
        self.vgsen3 = vgsen3 
        self.svalt1 = svalt1
        self.svalt2 = svalt2 
        self.s_for_sv = s_for_sv  

    
    def toDict(self):
        """ @return Dict representation of this CalibrationParameters
            object.  (Maybe there is a non-lazy Pythonic way to do this)
        """
        d = dict()
        if self.s1 != None:
            d['s1'] = self.s1
        if self.s2 != None:
            d['s2'] = self.s2
        if self.s3 != None:
            d['s3'] = self.s3
        if self.sv1 != None:
            d['sv1'] = self.sv1
        if self.sv2 != None:
            d['sv2'] = self.sv2
        if self.gw1 != None:
            d['gw1'] = self.gw1
        if self.gw2 != None:
            d['gw2'] = self.gw2
        if self.vgsen1 != None:
            d['vgsen1'] = self.vgsen1
        if self.vgsen2 != None:
            d['vgsen2'] = self.vgsen2
        if self.vgsen3 != None:
            d['vgsen3'] = self.vgsen3
        if self.svalt1 != None:
            d['svalt1'] = self.svalt1
        if self.svalt2 != None:
            d['svalt2'] = self.svalt2
        d['s_for_sv'] = self.s_for_sv
        

        return d

    @classmethod
    def newCalibrationParameters(cls):
        """ Create a new instance of CalibrationParameters with all parameters
            set to None -- i.e. work around Python's fake OOP implementation's
            inability to have more than one constructor

            @return New instance of CalibrationParameters
        """
        obj = cls(None, None, None, None, None, None, None, None, None, None, None, None, None)
        return obj