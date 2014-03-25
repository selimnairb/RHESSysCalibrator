from setuptools import setup

def readme():
    with open('README.txt') as f:
        return f.read()

setup(name='rhessyscalibrator',
      version='1.0.22',
      description='Libraries and command-line scripts for handling RHESSys model calibration.',
      long_description=readme(),
      classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Topic :: Scientific/Engineering :: GIS'        
      ],
      url='https://github.com/selimnairb/RHESSysCalibrator',
      author='Brian Miles',
      author_email='brian_miles@unc.edu',
      license='BSD',
      packages=['rhessyscalibrator', 'rhessyscalibrator.tests'],
      install_requires=['numpy>=1.7',
                        'matplotlib>=1.1',
                        'pandas',
      ],
      scripts=['bin/lsf-sim/bjobs.py',
               'bin/lsf-sim/bsub.py',
               'bin/rhessys_calibrator_behavioral.py',
               'bin/rhessys_calibrator_postprocess_behavioral.py',
               'bin/rhessys_calibrator_postprocess_behavioral_compare.py',
               'bin/rhessys_calibrator.py',
               'bin/rhessys_calibrator_postprocess.py',
               'bin/rhessys_calibrator_results.py'
      ],
      zip_safe=False)
