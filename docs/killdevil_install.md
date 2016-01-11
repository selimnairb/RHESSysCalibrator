
# Installing on KillDevil

# Setup a virtual python environment
Virtualenv is not available, so download:

    http://peak.telecommunity.com/dist/virtual-python.py

Then:

    mkdir env
    python virtual-python.py --prefix env

Next exit ~/.profile, adding ~/env/bin to PATH, as follows:

    PATH=$HOME/env/bin:$PATH; export PATH

Then, to enable your virtual python environment:

    source ~/.profile

You'll need to do this last part every time you log in.

# Install RHESSysCalibrator

Download RHESSysCalibrator from GitHub:

    git clone https://github.com/selimnairb/RHESSysCalibrator.git
    cd RHESSysCalibrator

Edit *install_requires* in setup.py to remove dependencies not needed on the cluster.  Initial 
value:

```
install_requires=['numpy>=1.7',
                        'scipy',
                        'matplotlib>=1.1',
                        'pandas',
                        'rhessysworkflows>=1.21'
      ],
```

Commented out value:

```
      install_requires=['numpy>=1.7',
                        'scipy',
#                        'matplotlib>=1.1',
#                        'pandas',
                        'rhessysworkflows>=1.21'
      ],
```
    
Install:

    python setup.py install
    cd ..

