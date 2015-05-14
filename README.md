**RHESSysCalibrator**			{#index}

**Introduction**

RHESSysCalibrator is a system for managing calibration sessions and run of
RHESSys; A calibratration session contains one or more runs.  Each run represents a
distinct and uniformly random set of sensitiviy parameters.  
RHESSysCalibrator uses a database (in sqlite3 format) to keep track of each 
session and run.  RHESSysCalibrator handles launching and management of each
run, and will update its database as run jobs are submitted, begin
running, and finish (either with or without error).  RHESSysCalibrator supports parallel execution of model runs using: (1) multiple processes (i.e. on your laptop or workstation); (2) compute clusters running Load Sharing Facility (LSF); and (3) compute clusters running PBS/TORQUE. 

In addition to creating new calibration sessions, it is also possible
to re-start sessions that have exited before completion. RHESSysCalibrator also handles calibration post processing (i.e. calculating Nash-Sutcliffe efficiency for modeled vs. observed streamflow), as well as uncertainty estimation using Generalized Likelihood Uncertainty Estimation (GLUE; Beven & Binley 1992).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Authors](#authors)
- [Source code](#source-code)
- [Installation instructions](#installation-instructions)
- [RHESSys calibration background](#rhessys-calibration-background)
- [Usage instructions](#usage-instructions)
- [Setup a new calibrator project](#setup-a-new-calibrator-project)
- [Configuring a calibration session](#configuring-a-calibration-session)
- [Create a new calibration session](#create-a-new-calibration-session)
  - [Using screen to run RHESSysCalibrator on compute clusters](#using-screen-to-run-rhessyscalibrator-on-compute-clusters)
- [Restarting failed model sessions](#restarting-failed-model-sessions)
- [Calculate model fitness statistics for basin-level output](#calculate-model-fitness-statistics-for-basin-level-output)
- [Performing GLUE uncertainty estimation](#performing-glue-uncertainty-estimation)
  - [Applying behavioral parameter sets to another RHESSys model](#applying-behavioral-parameter-sets-to-another-rhessys-model)
  - [Visualizing behavioral model output](#visualizing-behavioral-model-output)
  - [Comparing behavioral simulations](#comparing-behavioral-simulations)
  - [Visualizing behavioral model output using other tools](#visualizing-behavioral-model-output-using-other-tools)
- [Appendix](#appendix)
  - [Model directory structure](#model-directory-structure)
  - [References](#references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

Authors
-------
Brian Miles - brian_miles@unc.edu

Taehee Hwang - taehee@indiana.edu

Lawrence E. Band - lband@email.unc.edu

For questions or support contact [Brian Miles](brian_miles@unc.edu)


Source code
-----------
Source code can be found at: https://github.com/selimnairb/RHESSysCarlibrator

Documentation can be found at: http://pythonhosted.org/rhessyscalibrator


Installation instructions
-------------------------
RHESSysCalibrator requires [RHESSysWorkflows](https://github.com/selimnairb/RHESSysWorkflows) and [EcohydroLib](https://github.com/selimnairb/EcohydroLib); detailed
installation instructions for OS X and Linux can be found [here](https://github.com/selimnairb/RHESSysWorkflows).

> Though RHESSysCalibrator is written in Python it has only been tested under OS X and 
> Linux.  

Once you have installed RHESSysWorkflows and EcohydroLib using the instructions above, install RHESSysCarlibrator using pip:

	sudo pip install rhessyscalibrator
	
> Note that when installing RHESSysCalibrator and its dependencies on a compute cluster, 
> you will likely first need to setup a virtual Python environment using *virtualenv*.

RHESSys calibration background
------------------------------
A discussion of model calibration theory is beyond the scope of this manual.  Basic RHESSys model calibration is typically performed by calibrating the model's soil parameters against basin-level streamflow using a Monte Carlo approach.  These soil parameters as well as general calibration strategies are described in detail [here](https://github.com/RHESSys/RHESSys/wiki/Calibrating-and-running-RHESSys).

Usage instructions
------------------

## Setup a new calibrator project
The first step to using RHESSysCalibrator is to create empty 
RHESSysCalibrator project.  First create a new directory:

    mkdir MY_CALIBRATION_PROJECT
    
Then, populate this directory with the [correct subdirectory structure](#model-directory-structure):

    rhessys_calibrator.py -b Baisman_10m_calib --create

Next, if the RHESSys model you wish to calibrate was created using RHESSysWorkflows, you can use the provided *rw2rc.py* script to copy your RHESSysWorkflows project into the empty RHESSysCalibrator project:

    rw2rc.py -p MY_RHESSYSWORKFLOWS_PROJECT -b MY_CALIBRATION_PROJECT
    
If you are not working from a RHESSysWorkflows project, you will need to copy the necessary model files into the [correct locations](#model-directory-structure) in your calibration directory.
    
Lastly, copy observed daily streamflow and precipitation data for your watershed into the *obs* directory of your RHESSysCalibrator project, for example:

    cp PATH/TO/MY_OBSERVED_DATA MY_CALIBRATION_PROJECT/obs
    
Note that format for observered data must be a CSV file including the following columnsl:

    datetime,streamflow_mm,precip_mm
    1/1/1991,3.7,0
    1/2/1991,3.5,0
    1/3/1991,3.51,8.4
    1/4/1991,3.32,0.5
    1/5/1991,3.21,0
    ...
    
The datetime column must contain the data in MM/DD/YYYY format.  Streamflow and precipitation data must be in units of mm/day; streamflow must be in a column named "streamflow_mm", precipitation in a column named "precip_mm".  Make sure your line endings are Unix, not Windows or Mac. 
    
## Configuring a calibration session
RHESSysCalibrator uses a file called *cmd.proto* to control how calibration runs are created.  The typical contents of this file is:

    $rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable -pre $output_path -s $s1[0.01, 20.0] $s2[1.0, 150.0] $s3[0.1, 10.0] -sv $sv1[0.01, 20.0] $sv2[1.0, 150.0] -gw $gw1[0.001, 0.3] $gw2[0.01, 0.9]

or for older versions of RHESSysCalibrator (or if you used the *--noparamranges* option when creating your project), the ranges for each parameter will not be included:
 
    $rhessys -st 2003 1 1 1 -ed 2008 10 1 1 -b -t $tecfile -w $worldfile -r $flowtable -pre $output_path -s $s1 $s2 $s3 -sv $sv1 $sv2 -gw $gw1 $gw2

> Use the *--allparams* option when creating your project to include all possible RHESSys commmand line parameters
> in the project's *cmd.proto*.

Here we are setting the start and end dates of the calibration runs, as well as setting the soil parameters to calibrated against.  In this case for each model run the following parameters will be varied: decay of saturated hydraulic conductivity for soil drainage (s1 or m in model parlance); saturated hydraulic conductivity for soil drainage (s2 or Ksat0); soil depth (s3); decay of saturated hydraulic conductivity for infiltration (sv1 or m_v); vertical saturated conductivity for infilration (sv2 or Ksat0_v); bypass flow from detention storage directly to hillslope groundwater (gw1); loss from the saturated zone to hillslope groundwater (gw2).

In general, you should only need to change the start and end dates, which calibration parameters to include, and
the range of the uniform distribution from which each parameter will be sampled; parameter ranges are specified by placing the desired closed interval (i.e. two floating point numbers separated by commas and enclosed by square brackets) immediately following the parameter name.  Ignore all other parts of cmd.proto (e.g. $worldfile, $output_path) as these are used by RHESSysCalibrator to create the RHESSys command line for each model run created as part of the calibration session.
    
When running on compute clusters, we recommend that you also make a test cmd.proto for debugging your model:

    cp MY_CALIBRATION_PROJECT/cmd.proto MY_CALIBRATION_PROJECT/cmd.proto.test
    
For example edit cmd.proto.test to run the model for only a single month:

	$rhessys -st 2007 1 1 1 -ed 2007 2 1 1 -b -t $tecfile -w $worldfile -r $flowtable -pre $output_path $s1[0.01, 20.0] $s2[1.0, 150.0] $s3[0.1, 10.0] -sv $sv1[0.01, 20.0] $sv2[1.0, 150.0] -gw $gw1[0.001, 0.3] $gw2[0.01, 0.9]
	
Now save a copy of cmd.proto so that we don't overwrite it when testing our model:

    cp MY_CALIBRATION_PROJECT/cmd.proto MY_CALIBRATION_PROJECT/cmd.proto.run
    
Before running a debug calibration session, copy cmd.proto.test to cmd.proto:

	cp MY_CALIBRATION_PROJECT/cmd.proto.test MY_CALIBRATION_PROJECT/cmd.proto
	
## Create a new calibration session
To run RHESSysCalibrator, we use the *rhessys_calibrator* command:

	rhessys_calibrator.py -b MY_CALIBRATION_PROJECT -p 'My RHESSys model' -n 'Debug calibration session, one iteration' -i 1 -j 1 --parallel_mode process
	
The *-b* option specifies the RHESSyCalibrator project directory to use.  We describe the project with the *-p* option, and provide notes for this particular calibration session with the *-n* option.  As this is a test calibration session, we specify that we only want to run one iteration using the *-i* option, and that we only want to run at most one model run at a given time using the *-j*.  Lastly, we set the parallel mode to *process*, which is appropriate for running on a laptop or workstation.  To run the calibration session on a compute cluster running LSF change the command line as follows:

    rhessys_calibrator.py -b MY_CALIBRATION_PROJECT -p 'My RHESSys model' -n 'Debug calibration session, one iteration' -i 1 -j 1 --parallel_mode lsf --mem_limit N -q QUEUE_NAME
    
Where *N* is the amount of memory, in gigabytes (GB) that your model needs, and *QUEUE_NAME* is the name of the LSF queue to which jobs for this session should
be submitted.  Note that the queue name will be specific to your compute cluster (ask for administrator for details).  

Or to run the session on a compute cluster running PBS/TORQUE:

	rhessys_calibrator.py -b MY_CALIBRATION_PROJECT -p 'My RHESSys model' -n 'Debug calibration session, one iteration' -i 1 -j 1 --parallel_mode pbs --mem_limit N --wall_time M
	
Where *M* is the amount of hours you estimate that your model will need to complete. You may also optionally specify the *-q* option when running under PBS.

For details about these and other options, run *rhessys_calibrator* with the *--help* option:

    rhessys_calibrator.py --help

> While running, RHESSysCalibrator will print lots of ugly though usually informative 
> messages to the screen to tell you want it is doing.

Once you have successfully run your calibration project for a single iteration using your test *cmd.proto*, you can launch a real calibration session by first replacing the test *cmd.proto* with the real *cmd.proto*:

	cp MY_CALIBRATION_PROJECT/cmd.proto.run MY_CALIBRATION_PROJECT/cmd.proto

Then, you can launch a calibration session as follows.  For multiprocessor mode (i.e. on a laptop or workstation computer):

    rhessys_calibrator.py -b MY_CALIBRATION_PROJECT -p 'My RHESSys model' -n 'Debug calibration session, one iteration' -i 500 -j 8 --parallel_mode process
    
Where *-i* specifcies that this session should consist of 500 iterations or model realizations, with *-j* controlling the maximum number of iterations or simultaneous jobs (8 in this case) to run in parallel.  The number of simultaneous jobs possible in multiprocessor mode will depend on the number of processors/cores/virtual cores in your computer, as well as the amount of memory and the amount of memory your RHESSys model requires (which depends on the spatial extent and resolution of your model domain).

> You can see how much memory your model requires to run by using your computer's 
> Activity Monitor tool while the model is running.

When running on compute clusters, you will typically be able to running many more simultaneous jobs in parallel.  For example on LSF-based clusters:

    rhessys_calibrator.py -b MY_CALIBRATION_PROJECT -p 'My RHESSys model' -n 'Debug calibration session, one iteration' -i 5000 -j 1000 --parallel_mode lsf --mem_limit N -q QUEUE_NAME
    
or for PBS/TORQUE-based clusters:

    rhessys_calibrator.py -b MY_CALIBRATION_PROJECT -p 'My RHESSys model' -n 'Debug calibration session, one iteration' -i 5000 -j 1000 --parallel_mode pbs --mem_limit N --wall_time M
    
Here we are telling RHESSysCalibrator to run 5,000 model iterations in this session with at most 1,000 simultaneous jobs.  Note that the number of simultaneous jobs possible will depend on the size of the compute cluster (e.g. number of cores) as well as administrative policies.  For example, some systems restrict users to using at most a few hundred compute cores at any one time and may impose aggregate memory limits across all of your jobs, for example a few terabytes (TB).  Consult the documentation for your cluster before trying to run more than a few simultaneous jobs using RHESSysCalibrator.

### Using screen to run RHESSysCalibrator on compute clusters
When running calibration runs on compute clusters, we recommend running 
*rhessys_calibrator* within a *screen* session.  *Screen* is a tool that allows you to 
run commands in a terminal that will not exit if your connection to the compute
cluster is lost.  Assuming *screen* is installed on your cluters (if it is not, ask your administrator to install it), you would launch screen before running any RHESSysCalibrator commands:

    screen
    
This will cause screen to launch and to run a normal login shell, from which you can run RHESSysCalibrator commands.  To detach from your screen session hit Control-A then Control-D on your keyboard.  To re-attached to a detached screen session (whether you detached or we forceably disconnected) specify then *-r* option when running screen:

    screen -r

## Restarting failed model sessions
On occasion, the *rhessys_calibrator* session may be forceably stopped before all calibration runs have finished (you may even decide to quit a session yourself).  You can use the *rhessys_calibrator_restart* command to restart such a session, for LSF-based clusters:

    rhessys_calibrator_restart.py -b MY_CALIBRATION_PROJECT -s N -i 5000 -j 1000 --parallel_mode lsf --mem_limit M -q QUEUE_NAME
    
or for PBS/TORQUE-based clusters:

    rhessys_calibrator_restart.py -b MY_CALIBRATION_PROJECT -s N -i 5000 -j 1000 --parallel_mode pbs --mem_limit M --wall_time W  
    
Where the *-s* option is used to specify the ID of the session that you would like to restart.  Will print how many runs have completed, how many will be restarted, and how many new runs will be started, before asking you if you wish to continue.

## Calculate model fitness statistics for basin-level output
After the calibration session finishes (i.e. once all the model runs have completed), you can use *rhessys_calibrator_postprocess* to calculate model fitness parameters (e.g. Nash-Sutcliffe Efficiency for daily streamflow and daily log(streamflow)):

    rhessys_calibrator_postprocess.py -b MY_CALIBRATION_PROJECT -f MY_OBSERVED_DATA -s 2 --enddate 2007 2 1 1 --figureX 8 --figureY 6
    
The *-s* option specifies the calibration session for which we wish to calculate fitness parameters; typically session is *2* is our first realy calibration session as the first session was our testing session.  Here we also specify the end date of the time period over which we wish to calculate fitness parameters; this is done using the *--enddate* option.  Note that you can also specify the temporal aggregation with which to calculate fitness parameters using the *-p* (a.k.a. *--period*) option.  The default is 'daily', but 'weekly' and 'monthly' are also supported.  The *--figureX* and *--figureY* options control the X and Y dimensions (in inches) of output plots.

Once post-processing is finished, the following message will be printed:

    Fitness results saved to post-process session: N
    
where "N" is the number of the post-process session just created for your calibration session; remember this number.  The sensitivity of each parameter will be illustrated in "dotty plot" figure output as PDF file named *dotty_plots_SESSION_2_POSTPROCESS_1_daily.pdf* stored in the calibration project directory.
    
You can see the parameters used for each calibration run, as well as the fitness values for each run, by opening the calibration SQLite database stored in the calibration project.  We recommend that you use the SQLite Manager add-on in the FireFox web browser to do so, though you can use any tool that can read SQLite version 3 databases.  The calibration database for our project can be found here:

    MY_CALIBRATION_PROJECT/db/calibration.sqlite
    
> If you are working with a database originally created in RHESSysCalibrator 1.0, the filename will be *calibration.db* instead.
    
Using SQLite Manager, you can view calibration parameters and model fitness statistics, determine the model command line and output location for each model run.  The name and purpos of the most important tables are as follows:

Table | Data stored
--- | --- 
session | General information, one entry each time *rhessys_calibrator* or *rhessys_calibrator_behavioral* is run.
run | Detailed information about each model run in a session, multiple runs are associated with each session.
postprocess | General post-process information, one entry for each time *rhessys_calibrator_postprocess* or *rhessys_calibrator_behavioral* is run.
runfitness | Detailed run fitness information for a given model run, multiple runfitness entries are associated with each postprocess session. 

To export model run information to CSV files suitable for importing into data analysis tools, you can use the *rhessys_calibrator_postprocess_export* tool:

    rhessys_calibrator_postprocess_export.py -b MY_CALIBRATION_PROJECT -s N -f mypreferred_filename.csv

where "N" is the number of the post-process session output by *rhessys_calibrator_postprocess*.

## Performing GLUE uncertainty estimation
Once you have a suite of model realizations from a calibration session, RHESSysCalibrator can also facilitate simple uncertainty analysis using the Generalized Likelihood Uncertainty Estimation methodology (GLUE; Beven & Binley 1992).  

### Applying behavioral parameter sets to another RHESSys model
The *rhessys_calibrator_behavioral* command will allow you to apply so-called behavioral model parameters from a previous calibration session to a new behavioral session representing a particular model scenario (e.g. a change in land cover from forest to suburban development; climate change scenarios, etc.). 

The following invocation of *rhessys_calibrator_behavioral* will apply the top 100 model realizations, sorted in descending order by NSE-log then NSE, from post-process session 2 to a new behavioral RHESSysCalibrator project (make sure to copy the calibration.sqlite from the calibration project into the behavioral project), for LSF-based clusters:

    rhessys_calibrator_behavioral.py -b MY_BEHAVIORAL_CALIBRATION_PROJECT -p 'Behavioral runs for My RHESSys model' -s 2 -c cmd.proto -j 100 --parallel_mode lsf --mem_limit M -q QUEUE_NAME -f "postprocess_id=2 order by nse_log desc, nse desc limit 100"
    
or for PBS/TORQUE-based clusters:

    rhessys_calibrator_behavioral.py -b MY_BEHAVIORAL_CALIBRATION_PROJECT -p 'Behavioral runs for My RHESSys model' -s 2 -c cmd.proto -j 100 --parallel_mode pbs --mem_limit M --wall_time W -f "postprocess_id=2 order by nse_log desc, nse desc limit 100"
    
Note that to allow the time period for uncertainty estimation of behavioral simulations to differ from that of the calibration session, we can specify a particular *cmd.proto* to use, which may or may not be the same as the *cmd.proto* used for the calibration session.

> The *-f* option can be any valid SQLite "WHERE" clause.

When the behavioral runs complete *rhessys_calibrator_behavioral* will print out the number of the calibration session and post-process session created, e.g.:

    Behavioral results saved to session 3, post-process session 2

Take note of these for future reference in the visualization section below.

### Visualizing behavioral model output
Once the behavioral simulations are complete, you can visualize the uncertainty around estimates of streamflow using the *rhessys_calibrator_postprocess_behavioral* command:

    rhessys_calibrator_postprocess_behavioral.py -b MY_BEHAVIORAL_CALIBRATION_PROJECT -s 2 -of PDF --figureX 8 --figureY 3 --supressObs --plotWeightedMean

For a full list of options, run:

    rhessys_calibrator_postprocess_behavioral.py --help

*rhessys_calibrator_postprocess_behavioral* will output: (1) the number of modeled observations; (2) the number of observerations within the 95% uncertainty bounds; and (3) the Average Relative Interval Length (ARIL; Jin et al. 2010); ARIL represents the width of the prediction interval.  Better models are those that have a smaller ARIL and a higher number of observed streamflow values falling within the prediction interval, relative to a worse model.  

### Comparing behavioral simulations
You can use the *rhessys_calibrator_postprocess_behavioral_compare* command to compare two sets of behavioral runs:
    
    rhessys_calibrator_postprocess_behavioral_compare.py MY_BEHAVIORAL_CALIBRATION_PROJECT_1 MY_BEHAVIORAL_CALIBRATION_PROJECT_2 3 4 -t "Model scenario 1 vs. scenario 2 - 95% uncertainty bounds" -of PDF --figureX 8 --figureY 3 --plotWeightedMean --legend "Scenario 1" "Scenario 2" --behavioral_filter "nse>0 order by nse_log desc, nse desc limit 100"  --supressObs --color	
	
Here we are telling RHESSysCalibrator that we would like to compare post-process sessions 3 and 4, plotting the weighted ensemble mean (Seibert & Beven 2009) with the *--plotWeightedMean* option. Note that *MY_BEHAVIORAL_CALIBRATION_PROJECT_1* and *MY_BEHAVIORAL_CALIBRATION_PROJECT_2* could be the same RHESSysCalibrator project.  

*rhessys_calibrator_postprocess_behavioral_compare* will perform a Kolmogorov-Smirnov (K-S) test to determine if the weighted ensemble mean streamflow time series from the behavioral runs are statistically significant.  Here is some example output:

```
Critical value for Kolmogorov-Smirnov statistic (D_alpha; alpha=0.0500): 0.0712
Kolmogorov-Smirnov statistic (D): 0.0205
D > D_alpha? False

p-value: 0.9977
p-value < alpha? False
```

Which indicates that there is no statistically significant difference between the weighted ensemble mean streamflow time series of the two behavioral model scenarios.
	
### Visualizing behavioral model output using other tools
If you would like to visualize behavioral streamflow data using other analysis or visualization tools, you can use the *rhessys_calibrator_postprocess_behavioral_timeseries* command to out output: min, max, median, mean, and weighted ensemble mean time series:

	rhessys_calibrator_postprocess_behavioral_timeseries.py -b MY_BEHAVIORAL_CALIBRATION_PROJECT -s 3

where the *-s* or *--postprocess_session* option refers to the post-process session created for your behavioral run by *rhessys_calibrator_behavioral*.
	
Then, for example, you can use RHESSysWorkflow's *RHESSysPlot* command to make scatter plots of streamflow for two behavioral sessions:

	RHESSysPlot.py -p scatter -o MY_BEHAVIORAL_CALIBRATION_PROJECT/obs/MY_OBSERVED_DATA -b MY_BEHAVIORAL_CALIBRATION_PROJECT/behavioral_ts_SESSION_3_weighted_ensmb_mean.csv MY_BEHAVIORAL_CALIBRATION_PROJECT/behavioral_ts_SESSION_4_weighted_ensmb_mean.csv -c streamflow -t "Weighted ensemble mean daily streamflow: Scenario 1 v. Scenario 2" -l "Streamflow - Scenario 1 (mm/day)" "Streamflow - Scenario 2 (mm/day)" --supressObs -f scenario1_v_scenario2 --figureX 8 --figureY 6
	
See:

    RHESSysPlot.py --help 

for a complete description of possible options.
	
## Appendix  

### Model directory structure
Before running rhessys_calibrator.py to perform calibrations, it is first 
necessary to create the session directory structure.  This is done by
issuing the --create argument, along with the always-required
--basedir argument.  This will create the following directory
structure within $BASEDIR:

```
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
```

### References
Beven, K. & Binley, A., 1992. The future of distributed models: Model calibration and uncertainty prediction. Hydrological Processes, 6(3), pp.279–298.

Jin, X., Xu, C.-Y., Zhang, Q., & Singh, V. P., 2010. Parameter and modeling uncertainty simulated by GLUE and a formal Bayesian method for a conceptual hydrological model. Journal of Hydrology, 383(3-4), pp.147–155.

Seibert, J. & Beven, K J, 2009. Gauging the ungauged basin: how many discharge measurements are needed? Hydrology and Earth System Sciences.


