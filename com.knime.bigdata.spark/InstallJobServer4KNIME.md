# Installation Instructions 
This document describes how to install the Spark Job-Server on a Linux server. KNIME utilizes the Job-Server for some of its Big Data components - primarily for the Spark integration.


## Background
The spark-jobserver provides a RESTful interface for submitting and managing [Apache Spark](http://spark-project.org) jobs, jars, and job contexts. 
Please check out the [spark-jobserver GIT hub repository](https://github.com/spark-jobserver/spark-jobserver) for further information, including licensing conditions, contributors and mailing lists.

## Version
The packaged version provided by KNIME and described in this document is based on Version 0.5.0 of the Job-Server. It has been deployed and tested with Spark Versions 1.2.1 and 1.2.2 and Hadoop Version 2.6.0.

## Installation

The Job-Server must be installed a on (Linux) server that is co-located in the same network as your Hadoop / Spark installation. It can be installed on the Hadoop master server or any other server that has unrestricted access to the Hadoop installation. 

0. Create some directory for the Job-Server installation. For example /usr/local/jobserver.
1. Download and unpack the pre-packaged zip file (TODO - TOBIAS provide link) into your installation directory. 
2. Edit `environment.sh` as appropriate. The most important settings are:    
  2.1 `master` use `master = "yarn-client"` (default) when running Yarn or `master = "spark://localhost:7077"` (or similar) when running in stand-alone mode. 
  2.2 `rootdir` use `rootdir = /tmp/jobserver` (default) or some other temporary directory. This is where job information and jar files are kept (see also 'Hints' below).
  Please note that some settings are overwritten by the KNIME configuration. Examples of overwritten settings are `num-cpu-cores` and `memory-per-node`.
3. Edit `settings.sh` as appropriate.    
  3.1 `INSTALL_DIR` this should point to the installation directory (but is not used)     
  3.2 `SPARK_HOME`, please change if Spark is not installed under `/usr/local/spark`     
  3.3 `LOG_DIR`, set to some log directory     
4. Edit `log4j-server.properties` as appropriate.   
   
## Starting the Job-Server
On the remote server, start the Job-Server in the installation directory with `server_start.sh` 

## Stopping the Job-Server
Stop the Job-Server with `server_stop.sh`

## Hints
Point your browser to `http://<server>:8090` to check out the status of the Job-Server. Three different tabs provide information about active and completed jobs, contexts and jars.

The Job-Server does currently not support user credentials. This means that anybody who has access to the server where the Job-Server is running can start and stop contexts and jobs. 

It might be advisable to re-start the Job-Server every once in a while and to clean-up the `rootdir`. Either remove the entire directory or only the jar files under `rootdir`/filedao/data


