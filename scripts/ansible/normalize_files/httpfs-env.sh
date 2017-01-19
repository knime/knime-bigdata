#!/bin/bash

# Add these to control and set the Catalina directories for starting and finding the httpfs application
export CATALINA_BASE=/usr/hdp/current/hadoop-httpfs
export HTTPFS_CATALINA_HOME=/etc/hadoop-httpfs/tomcat-deployment
 
# Set a log directory that matches your standards
export HTTPFS_LOG=/var/log/hadoop-httpfs
 
# Set a tmp directory for httpfs to store interim files
export HTTPFS_TEMP=/tmp/httpfs
