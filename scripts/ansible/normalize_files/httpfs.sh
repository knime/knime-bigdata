#!/bin/bash

# Autodetect JAVA_HOME if not defined
if [ -e /usr/libexec/bigtop-detect-javahome ]; then
  . /usr/libexec/bigtop-detect-javahome
elif [ -e /usr/lib/bigtop-utils/bigtop-detect-javahome ]; then
  . /usr/lib/bigtop-utils/bigtop-detect-javahome
fi

### Added to assist with locating the right configuration directory
export HTTPFS_CONFIG=/etc/hadoop-httpfs/conf

### Remove the original HARD CODED Version reference...  I mean, really???
export HADOOP_HOME=${HADOOP_HOME:-/usr/hdp/current/hadoop-client}
export HADOOP_LIBEXEC_DIR=${HADOOP_HOME}/libexec

exec /usr/hdp/current/hadoop-httpfs/sbin/httpfs.sh.distro "$@"
