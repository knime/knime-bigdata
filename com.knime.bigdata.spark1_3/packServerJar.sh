#!/bin/bash

cp lib/job-server-knime-utils*.jar resources/knimeJobs.jar
cd bin

jar uvf ../resources/knimeJobs.jar com/knime/bigdata/spark/jobserver/jobs/*.class com/knime/bigdata/spark/jobserver/server/*.class com/knime/bigdata/spark/jobserver/server/transformation/*.class

cd ..
