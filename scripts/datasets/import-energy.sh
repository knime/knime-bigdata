#!/bin/bash

#
# Downloads the energy dataset from S3, uploads it to HDFS and imports it as a Hive
# table.
#

DIR=$(mktemp -d)

for i in {1..6} ; do
	wget -O $DIR/File${i}.txt https://s3-eu-west-1.amazonaws.com/dataset-energy/File${i}.txt
done

chmod go+x $DIR
chmod -R go+r $DIR

AS_HDFS="sudo su -l hdfs -c"
AS_HIVE="sudo su -l hive -c"

$AS_HDFS "hdfs dfs -mkdir /tmp/energy-dataset"

for f in $DIR/* ; do
	echo "Uploading $f"
	$AS_HDFS "hdfs dfs -put $f /tmp/energy-dataset/"
done

#
# Import Hive table
#

cat >$DIR/import.hivesql <<- EOM
CREATE EXTERNAL TABLE energy (meter_id INT, date_time STRING, kw_30 DOUBLE)
 COMMENT 'Energy meter table'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '32'
 STORED AS TEXTFILE
 LOCATION '/tmp/energy-dataset/';
EOM
chmod -R go+r $DIR
$AS_HIVE "hive --service cli -f $DIR/import.hivesql"

rm -R $DIR
