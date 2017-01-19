#!/bin/bash

#
# Takes the iris dataset as first argument, uploads it to HDFS and imports it as a Hive
# table.
#

DIR=$(mktemp -d)

cp $1 $DIR/

chmod go+x $DIR
chmod -R go+r $DIR

AS_HDFS="sudo su -l hdfs -c"
AS_HIVE="sudo su -l hive -c"

$AS_HDFS "hdfs dfs -mkdir /tmp/iris-dataset"

for f in $DIR/* ; do
	echo "Uploading $f"
	$AS_HDFS "hdfs dfs -put $f /tmp/iris-dataset/"
done

#
# Import Hive table
#

cat >$DIR/import.hivesql <<- EOM
CREATE EXTERNAL TABLE iris (sepal_length DOUBLE, sepal_width DOUBLE, petal_length DOUBLE, petal_width DOUBLE, class STRING)
 COMMENT 'Iris table'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '32'
 STORED AS TEXTFILE
 LOCATION '/tmp/iris-dataset/';
EOM
chmod -R go+r $DIR
$AS_HIVE "hive --service cli -f $DIR/import.hivesql"

rm -R $DIR
