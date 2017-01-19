#!/bin/bash

#
# Downloads the energy dataset from S3, uploads it to HDFS and imports it as a Hive
# table.
#

DIR=$(mktemp -d)

wget -O $DIR/cdrs.txt https://s3-eu-west-1.amazonaws.com/dataset-cdr/cdrs.txt
wget -O $DIR/recharges.txt https://s3-eu-west-1.amazonaws.com/dataset-cdr/recharges.txt

chmod go+x $DIR
chmod -R go+r $DIR

AS_HDFS="sudo su -l hdfs -c"
# AS_HIVE="sudo su -l hive -c"

$AS_HDFS "hdfs dfs -mkdir -p /demo/data/CDR"

for f in $DIR/* ; do
	echo "Uploading $f"
	$AS_HDFS "hdfs dfs -put $f /demo/data/CDR/"
done

rm -R $DIR
