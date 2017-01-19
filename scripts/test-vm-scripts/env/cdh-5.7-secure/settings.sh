VM_NAME="cdh-5-7-0"
JOB_SERVER_VARIANT="${SJS_VERSION}_cdh-5.7"
JOB_SERVER_URL="https://$HOSTNAME:8090"
TESTS="(SparkExecutor|BigDataConnectors)/(spark_auth|HDFS_SSL)/.+"
SERVICES="hadoop-hdfs-datanode hadoop-hdfs-journalnode hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-httpfs"
START_IMPALA="false"
