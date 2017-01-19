VM_NAME="cdh-5-8-0"
JOB_SERVER_VARIANT="${SJS_VERSION}_cdh-5.8"
JOB_SERVER_URL="http://$HOSTNAME:8090"
TESTS="(SparkExecutor|BigDataConnectors)/(spark_1_6|spark_all|Hive|Impala|HDFS)/.+"
SERVICES="mysqld zookeeper-server \
    hadoop-hdfs-datanode hadoop-hdfs-journalnode hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-httpfs \
    hadoop-yarn-nodemanager hadoop-yarn-resourcemanager \
    hive-metastore hive-server2"
START_IMPALA="true"
