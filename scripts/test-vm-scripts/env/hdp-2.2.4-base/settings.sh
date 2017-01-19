VM_NAME="hdp-2-2-4"
JOB_SERVER_VARIANT="${SJS_VERSION}_hdp-2.2.4"
JOB_SERVER_URL="http://$HOSTNAME:8090"
TESTS="(SparkExecutor|BigDataConnectors)/(spark_1_2|spark_all|Hive|HDFS)/.+"
SERVICES="HDFS YARN Zookeeper Hive_Metastore WebHCat" # Ranger missing in start makefile
START_RANGER="true"
