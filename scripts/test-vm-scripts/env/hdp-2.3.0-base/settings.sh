VM_NAME="hdp-2-3-0"
JOB_SERVER_VARIANT="${SJS_VERSION}_hdp-2.3.0"
JOB_SERVER_URL="http://$HOSTNAME:8090"
TESTS="(SparkExecutor|BigDataConnectors)/(spark_1_3|spark_all|Hive|HDFS)/.+"
SERVICES="HDFS YARN Zookeeper Hive_Metastore WebHCat" # Ranger is not included in startup makefile
START_RANGER="true"
