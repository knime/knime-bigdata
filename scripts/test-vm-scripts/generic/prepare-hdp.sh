#
# Generic hortonworks setup script
# Run this after job server installation.
#
SANDBOX_HOSTNAME="sandbox.hortonworks.com"

if [ -d $CFG_DIR/files ]; then
    log "Uploading additional files"
    scp -pr $SCPPARAM $CFG_DIR/files/* root@$HOSTNAME:/
fi

# Fix DNS lookups
run_on_vm "echo 'nameserver 172.17.21.1' > /etc/resolv.conf"

log "Replacing sandbox.hortonworks.com with $HOSTNAME in /etc"
run_on_vm "
    find /etc/ -type f -print0 > /tmp/etc.files
    cat /tmp/etc.files | xargs -0 -L1 sed -i -e 's#$SANDBOX_HOSTNAME#$HOSTNAME#g'"

# mkdir /usr/hdp/2.4.0.0-169/hadoop-hdfs/logs
# mkdir /usr/hdp/2.4.0.0-169/hadoop/logs/
# mkdir /usr/hdp/2.4.0.0-169/hadoop-mapreduce/logs
# mkdir /usr/hdp/2.4.0.0-169/hadoop-yarn/logs
# sh "chown hdfs:hadoop /usr/hdp/2.4.0.0-169/hadoop*/logs"
# sh "chmod 777 /usr/hdp/2.4.0.0-169/hadoop*/logs"

# run_on_vm "
#     sync && \
#     echo 0 > /proc/sys/kernel/hung_task_timeout_secs && \
#     echo 3 > /proc/sys/vm/drop_caches && \
#     echo 0 > /proc/sys/vm/drop_caches"

if [ "$START_RANGER" = "true" ]; then
    log "Starting ranger services"
    run_on_vm "service ranger-admin start && service ranger-usersync start"
fi

log "Starting services $SERVICES and spark-job-server"
run_on_vm "
    make --makefile /usr/lib/hue/tools/start_scripts/start_deps.mf -B -i $SERVICES
    service spark-job-server start
"
run_on_vm "/usr/hdp/current/hadoop-httpfs/etc/rc.d/init.d/hadoop-httpfs start"
. $BASE_DIR/generic/wait-for-job-server.sh

log "Initializing spark-job-server home in HDFS"
run_on_vm "su hdfs -c 'hdfs dfs -mkdir /user/spark-job-server; hdfs dfs -chown spark-job-server /user/spark-job-server'"

log "Importing test data"
scp $SCPPARAM $BASE_DIR/test-data/* root@$HOSTNAME:/tmp/
run_on_vm '
  # hive
  while ! (netstat -tulpn | grep :10000 > /dev/null); do echo "waiting for hive..."; sleep 5; done
  beeline -u jdbc:hive2://localhost:10000 -n hive -f /tmp/load_iris_hive.sql
  # cdrs.txt
  su hdfs -c "hdfs dfs -chmod -R o+r /demo/data/"
'

log "HDFS save mode:"
run_on_vm "hdfs dfsadmin -fs hdfs://$HOSTNAME:8020 -safemode get | grep 'Safe mode is'"

