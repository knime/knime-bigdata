#
# Generic cloudera setup script
# Run this after job server installation.
#
SANDBOX_HOSTNAME="quickstart.cloudera"

if [ -d $CFG_DIR/files ]; then
    log "Uploading additional files"
    scp -pr $SCPPARAM $CFG_DIR/files/* root@$HOSTNAME:/
fi

# Fix DNS lookups
run_on_vm "echo 'nameserver 172.17.21.1' > /etc/resolv.conf"

log "Replacing quickstart.cloudera with $HOSTNAME in /etc"
run_on_vm "
    find /etc/ -type f -print0 > /tmp/etc.files
    cat /tmp/etc.files | xargs -0 -L1 sed -i -e 's#$SANDBOX_HOSTNAME#$HOSTNAME#g'"

# fix cdh spark default fs bug
run_on_vm "ln -s /etc/hadoop/conf/core-site.xml /etc/spark/conf/core-site.xml"

# run_on_vm "
#     sync && \
#     echo 0 > /proc/sys/kernel/hung_task_timeout_secs && \
#     echo 3 > /proc/sys/vm/drop_caches && \
#     echo 0 > /proc/sys/vm/drop_caches"

log "Starting services via init scripts"
run_on_vm "
    for service in $SERVICES spark-job-server; do
        service \$service start
    done
"
. $BASE_DIR/generic/wait-for-job-server.sh

log "Initializing spark-job-server home in HDFS"
run_on_vm "su hdfs -c 'hdfs dfs -mkdir /user/spark-job-server; hdfs dfs -chown spark-job-server /user/spark-job-server'"

log "Importing test data"
scp $SCPPARAM $BASE_DIR/test-data/* root@$HOSTNAME:/tmp/
run_on_vm '
  # cdrs.txt
  su hdfs -c "hdfs dfs -mkdir -p /demo/data/CDR"
  su hdfs -c "hdfs dfs -copyFromLocal /tmp/cdrs.txt /demo/data/CDR/cdrs.txt"
'
if echo "$SERVICES" | grep -i hive > /dev/null; then
  run_on_vm '
    # hive
    while ! (netstat -tulpn | grep :10000 > /dev/null); do echo "waiting for hive..."; sleep 5; done
    beeline -u jdbc:hive2://localhost:10000 -n hive -f /tmp/load_iris_hive.sql
  '
fi

if [ "$START_IMPALA" = "true" ]; then
  log "Starting impala and loading test data"
  run_on_vm '
    while ! (netstat -tulpn | grep :9083 > /dev/null); do echo "waiting for hive metastore..."; sleep 5; done
    service impala-state-store start
    service impala-catalog start
    service impala-server start

    # test data
    while ! (netstat -tulpn | grep :21000 > /dev/null); do echo "waiting for impala..."; sleep 5; done
    impala-shell -u impala -q "INVALIDATE METADATA"
  '
fi

log "HDFS save mode:"
run_on_vm "hdfs dfsadmin -fs hdfs://$HOSTNAME:8020 -safemode get | grep 'Safe mode is'"

