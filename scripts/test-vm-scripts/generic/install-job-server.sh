JOB_SERVER_USER=spark-job-server
JOB_SERVER_DIR="/opt/spark-job-server-$JOB_SERVER_VARIANT"

log "Installing spark job server into $JOB_SERVER_VARIANT"
scp $SCPPARAM $BASE_DIR/spark-job-server/$JOB_SERVER_TGZ root@$HOSTNAME:/opt/
run_on_vm "
  tar xf /opt/$JOB_SERVER_TGZ -C /opt/
  ln -s $JOB_SERVER_DIR /opt/spark-job-server
  id $JOB_SERVER_USER >/dev/null 2>&1 || useradd -U -M -s /bin/false -d $JOB_SERVER_DIR $JOB_SERVER_USER
  chown -R ${JOB_SERVER_USER}:${JOB_SERVER_USER} /opt/spark-job-server
  ln -s /opt/spark-job-server/spark-job-server-init.d /etc/init.d/spark-job-server
  cat /opt/spark-job-server/build.version
"
