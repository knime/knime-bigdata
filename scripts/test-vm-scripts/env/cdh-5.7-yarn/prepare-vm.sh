. $BASE_DIR/generic/install-job-server.sh

# enable yarn with low executor count
run_on_vm "
    sed -i -e 's/^\s*master\s*=.*$/master = \"yarn-client\"/' \
           -e 's/^.*spark.executor.instances.*$/spark.executor.instances = 2/' /opt/spark-job-server/environment.conf"

. $BASE_DIR/generic/prepare-cdh.sh
