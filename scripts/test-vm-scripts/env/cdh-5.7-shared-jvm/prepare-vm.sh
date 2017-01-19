. $BASE_DIR/generic/install-job-server.sh

# disable context-per-jvm
run_on_vm "sed -i -e 's#context-per-jvm.*#context-per-jvm = false#' /opt/spark-job-server/environment.conf"

. $BASE_DIR/generic/prepare-cdh.sh
