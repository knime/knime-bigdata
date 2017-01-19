. $BASE_DIR/generic/install-job-server.sh

# enable context-per-jvm
run_on_vm "sed -i -e 's#context-per-jvm.*#context-per-jvm = true#' /opt/spark-job-server/environment.conf"

. $BASE_DIR/generic/prepare-hdp.sh
