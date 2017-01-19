. $BASE_DIR/generic/install-job-server.sh

. $BASE_DIR/generic/generate-ssl-certificates.sh
scp -pr $SCPPARAM $BASE_DIR/server.keystore.jks root@$HOSTNAME:/etc/hadoop/
run_on_vm "
    rm /etc/hadoop-httpfs/tomcat-conf
    ln -s /etc/alternatives/hadoop-httpfs-tomcat-conf /etc/hadoop-httpfs/tomcat-conf
    rm /etc/alternatives/hadoop-httpfs-tomcat-conf
    ln -s /etc/hadoop-httpfs/tomcat-conf.https /etc/alternatives/hadoop-httpfs-tomcat-conf"

# enable context-per-jvm, ssl and authentication
run_on_vm "sed -i -e 's#context-per-jvm.*#context-per-jvm = true#' /opt/spark-job-server/environment.conf"
run_on_vm "sed -i 's#spray.can.server {#spray.can.server {\nssl-encryption = on\nkeystore = \"/etc/hadoop/server.keystore.jks\"\nkeystorePW = \"testtest\"#' /opt/spark-job-server/environment.conf"
run_on_vm "echo -n 'shiro {
    authentication = on
    config.path = \"/opt/spark-job-server/shiro.ini\"
}' >> /opt/spark-job-server/environment.conf"

. $BASE_DIR/generic/prepare-cdh.sh
