# Configure host system
run_on_vm "
    echo '# disabled' > /etc/resolv.conf
    sed -ie 's#dockerd#dockerd --bip=192.168.140.1/24#' /usr/lib/systemd/system/docker.service
    systemctl daemon-reload
    systemctl restart docker.service"

echo "Waiting for docker daemon to start up"
run_on_vm "until /usr/bin/docker ps 2>&1| grep STATUS>/dev/null; do sleep 1; done; >/dev/null"

echo "Starting docker container"
run_on_vm "docker run -v hadoop:/hadoop --name sandbox --hostname '$HOSTNAME' --privileged -d \
    -p 10000:10000 \
    -p 1000:1000 \
    -p 10001:10001 \
    -p 10500:10500 \
    -p 11000:11000 \
    -p 1100:1100 \
    -p 1220:1220 \
    -p 14000:14000 \
    -p 15000:15000 \
    -p 16010:16010 \
    -p 16030:16030 \
    -p 18080:18080 \
    -p 1988:1988 \
    -p 19888:19888 \
    -p 21000:21000 \
    -p 2100:2100 \
    -p 2181:2181 \
    -p 2222:22 \
    -p 4040:4040 \
    -p 4200:4200 \
    -p 42111:42111 \
    -p 50070:50070 \
    -p 5007:5007 \
    -p 50075:50075 \
    -p 50095:50095 \
    -p 50111:50111 \
    -p 5011:5011 \
    -p 60000:60000 \
    -p 6001:6001 \
    -p 6003:6003 \
    -p 60080:60080 \
    -p 6008:6008 \
    -p 6080:6080 \
    -p 6188:6188 \
    -p 8000:8000 \
    -p 8005:8005 \
    -p 8020:8020 \
    -p 8040:8040 \
    -p 8042:8042 \
    -p 8050:8050 \
    -p 8080:8080 \
    -p 8082:8082 \
    -p 8086:8086 \
    -p 8088:8088 \
    -p 8090:8090 \
    -p 8091:8091 \
    -p 8188:8188 \
    -p 8443:8443 \
    -p 8744:8744 \
    -p 8765:8765 \
    -p 8886:8886 \
    -p 8888:8888 \
    -p 8889:8889 \
    -p 8983:8983 \
    -p 8993:8993 \
    -p 9000:9000 \
    -p 9090:9090 \
    -p 9995:9995 \
    -p 9996:9996 \
    sandbox-httpfs /usr/sbin/sshd -D"
run_on_vm "docker cp /root/.ssh/authorized_keys sandbox:/root/.ssh/authorized_keys "
run_on_vm "echo -n knime123 | docker exec -i sandbox passwd --stdin root > /dev/null"

####### configure docker system ########
SSHPARAM="$SSHPARAM -p 2222"
SCPPARAM="-P 2222 $SCPPARAM"

# job server with context-per-jvm
. $BASE_DIR/generic/install-job-server.sh
run_on_vm "sed -i -e 's#context-per-jvm.*#context-per-jvm = true#' /opt/spark-job-server/environment.conf"

# copy config
scp -pr $SCPPARAM $CFG_DIR/files/* root@$HOSTNAME:/

. $BASE_DIR/generic/prepare-hdp.sh
