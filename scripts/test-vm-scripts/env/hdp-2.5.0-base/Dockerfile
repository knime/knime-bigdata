# Install httpfs in HDP 2.5 docker sandbox
#
# Place this file and rpm's into /root/sandbox-httpfs and run:
#  docker build -f /root/sandbox-httpfs/Dockerfile /root/sandbox-httpfs/ -t sandbox-httpfs
#
# Required rpms:
#  http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.5.0.0/hadoop/hadoop_2_5_0_0_1245-httpfs-2.7.3.2.5.0.0-1245.el6.x86_64.rpm
#  http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.5.0.0/hadoop/hadoop_2_5_0_0_1245-httpfs-server-2.7.3.2.5.0.0-1245.el6.x86_64.rpm
#
FROM sandbox
COPY hadoop*httpfs*.rpm /tmp/
RUN rpm -i /tmp/hadoop*httpfs*.rpm
RUN ln -s /usr/hdp/current/hadoop-httpfs/webapps /etc/hadoop-httpfs/tomcat-deployment/

