#!/bin/sh
#
# Disable all big data services on cdh
#
# Run this script from guestfish:
#   guestfish -a test-vm.vmdk -i
#       > copy-in .../bootstrap-cdh.sh /tmp/
#       > sh "sh /tmp/bootstrap-cdh.sh"
#
chkconfig cloudera-quickstart-init off
chkconfig hadoop-hdfs-datanode off
chkconfig hadoop-hdfs-journalnode off
chkconfig hadoop-hdfs-namenode off
chkconfig hadoop-hdfs-secondarynamenode off
chkconfig hadoop-httpfs off
chkconfig hadoop-mapreduce-historyserver off
chkconfig hadoop-yarn-nodemanager off
chkconfig hadoop-yarn-proxyserver off
chkconfig hadoop-yarn-resourcemanager off
chkconfig hbase-master off
chkconfig hbase-regionserver off
chkconfig hbase-rest off
chkconfig hbase-thrift off
chkconfig hive-metastore off
chkconfig hive-server2 off
chkconfig hue off
chkconfig impala-catalog off
chkconfig impala-server off
chkconfig impala-state-store off
chkconfig mysqld off
chkconfig oozie off
chkconfig sentry-store off
chkconfig solr-server off
chkconfig spark-history-server off
chkconfig sqoop2-server off
chkconfig zookeeper-server off

# broken virtual box guest addon modules
chkconfig dkms_autoinstaller off

# bootmenu
[ -f /boot/grub/grub.conf ] && sed -i -e 's#timeout=.*#timeout=0#' /boot/grub/grub.conf
[ -f /boot/grub2/grub.cfg ] && sed -i -e 's#timeout=.*#timeout=0#' /boot/grub2/grub.cfg

