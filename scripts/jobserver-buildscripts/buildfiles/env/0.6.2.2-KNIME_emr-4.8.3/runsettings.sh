# ############################################################################
# Environment variables for the Spark Job Server (sourced by server_start.sh)
# ############################################################################

# The amount of RAM (eg 512m, 2G) to allocate to job server. The job server
# runs within the Spark driver process (via spark-submit). Hence this actually
# sets "spark.driver.memory" (see Spark documentation).
JOBSERVER_MEMORY=4G

# Spark installation and configuration directories on this machine.
SPARK_HOME=/usr/lib/spark
SPARK_CONF_DIR=$SPARK_HOME/conf

# Optional spark-submit arguments.
SPARK_SUBMIT_OPTIONS="--conf spark.sql.hive.metastore.jars=/usr/lib/hive/lib/* --conf spark.sql.hive.metastore.version=1.0.0"

# Optional spark-submit java arguments (driver-java-options)
# SPARK_SUBMIT_JAVA_OPTIONS="-Dspark.executor.extraClassPath=$SPARK_HOME/../hive/lib/*"

# Logging directory of the job server.
# IMPORTANT: If you change this value AND are using the boot-script (spark-job-server-init.d),
# you also have to change it in the boot-script!
LOG_DIR=/mnt/spark-job-server/log

# Name of pid file that will be created by server_start.sh. This is interpreted
# relative to the job server installation directory.
# IMPORTANT: If you change this value AND are using the boot-script (spark-job-server-init.d),
# you also have to change it in the boot-script!
PIDFILE=spark-jobserver.pid

# This option specifies the maximum total size of java.nio (New I/O package) direct buffer allocations.
MAX_DIRECT_MEMORY=512M

# Only needed for YARN running outside of the cluster
# You will need to COPY these files from your cluster to the remote machine
# Normally these are kept on the cluster in /etc/hadoop/conf
# YARN_CONF_DIR=/pathToRemoteConf/conf

# On Kerberos secured clusters, jobserver requires a TGT ticket in order
# to access other Hadoop services and do impersonation. Jobserver can automatically
# acquire and renew a TGT. Set JOBSERVER_KEYTAB, if you want jobserver to acquire and renew
# a TGT automatically. If the principal that matches the
# keytab file differs from the assumed default principal you need to set JOBSERVER_PRINCIPAL.
# The assumed default principal is:
# <jobserver-linux-user>/$(hostname -f)/<default realm from /etc/krb5.conf>
# export JOBSERVER_KEYTAB=/path/to/keytab
# export JOBSERVER_PRINCIPAL=user/host@REALM
