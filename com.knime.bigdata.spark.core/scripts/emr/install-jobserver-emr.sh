#!/bin/bash
#
# Versions are auto detected if not defined in ENV.
# Run installer with custom versions:
#   KNIME_VERSION="3.3" SPARK_VERSION="1.6" JS_VERSION="0.6.2.1" sh install-jobserver-emr.sh
#

if [ "$UID" != "0" ] ; then
  echo "Script must be run as root!"
  exit 1
fi 

MIN_ARGS=0  # Min number of args
MAX_ARGS=3  # Max number of args
USAGE="Usage: $(basename $0) [--clear-tmp] [--clear-log] [jobserver-tar-gz]
  --clear-tmp         Whether to delete /tmp/spark-jobserver*
  --clear-log         Whether to delete /var/log/spark-jobserver*
  --help              Print this help.

Script must be run as root user.

If you do not specify <jobserver-tar-gz> then this script will download
a jobserver build from the KNIME website. This requires at least two environment variables to
construct the download URL:
 - JS_VERSION must be set to a version string of a jobserver build, e.g. JS_VERSION=0.6.2.1
 - KNIME_VERSION must be set to a KNIME release version, e.g. KNIME_VERSION=3.3
For the download, this script will try to autodetect the Spark version. You can overwrite the
autodetectiom by setting an environment variable SPARK_VERSION to a Spark release, e.g.
SPARK_VERSION=1.6
"

[ $# -lt "$MIN_ARGS" ] && { echo "${USAGE}" ; exit 1 ; }
[ $# -gt "$MAX_ARGS" ] && { echo "${USAGE}" ; exit 1 ; }

CLEARTMP=""
CLEARLOG=""
DOWNLOAD="true"
JSBUILD==""
INSTDIR=/mnt

for var in "$@" ; do
    if [ "$var" = "--clear-tmp" ] ; then
      CLEARTMP="true"
    elif [ "$var" = "--clear-log" ] ; then
      CLEARLOG="true"
    elif [ "$var" = "--help" ] ; then
      echo "${USAGE}" ; exit 1
    else
      DOWNLOAD=""
      JSBUILD="$var"
    fi
done

# Identify spark version and download job server
if [ -n "$DOWNLOAD" ] ; then
  [ -z "$JS_VERSION" ] && { echo "Environment variable JS_VERSION not set." ; echo "${USAGE}" ; exit 1 ; }
  [ -z "$KNIME_VERSION" ] && { echo "Environment variable KNIME_VERSION not set." ; echo "${USAGE}" ; exit 1 ; }


  if [ -z "$SPARK_VERSION" ] ; then
    [ -d "/home/hadoop/spark/bin" ] && export PATH=/home/hadoop/spark/bin:$PATH

    echo "Detecting spark version..."
    echo 'val fw = new java.io.FileWriter("/tmp/spark-version.txt"); try fw.write(org.apache.spark.SPARK_VERSION.split("\\.").take(2).mkString(".")) finally fw.close(); exit' | spark-shell --master local 2> /dev/null > /dev/null
    SPARK_VERSION=$(cat /tmp/spark-version.txt)

    if [ -z "$SPARK_VERSION" ] ; then
      echo "Unable to identify current spark version! Set SPARK_VERSION in ENV or ensure spark-shell is available."
      exit 1
    else
      echo "Found spark version $SPARK_VERSION."
    fi
  fi

  JSBUILD="/tmp/spark-job-server.tar.gz"
  ENV_CONF="/tmp/spark-job-server-environment.conf"
  BASE_URL="https://download.knime.org/store/$KNIME_VERSION/spark-job-server-$JS_VERSION-KNIME"
  JOB_SERVER_URL="${BASE_URL}_spark-${SPARK_VERSION}.tar.gz"
  ENV_CONF_URL="${BASE_URL}_emr_environment.conf"

  echo "Downloading job server from: $JOB_SERVER_URL"
  wget -q -O $JSBUILD "$JOB_SERVER_URL"
  echo "Downloading environment.conf from: $ENV_CONF_URL"
  wget -q -O $ENV_CONF "$ENV_CONF_URL"
fi

[ -f "$JSBUILD" ] || { echo "$JSBUILD does not exist" ; exit 1 ; }
[[ "$JSBUILD" =~ ^.*\.tar\.gz$ ]] || { echo "$JSBUILD does not exist" ; exit 1 ; }


if [ -e "$INSTDIR/spark-job-server" ] ; then
  echo "Stopping spark-job-server"
  
  # stop any running jobserver
  command -v  systemctl >/dev/null 2>&1 && { systemctl stop spark-job-server ; }
  command -v  systemctl >/dev/null 2>&1 || { /etc/init.d/spark-job-server stop ; }

  # backup old installation files
  BAKDIR="/root/install-jobserver-backup-$( date --rfc-3339=seconds | sed 's/ /_/' )"
  mkdir -p "$BAKDIR"
  
  if [ -L "$INSTDIR/spark-job-server" ] ; then
    rm "$INSTDIR/spark-job-server"
  fi
  
  echo "Backing up spark-job-server installation(s) to $BAKDIR"
  # move any old jobserver files into backup area
  mv "$INSTDIR"/spark-job-server* "$BAKDIR/"
  
  [ -n "$CLEARTMP" ] && { rm -Rf /tmp/spark-job-server/ ; rm -Rf /tmp/spark-jobserver/ ; }
  [ -n "$CLEARLOG" ] && { rm -Rf /var/log/spark-job-server/ ; }
fi

pushd $(mktemp -d) > /dev/nul
tar -xzf "$JSBUILD" -C "$PWD"/
JSDIR="$(basename ./*)"
mv "$JSDIR" "$INSTDIR/"
rm -R $PWD
popd > /dev/null

pushd "$INSTDIR" > /dev/null
[ -n "$ENV_CONF" ] && mv $ENV_CONF $JSDIR/environment.conf
### EMR image version ###
if [ -d "/usr/lib/spark" ] ; then
JOBSERVER_USER=hadoop
cat - >> $JSDIR/settings.sh <<[SETTINGS]
# EMR settings
SPARK_HOME="/usr/lib/spark"
SPARK_CONF_DIR="/usr/lib/spark/conf"
LOG_DIR="${INSTDIR}/${JSDIR}/log"
SPARK_SUBMIT_OPTIONS="--conf spark.sql.hive.metastore.jars=/usr/lib/hive/lib/* --conf spark.sql.hive.metastore.version=1.0.0"
[SETTINGS]


# write a hive-site.xml specifically for Spark which fixes the following issue that may appear
# during Spark2Hive jobs:
# https://issues.apache.org/jira/browse/SPARK-11021 (can happen )
cat - > /usr/lib/spark/conf/hive-site.xml <<[HIVESETTINGS]
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.exec.stagingdir</name>
    <value>/tmp/hive/spark-\${user.name}</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>$(grep -o 'thrift://[^<]*' /etc/hive/conf/hive-site.xml)</value>
  <description>JDBC connect string for a JDBC metastore</description>
</property>
</configuration>
[HIVESETTINGS]

# this line removes /etc/hive/conf/hive-site.xml from the classpath (see above, we created a hive-site.xml specifically for Spark)
sed -i '/^spark.driver.extraClassPath/ s/\/etc\/hive\/conf://' /usr/lib/spark/conf/spark-defaults.conf
# this line adds the guava libs to the driver classpath, otherwise instantiating a Hive client fails with ClassNotFoundException
sed -i '/^spark.driver.extraClassPath/ s/$/:\/usr\/lib\/hive\/lib\/guava-11.0.2.jar/' /usr/lib/spark/conf/spark-defaults.conf

# make HDFS homedir
su -c "hadoop fs -mkdir /user/$JOBSERVER_USER" hadoop
su -c "hadoop fs -chown $JOBSERVER_USER /user/$JOBSERVER_USER" hadoop

### AmazonAMI version ###
elif [ -d "/home/hadoop/spark" ] ; then
JOBSERVER_USER=hadoop
sed -r "s#^USER=.*\$#USER=${JOBSERVER_USER}#" -i $JSDIR/spark-job-server-init.d
cat - >> $JSDIR/settings.sh <<[SETTINGS]
# EMR settings
SPARK_HOME="/home/hadoop/spark"
SPARK_CONF_DIR="/home/hadoop/spark/conf"
LOG_DIR="${INSTDIR}/${JSDIR}/log"
source /etc/hadoop/yarn-env.sh
[SETTINGS]
mkdir -p /mnt/spark
chown -R $JOBSERVER_USER /mnt/spark
fi

id $JOBSERVER_USER >/dev/null 2>&1 || { useradd -U -M -s /bin/false -d ${INSTDIR}/${JSDIR} $JOBSERVER_USER; }
chown -R $JOBSERVER_USER "$JSDIR"
sed -r "s#^JSDIR=.*\$#JSDIR=${INSTDIR}/${JSDIR}#" -i $JSDIR/spark-job-server-init.d
sed -r "s#^LOGDIR=.*\$#LOGDIR=${INSTDIR}/${JSDIR}/log#" -i $JSDIR/spark-job-server-init.d
sed -r "s#^USER=.*\$#USER=${JOBSERVER_USER}#" -i $JSDIR/spark-job-server-init.d

if [ -L "$INSTDIR/spark-job-server" ] ; then
  rm "$INSTDIR/spark-job-server"
fi
ln -s "$JSDIR" spark-job-server
popd > /dev/null

if [ -e /etc/init.d/spark-job-server ] ; then
  rm /etc/init.d/spark-job-server
fi
ln -s "$INSTDIR"/spark-job-server/spark-job-server-init.d /etc/init.d/spark-job-server

command -v  systemctl >/dev/null 2>&1 && { systemctl daemon-reload ; systemctl enable spark-job-server ; systemctl start spark-job-server ; }
command -v  systemctl >/dev/null 2>&1 || { chkconfig spark-job-server on ; /etc/init.d/spark-job-server start ; }
