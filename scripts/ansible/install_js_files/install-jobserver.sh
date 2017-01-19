#!/bin/bash

if [ "$UID" != "0" ] ; then
  echo "Script must be run as root!"
  exit 1
fi 

MIN_ARGS=1  # Min number of args
MAX_ARGS=3  # Max number of args
USAGE="Usage: $(basename $0) [--clear-tmp] [--clear-log] <jobserver-tar-gz>
  --clear-tmp         Whether to delete /tmp/spark-jobserver*
  --clear-log         Whether to delete /var/log/spark-jobserver*
Script must be run as root user."

[ $# -lt "$MIN_ARGS" ] && { echo "${USAGE}" ; exit 1 ; }
[ $# -gt "$MAX_ARGS" ] && { echo "${USAGE}" ; exit 1 ; }

CLEARTMP=""
CLEARLOG=""
JSBUILD==""
INSTDIR=/opt
JSUSER=spark-job-server

for var in "$@" ; do
    if [ "$var" = "--clear-tmp" ] ; then
      CLEARTMP="true"
    elif [ "$var" = "--clear-log" ] ; then
      CLEARLOG="true"
    else
      JSBUILD="$var"
    fi
done

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

if [ -z """$(getent passwd | cut -d ':' -f 1 | grep "$JSUSER" )""" ] ; then
  useradd -d "$INSTDIR/spark-job-server" -M -r -s /bin/false "$JSUSER"
fi

pushd $(mktemp -d)
tar -xzf "$JSBUILD" -C "$PWD"/
JSDIR="$(basename ./*)"
chown -R "$JSUSER" "$JSDIR"
mv "$JSDIR" "$INSTDIR/"
rm -R $PWD
popd

pushd "$INSTDIR"
if [ -L "$INSTDIR/spark-job-server" ] ; then
  rm "$INSTDIR/spark-job-server"
fi
ln -s "$JSDIR" spark-job-server
popd

if [ -e /etc/init.d/spark-job-server ] ; then
  rm /etc/init.d/spark-job-server
fi
ln -s "$INSTDIR"/spark-job-server/spark-job-server-init.d /etc/init.d/spark-job-server

command -v  systemctl >/dev/null 2>&1 && { systemctl daemon-reload ; systemctl enable spark-job-server ; systemctl start spark-job-server ; }
command -v  systemctl >/dev/null 2>&1 || { chkconfig spark-job-server on ; /etc/init.d/spark-job-server start ; }

su -l -c """hdfs dfs -mkdir -p "/user/$JSUSER" ; hdfs dfs -chown -R "$JSUSER" "/user/$JSUSER"""" hdfs
