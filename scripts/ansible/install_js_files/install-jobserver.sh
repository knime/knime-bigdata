#!/bin/bash

if [ "$UID" != "0" ] ; then
  echo "Script must be run as root!"
  exit 1
fi

MIN_ARGS=2  # Min number of args
MAX_ARGS=4  # Max number of args
USAGE="Usage: $(basename $0) [--clear-tmp] [--clear-log] <jobserver-tar-gz> [<linkname>]
  --clear-tmp         Whether to delete /tmp/spark-jobserver*
  --clear-log         Whether to delete /var/log/spark-jobserver*
Script must be run as root user."

[ $# -lt "$MIN_ARGS" ] && { echo "${USAGE}" ; exit 1 ; }
[ $# -gt "$MAX_ARGS" ] && { echo "${USAGE}" ; exit 1 ; }

CLEARTMP=""
CLEARLOG=""
JSBUILD=""
LINKNAME="spark-job-server"

INSTDIR=/opt
JSUSER=spark-job-server

for var in "$@" ; do
    if [ "$var" = "--clear-tmp" ] ; then
      CLEARTMP="true"
    elif [ "$var" = "--clear-log" ] ; then
      CLEARLOG="true"
    elif [ -z "$JSBUILD" ] ; then
      JSBUILD="$var"
    else
      LINKNAME="$var"
    fi
done

JSBUILD=$( readlink -f "$JSBUILD" )

[ -f "$JSBUILD" ] || { echo "$JSBUILD does not exist" ; exit 1 ; }
[[ "$JSBUILD" =~ ^.*\.tar\.gz$ ]] || { echo "$JSBUILD does not exist" ; exit 1 ; }
[ -n "$LINKNAME" ] || { echo "<linkname> has not been provided" ; exit 1 ; }

if [ -e "$INSTDIR/$LINKNAME" ] ; then
  echo "Stopping spark-job-server"

  # stop any running jobserver
  command -v  systemctl >/dev/null 2>&1 && { systemctl stop $LINKNAME ; }
  command -v  systemctl >/dev/null 2>&1 || { /etc/init.d/$LINKNAME stop ; }

  # backup old installation files
  BAKDIR="/root/install-jobserver-backup-$( date --rfc-3339=seconds | sed 's/ /_/' )"
  mkdir -p "$BAKDIR"

  if [ -L "$INSTDIR/$LINKNAME" ] ; then
    rm "$INSTDIR/$LINKNAME"
  fi

  LINKTARGET=$( readlink -f "$INSTDIR/$LINKNAME" )

  echo "Backing up $LINKTARGET to $BAKDIR"
  # move any old jobserver files into backup area
  mv "$LINKTARGET" "$BAKDIR/"

  [ -n "$CLEARTMP" ] && { rm -Rf "/tmp/$LINKNAME" ; }
  [ -n "$CLEARLOG" ] && { rm -Rf "/var/log/$LINKNAME" ; }
fi

if [ -z """$(getent passwd | cut -d ':' -f 1 | grep "$JSUSER" )""" ] ; then
  useradd -d "$INSTDIR/$LINKNAME" -M -r -s /bin/false "$JSUSER"
fi

pushd $(mktemp -d)
tar -xzf "$JSBUILD" -C "$PWD"/
JSDIR="$(basename ./*)"
chown -R "$JSUSER" "$JSDIR"
mv "$JSDIR" "$INSTDIR/"
rm -R $PWD
popd

pushd "$INSTDIR"
if [ -L "$INSTDIR/$LINKNAME" ] ; then
  rm "$INSTDIR/$LINKNAME"
fi
ln -s "$JSDIR" "$LINKNAME"
popd

if [ -e "/etc/init.d/$LINKNAME" ] ; then
  rm "/etc/init.d/$LINKNAME"
fi
ln -s "$INSTDIR/$LINKNAME"/spark-job-server-init.d "/etc/init.d/$LINKNAME"

command -v  systemctl >/dev/null 2>&1 && { systemctl daemon-reload ; systemctl enable "$LINKNAME" ; }
command -v  systemctl >/dev/null 2>&1 || { chkconfig "$LINKNAME" on ; }
