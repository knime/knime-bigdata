#!/bin/bash
#
# spark-job-server    The open-source Spark Job Server
#
# chkconfig: 2345 70 30
# description: The Spark Job Server provides a RESTful interface to manage Spark jobs

. /etc/init.d/functions

# installation directory of the job server
JSDIR=/opt/spark-job-server
# the Linux user under which the job server runs
USER=hadoop

DAEMON_START="$JSDIR/server_start.sh"
DAEMON_STOP="$JSDIR/server_stop.sh"
LOGDIR=/var/log/spark-job-server
PIDFILE="$JSDIR/spark-jobserver.pid"
DO="/sbin/runuser -s /bin/bash $USER -c"

jobserver_start() {

        echo -n "Starting Spark Job-Server: "
        for dir in "$(dirname $PIDFILE)/" "$LOGDIR"
        do
            mkdir -p "$dir"
            chown -R $USER "$dir"
        done

        # Check if already running
        if [ -e "$PIDFILE" ] && checkpid """$(cat "$PIDFILE")""" ; then
            if ps -p """$(cat "$PIDFILE")""" -f | grep -q "$DAEMON_START" ; then
              echo "already running"
              return
            fi
        fi

        pushd "$JSDIR" >/dev/null
        $DO "$DAEMON_START"
        ret=$?
        popd >/dev/null
        base=$(basename $0)
        if [ $ret -eq 0 ]; then
            sleep 3
            test -e "$PIDFILE" && checkpid """$(cat "$PIDFILE")"""
            ret=$?
        fi
        if [ $ret -eq 0 ]; then
            # increase no-of-open-fds ulimit for process (this is somewhat low on EMR)
            prlimit --nofile=32768 --pid """$(cat "$PIDFILE")"""
            success $"$base startup"
        else
            failure $"$base startup"
        fi
        echo
        return $ret
}

jobserver_stop() {
        echo -n "Shutting down Spark Job-Server: "

        pushd "$JSDIR" >/dev/null
        $DO "$DAEMON_STOP" >/dev/null
        ret=$?
        popd >/dev/null
        if [ $ret -eq 0 ]; then
            success $"$base shutdown"
        else
            failure $"$base shutdown"
        fi
        echo
        return $ret
}

jobserver_restart() {
  jobserver_stop
  jobserver_start
}

case "$1" in
    start)
        jobserver_start
        ;;
    stop)
        jobserver_stop
        ;;
    status)
        status -p "$PIDFILE" spark-job-server
        ;;
    restart)
        jobserver_restart
        ;;
    *)
        echo "Usage: spark-job-server {start|stop|status|restart"
        exit 1
        ;;
esac
exit $?
