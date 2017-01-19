#1/bin/bash

E_BADARGS=85

if [[ ( ! -n "$1" ) || ( ( "$1" != "start") && ( "$1" != "stop") ) ]] ; then
  echo "Usage: $(basename $0) [start|stop] [--noblock] [SERVICE1 [SERVICE2 [SERVICE3 [...]]]]"
  echo "  --noblock    Do not wait until services are started/stopped (just issues the REST commands)"
  exit $E_BADARGS
fi

ACTION="$1"

BLOCKING=yes
if [[ "$2" = "--noblock" ]] ; then
  BLOCKING=no
fi

CLUSTER_NAME=HDP_2_3_4_7
AMB_HOST=localhost
AMB_PORT=8080
AMB_ADMIN_PW=admin

CLUSTER_URL="http://${AMB_HOST}:${AMB_PORT}/api/v1/clusters/${CLUSTER_NAME}"

# ########## Function definitions ####################

function print_ok {
  echo -e "\033[1;32m [OK] \033[0m"
}

function print_ok_with_msg {
  # first arg: the msg
  echo -e "\033[1;32m [OK: $1] \033[0m"
}

function print_fail {
  # first arg: reason
  echo -e "\033[1;31m [FAILED: $1] \033[0m"
}

function do_curl_get_with_headers {
  # first arg: url suffix (with leading slash)
  curl -u admin:${AMB_ADMIN_PW} -s -i -H 'X-Requested-By: ambari' ${CLUSTER_URL}$1
}

function do_curl_get {
  # first arg: url suffix (with leading slash)
  curl -u admin:${AMB_ADMIN_PW} -s -H 'X-Requested-By: ambari' ${CLUSTER_URL}$1
}

function do_curl_put {
  # first arg: content of HTTP PUT data
  # second arg: url suffix (with leading slash)
  curl -u admin:${AMB_ADMIN_PW} -s -H 'X-Requested-By: ambari' -X PUT -d "$1" ${CLUSTER_URL}$2
}

function do_get_service_status {
  # first arg: service name
  do_curl_get /services/$1 | jq -r ".ServiceInfo.state"
}

function start_amb_service {
  # first arg: service name
  echo -n "Starting service $1 via Ambari "

  local resp=$(do_get_service_status $1)
  if [ "${resp}" = "STARTED" ] ; then
    print_ok_with_msg "ALREADY STARTED"
    return 0
  elif [[ "${resp}" = "INSTALLED" ]] ; then
    resp=$( do_curl_put "{\"RequestInfo\": {\"context\" :\"Start $1 via REST\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"STARTED\"}}}" "/services/$1" | jq -r ".Requests.status" )
    if [ "${resp}" != "Accepted" ] ; then
      print_fail "Command result was ${resp}"
      return 1
    fi
  else
    print_fail "Service status is ${resp}"
    return 1
  fi

  while [[ $BLOCKING = "yes" && "$(do_get_service_status $1)" != "STARTED" ]] ; do
    sleep 1
  done
  print_ok
  return 0
}

function stop_amb_service {
  # first arg: service name
  echo -n "Stopping service $1 via Ambari "

  local resp=$( do_curl_get /services/$1 | jq -r ".ServiceInfo.state" )

  if [ "${resp}" = "INSTALLED" ] ; then
    print_ok_with_msg "ALREADY STOPPED"
    return 0
  fi

  if [[ "${resp}" = "STARTED" || "${resp}" = "STARTING" ]] ; then
    resp=$( do_curl_put "{\"RequestInfo\": {\"context\" :\"Stop $1 via REST\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"INSTALLED\"}}}" "/services/$1" | jq -r ".Requests.status" )
    if [ "${resp}" != "Accepted" ] ; then
      print_fail "Command result was ${resp}"
      return 1
    fi
  fi

  resp=$( do_curl_get /services/$1 | jq -r ".ServiceInfo.state" )
  while [[ $BLOCKING = "yes" && "${resp}" != "INSTALLED" ]] ; do
    sleep 1
    resp=$( do_curl_get /services/$1 | jq -r ".ServiceInfo.state" )
  done
  print_ok
  return 0
}


# ################## Determine Services ########################################
services=()
while test $# -gt 0 ; do
  if [[ "$1" != "start" && "$1" != "stop" && "$1" != "--noblock" ]] ; then
    services=("${services[@]}" "$1")
  fi
  shift
done

if [ ${#services[@]} = 0 ] ; then
  services=( HDFS ZOOKEEPER YARN HIVE )
fi

echo "Services to start/top: ${services[@]}"
# ##############################################################################

# ########## Wait for Ambari cluster and service status to be known ############
echo -n "Waiting for Ambari cluster ${CLUSTER_URL} "
resp=$( do_curl_get_with_headers /services | head -1 | grep -o '200 OK' )
while [ -z "${resp}" ] ; do
  sleep 1
  resp=$( do_curl_get_with_headers /services | head -1 | grep -o '200 OK' )
done

for i in ${services[@]} ; do
  while [[ "$(do_get_service_status $i)" = "UNKNOWN" ]] ; do
    sleep 1
  done
done
print_ok
# ##############################################################################

# ########## Start/stop services ###############################################
for i in ${services[@]} ; do
  if [ "${ACTION}" = "start" ] ; then
    start_amb_service $i
    if [[ $? != 0 ]] ; then
      echo "Exiting with error"
      exit 1
    fi
  else
    stop_amb_service $i
    if [[ $? != 0 ]] ; then
      echo "Exiting with error"
      exit 1
    fi
  fi
done
# ##############################################################################
