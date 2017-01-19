#!/bin/bash
#
# big data test vm prepare script (starts and bootstrap vm)
#

if [ $# -lt 2 ] || [ $# -gt 5 ]; then
  echo "usage: $0 config hostname sjs-version [vboxshh] [debug]"
  echo -e "\tconfig - config dir (e.g. env/cdh-5.7)"
  echo -e "\thostname - VM hostname (e.g. bdvm-local or bdvm-knime412)"
  echo -e "\tsjs-version - Spark job server version (e.g. 0.6.2-KNIME)"
  echo -e "\tvboxssh - Optional remote virtual box host ssh string (e.g. jenkins@knime412.kn.knime.com)"
  echo -e "\tdebug - Enable debug mode (aka set -x)"
  exit 1
fi

BASE_DIR=$(readlink -f $(dirname $0))
CFG_DIR=$BASE_DIR/$1
HOSTNAME=$2
SJS_VERSION=$3
VBOX_SSH=$4
DEBUG=$5
SSHKEY=$(readlink -e $BASE_DIR/id_bd_vm)
SSHPARAM="-i $SSHKEY -q -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
SSHPARAM_VBM="-q -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
SCPPARAM="-i $SSHKEY -q -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"

if [ -n "$DEBUG" ]; then
    echo "Entering debug mode!"
    set -x
fi

if [ ! -d "$CFG_DIR" ]; then
    echo "Config not found: $CFG_DIR"
    exit 1
fi

. $CFG_DIR/settings.sh
JOB_SERVER_TGZ="spark-job-server-${JOB_SERVER_VARIANT}.tar.gz"

run_vbox_manage() {
  if [ -z $VBOX_SSH ]; then
    log "Running VBoxManage $1"
    VBoxManage $1
  else
    log "Running VBoxManage $1 via $VBOX_SSH"
    ssh $SSHPARAM_VBM $VBOX_SSH "VBoxManage $1"
  fi
}

run_on_vm() {
  ssh $SSHPARAM -i $SSHKEY root@$HOSTNAME "$1"
}

log() {
    echo "[$(date)] $1"
}

if [ ! -f "$BASE_DIR/spark-job-server/$JOB_SERVER_TGZ" ]; then
  echo "Can't find job server in $BASE_DIR/spark-job-server/$JOB_SERVER_TGZ"
  exit 1
fi

log "Preparing env with $CFG_DIR on vm $VM_NAME with hostname $HOSTNAME"

if ! run_vbox_manage "showvminfo $VM_NAME --machinereadable" | grep 'VMState="poweroff"' > /dev/null; then
  echo "VM is in unknown state! Power off first."
  exit 1
fi

# reset and start
run_vbox_manage "snapshot $VM_NAME restore bootstrap"

if [ -n "$DISPLAY" ]; then
    run_vbox_manage "startvm $VM_NAME"
else
    run_vbox_manage "startvm $VM_NAME --type headless"
fi

log "Waiting for vm startup..."
UP=0
while [ $UP -eq 0 ]; do
  UP=$(ssh $SSHPARAM root@$HOSTNAME 'ps a | grep -v grep | grep /etc/rc.d/rc > /dev/null;echo $?') || UP=0
  [ $UP -eq 0 ] && sleep 2
done
log "VM started"

# Run host specific setup
. $CFG_DIR/prepare-vm.sh

log "Creating jenkins-tests.txt with $TESTS"
echo -n "$TESTS" > $BASE_DIR/jenkins-tests.txt
for file in prefs.epf flowvariables.csv; do
    log "Creating jenkins-$file"
    sed -e "s#%HOSTNAME%#$HOSTNAME#g" -e "s#%BASE_DIR%#$BASE_DIR#g" < $CFG_DIR/$file > $BASE_DIR/jenkins-$file
done

log "VM prepared!"

