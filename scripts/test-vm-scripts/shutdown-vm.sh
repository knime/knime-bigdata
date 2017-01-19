#!/bin/bash
#
# big data test vm shutdown script
#

if [ $# -lt 2 ] || [ $# -gt 4 ]; then
  echo "usage: $0 config hostname [vboxshh] [debug]"
  echo -e "\tconfig - config dir (e.g. env/cdh-5.7)"
  echo -e "\thostname - VM hostname (e.g. bdvm-local or bdvm-knime412)"
  echo -e "\tvboxssh - Optional remote virtual box host ssh string (e.g. jenkins@knime412.kn.knime.com)"
  echo -e "\tdebug - Enable debug mode (aka set -x)"
  exit 1
fi

BASE_DIR=$(readlink -f $(dirname $0))
CFG_DIR=$BASE_DIR/$1
HOSTNAME=$2
VBOX_SSH=$3
DEBUG=$4
SSHKEY=$(readlink -e $BASE_DIR/id_bd_vm)
SSHPARAM="-i $SSHKEY -q -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
SSHPARAM_VBM="-q -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"

if [ -n "$DEBUG" ]; then
    echo "Entering debug mode!"
    set -x
fi

if [ ! -d "$CFG_DIR" ]; then
    echo "Config not found: $CFG_DIR"
    exit 1
fi
. $CFG_DIR/settings.sh

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

CONTEXTS=$(curl -k -s $JOB_SERVER_URL/contexts)
log "Registered contexts: $CONTEXTS"

log "Memory usage on VM"
run_on_vm "free"

log "Power off VM with $CFG_DIR on vm $VM_NAME"

if ! run_vbox_manage "showvminfo $VM_NAME --machinereadable" | grep 'VMState="running"' > /dev/null; then
  echo "VM is in unknown state! Don't know how to shutdown..."
  exit 1
fi

# reset and start
run_vbox_manage "controlvm $VM_NAME poweroff"

