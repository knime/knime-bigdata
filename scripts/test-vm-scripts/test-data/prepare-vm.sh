#!/bin/bash
#
# big data test vm prepare script (starts and bootstrap vm)
#

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
  echo "usage: $0 config hostname [vboxshh]"
  echo "\tconfig - config dir (e.g. env/0.6.2-KNIME_cdh-5.7)"
  echo "\thostname - VM hostname (e.g. bdvm-knime412)"
  echo "\tvboxssh - Optional remote virtual box host ssh string (e.g. jenkins@knime412.kn.knime.com)"
  exit 1
fi

if [ ! -d "$1" ]; then
    echo "Config not found: $1"
    exit 1
fi

CFG_DIR=$1
HOSTNAME=$2
VBOX_SSH=$3
SSHKEY=$(readlink -e id_bd_vm)
SSHPARAM="-q -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
SCPPARAM="-q -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
. $CFG_DIR/settings.sh
JOB_SERVER_TGZ="spark-job-server-${JOB_SERVER_VARIANT}.tar.gz"

echo "Preparing env with $CFG_DIR on vm $VM_NAME with hostname $HOSTNAME"

run_vbox_manage() {
  if [ -z $VBOX_SSH ]; then
    VBoxManage $1
  else
    ssh $SSHPARAM $VBOX_SSH "$1"
  fi
  echo "Running $1 on $VBOX_HOST"
}

run_on_vm() {
  ssh $SSHPARAM -i $SSHKEY root@$HOSTNAME "$1"
}

finish() {
    popd
}
trap finish EXIT

pushd . >/dev/null
cd "$(dirname $0)"

if [ ! -f "spark-job-server/$JOB_SERVER_TGZ" ]; then
  echo "Can't find job server in $JOB_SERVER_TGZ"
  exit 1
fi

if run_vbox_manage "showvminfo $VM_NAME --machinereadable" | grep VMState="poweroff" > /dev/null; then
  echo "VM is in unknown state! Power off first."
  exit 1
fi

# reset and start
run_vbox_manage "snapshot $VM_NAME restorecurrent"

if [ -n "$DISPLAY" ]; then
    run_vbox_manage "startvm $VM_NAME"
else
    run_vbox_manage "startvm $VM_NAME --type headless"
fi

echo "Waiting for vm startup..."
UP=0
while [ $UP -eq 0 ]; do
  UP=$(ssh $SSHPARAM -i $SSHKEY root@$HOSTNAME 'ps a | grep -v grep | grep /etc/rc.d/rc > /dev/null;echo $?') || UP=0
  [ $UP -eq 0 ] && sleep 2
done
echo "VM started"

# Run host specific setup
. $CFG_DIR/prepare-vm.sh

echo "VM prepared!"

