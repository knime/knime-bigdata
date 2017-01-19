#!/bin/sh
#
# Generic CentOS setup script
#
# Run this script from guestfish:
#   guestfish -a test-vm.vmdk -i
#       > copy-in .../bootstrap-centos.sh /tmp/
#       > sh "sh /tmp/bootstrap-centos.sh bdvm-knime412 172.17.21.71 172.17.21.1 04:12:2c:ca:15:82"
#
HOSTNAME="$1"
IP="$2"
GATEWAY="$3"
MAC="$4"

if [ -z "$MAC" ]; then
    echo "usage: $0 hostname ip gateway mac"
    exit 1
fi

echo "Initializing static IP $IP via $GATEWAY at $HOSTNAME with $MAC"

cat <<[NETWORK] > /etc/sysconfig/network
NETWORKING=yes
HOSTNAME=$HOSTNAME
[NETWORK]

cat <<[IFCFG] > /etc/sysconfig/network-scripts/ifcfg-eth0
DEVICE="eth0"
IPV6INIT="no"
MTU="1500"
NM_CONTROLLED="yes"
ONBOOT="yes"
TYPE="Ethernet"
UUID="04fd53bf-5351-4507-9c70-3900622a56ad"

HWADDR="$MAC"

BOOTPROTO="static"
IPADDR=$IP
NETMASK=255.255.255.0
GATEWAY=$GATEWAY

# BOOTPROTO="dhcp"
# DHCP_HOSTNAME="$HOSTNAME"
[IFCFG]

cat >> /etc/udev/rules.d/70-persistent-net.rules <<[RULE]
SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", ATTR{address}=="$MAC", ATTR{type}=="1", KERNEL=="eth*", NAME="eth0"
[RULE]

echo $HOSTNAME > /etc/hostname

cat <<[HOSTS] > /etc/hosts
127.0.0.1 localhost localhost.localdomain
$IP $HOSTNAME
[HOSTS]

echo "Network setup done"

echo "Setting root password and ssh key"
echo -n knime123 | passwd --stdin root > /dev/null
mkdir /root/.ssh
chmod 0600 /root/.ssh
cat <<[PUBKEY] > /root/.ssh/authorized_keys
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDD+LuNcjdivkFyp8uWRlotGO9mjP7KUMwgcHnBRvTg6z55SHRTacJ40QpcmXlFjLavQBfYOzLDkEl/LmZPj3mZmphVTrmKKINfcXTCoeap7ufwH6JGFq3OerHkKUiVf1H4OrmQ9kUG80nA1n2IHZxTwiZ4/JDsRBX5ThWhp2y6ZFLrkprYPAfqw+F4lVEeViJZ3OxfDmHmWvyjZP7YQ9REpsfK0c1XbayYrQ7yfX7aOmeRh2TkYybMgo61XuydisPpV0Y0pKzV69lPcbCK67LO2pRxLLyKFnPCSD8waBN91URp5a0l9v8/ivEcN4f0gDn2WY8qeYEWb2fBO8uiSM9p sascha@belka
[PUBKEY]

# bootmenu
[ -f /boot/grub/grub.conf ] && sed -i -e 's#timeout=.*#timeout=0#' /boot/grub/grub.conf
[ -f /boot/grub2/grub.cfg ] && sed -i -e 's#timeout=.*#timeout=0#' /boot/grub2/grub.cfg

echo "id:3:initdefault:" > /etc/inittab 

echo "Changing timezone"
echo -e 'ZONE="Europe/Berlin"\nUTC=true\nCLOCKMODE=GMT' > /etc/sysconfig/clock
cp /usr/share/zoneinfo/Europe/Berlin /etc/localtime

echo "Bootstrap done"

