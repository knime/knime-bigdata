#!/bin/bash

resize2fs /dev/xvda1

if [ "$(ping -q -c1 google.com)" ] ; then
	yum install -y epel-release
	yum install -y python-pip wget nano vim libgfortran
	pip install awscli
	yum install -y ntp
	chkconfig --level 345 ntpd on
	/etc/init.d/ntpdate start
	service ntpd start
else
	echo "No internet connection at bootstrap time" >> /root/bootstrap.log
fi
