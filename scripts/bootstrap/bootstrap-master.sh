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

echo 'wget -O - "https://www.goip.de/setip?username=dhIfxV2hFHaNSCC&password=bK8TkfbM3Mes2wb&subdomain=demo-master.goip.de" >/dev/null 2>&1' >>/etc/rc.local
