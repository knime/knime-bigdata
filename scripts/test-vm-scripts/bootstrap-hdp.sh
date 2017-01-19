#!/bin/sh
#
# Disable all big data services on hdp
#
# Run this script from guestfish:
#   guestfish -a test-vm.vmdk -i
#       > copy-in .../bootstrap-hdp.sh /tmp/
#       > sh "sh /tmp/bootstrap-hdp.sh"
#
[ -f /etc/init.d/startup_script ] && chkconfig startup_script off
[ -f /etc/init.d/ambari-agent ] && chkconfig ambari-agent off
[ -f /etc/init.d/ambari-server ] && chkconfig ambari-server off
[ -f /etc/init.d/hst ] && chkconfig hst off
[ -f /etc/init.d/httpd ] && chkconfig httpd off
[ -f /etc/init.d/shellinaboxd ] && chkconfig shellinaboxd off
[ -f /etc/systemd/system/multi-user.target.wants/sandbox.service ] && rm -f  /etc/systemd/system/multi-user.target.wants/sandbox.service

echo "Check /etc/rc3.d !"
