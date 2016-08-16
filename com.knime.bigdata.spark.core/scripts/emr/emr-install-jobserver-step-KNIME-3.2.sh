#!/bin/sh
#
# Run this script as additional EMR step:
# Step type: Custom JAR
# JAR location: s3://elasticmapreduce/libs/script-runner/script-runner.jar
# Arguments: s3://knime-big-data/emr-install-jobserver-step-KNIME-3.2.sh
#
INFO_DIR="/mnt/var/lib/info/"
KNIME_VERSION="3.2"
INSTALL_SCRIPT_URL="https://download.knime.org/store/${KNIME_VERSION}/install-jobserver-emr.sh"

command -v jq >/dev/null 2>&1 || { sudo yum install -y jq ; }

if [ "$(jq '.isMaster' < ${INFO_DIR}/instance.json)" = "true" ]; then
  echo "Found master node, running job server install script..."
  curl -s "${INSTALL_SCRIPT_URL}" | sudo /bin/sh
else
  echo "Found slave node, no job server installation required."
fi
