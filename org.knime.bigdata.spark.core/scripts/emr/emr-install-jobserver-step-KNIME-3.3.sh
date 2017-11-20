#!/bin/sh
#
# Run this script as additional EMR step:
# Step type: Custom JAR
# JAR location: s3://elasticmapreduce/libs/script-runner/script-runner.jar
# Arguments: s3://knime-big-data/emr-install-jobserver-step-KNIME-3.3.sh [optional-conf-tag]

INFO_DIR="/mnt/var/lib/info/"
export KNIME_VERSION="3.3"
export JS_VERSION="0.6.2.1-KNIME_emr-4.8.3"
export CONF_TAG="$1"
INSTALL_SCRIPT_URL="https://download.knime.org/store/${KNIME_VERSION}/install-jobserver-emr.sh"

command -v jq >/dev/null 2>&1 || { sudo yum install jq ; }

if [ "$(jq '.isMaster' < ${INFO_DIR}/instance.json)" = "true" ]; then
  echo "Found master node, running job server install script..."
  curl -s "${INSTALL_SCRIPT_URL}" | sudo "KNIME_VERSION=${KNIME_VERSION}" "JS_VERSION=${JS_VERSION}" "CONF_TAG=${CONF_TAG}" /bin/sh
else
  echo "Found slave node, no job server installation required."
fi
