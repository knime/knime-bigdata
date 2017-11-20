#!/bin/bash
#
# Run this script as an EMR Bootstrap Action:
# Step type: Custom Action
# Script location: s3://knime-big-data/emr-install-knime-step-KNIME-3.3.sh
# Arguments: s3://knime-big-data/s3://knime-big-data/knosp_knime_3.3.1.linux.gtk.x86_64.tar.gz
#

export INSTALLATION_DIR="/mnt/"

KNIME_URL="$1"
[ -z "${KNIME_URL}" ] && { echo "You need to provide an URL with a KNIME AP archive. Accepted protocols: s3, http(s)" ; exit 1 ; }

command -v jq >/dev/null 2>&1 || { sudo yum install jq ; }

INFO_DIR="/mnt/var/lib/info/"
if [ "$(jq '.isMaster' < ${INFO_DIR}/instance.json)" = "true" ]; then
  echo "Found master node, updating jobserver's environment.conf"
  # modify jobserver environment.conf
else
  echo "Found slave node, installing KNIME from ${KNIME_URL}"

  pushd "${INSTALLATION_DIR}" >/dev/null
  
  echo "### Downloading ${KNIME_URL}"
  if [[ "${KNIME_URL}" =~ ^https?:// ]] ; then
    curl -O "${KNIME_URL}"
  elif [[ "${KNIME_URL}" =~ ^s3:// ]] ; then
    aws s3 cp "${KNIME_URL}" ./
  else
    echo "Unsupported URL scheme in ${KNIME_URL}. Exiting."
    exit 1
  fi
  echo
  
  archive="""$(basename "${KNIME_URL}")"""
  echo "### Extracting ${archive}"
  if [[ "${archive}" =~ \.zip$ ]] ; then
    knimedir="""$( unzip -q -l "${archive}" | head -3 | tail -1 | egrep -o "\S+$" | tr -d "/" )"""
    unzip "${archive}"
    [ $? != "0" ] && { echo "Failed to extract ${archive}. Exiting." ; exit 1 ; }
  elif [[ "${archive}" =~ \.tar\.gz$ ]] ; then
    knimedir="""$( tar tzf "${archive}" | head -1 | tr -d "/" )"""
    tar xzf "${archive}"
    [ $? != "0" ] && { echo "Failed to extract ${archive}. Exiting." ; exit 1 ; }
  elif [[ "${archive}" =~ \.tar\.bz2$ ]] ; then
    knimedir="""$( tar tjf "${archive}" | head -1 | tr -d "/" )"""
    tar xjf "${archive}"
    [ $? != "0" ] && { echo "Failed to extract ${archive}. Exiting." ; exit 1 ; }
  else
    echo "Unsupported archive format: ${archive}. Exiting."
    exit 1
  fi
  echo
  
  echo "### Fixing ownership and permissions in $(readlink -f ${knimedir})"
  # fix ownership and permissions
  sudo chown -R root:root "${knimedir}"
  sudo chmod -R go-w "${knimedir}"
  sudo chmod -R go+r "${knimedir}"
  sudo find "${knimedir}" -type d -exec chmod go+x '{}' \;
  sudo ln -s "${knimedir}" knime
  echo
  
  echo "Success"
  popd
fi
