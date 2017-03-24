#!/bin/bash

# This script builds a KNIME AP installation file (tar.gz) for 64bit Linux,
# that has a the Big Data Extension as well as a configurable list of features
# installed. and includes a license (for big data extensions).
#
# You can use this script to prepare the KNIME installation on Hadoop cluster nodes
# that are supposed to run KNIME-on-Spark.
#


# comma separated list of features to install (no whitespaces, each feature must end with "".feature.group")
ADDITIONAL_FEATURES="org.knime.features.ext.textprocessing.feature.group"

MIN_ARGS=0  # Min number of args
MAX_ARGS=4  # Max number of args
USAGE="Usage: $(basename $0) [--version <knime-version>|--basefiles <folder>] [--license <licensefile>] [--help]
  --version           A KNIME Analytics Platform release to use as a starting point, e.g. 3.3.1
                      In this case KNIME Analytics Platform, the KNIME Big Data Extensions and the features
                      to install will be downloaded from the KNIME website
  --basefiles         A folder that contains all of the following files:
                          1. KNIME Analytics Platform (pattern: knime_X.Y.Z.linux.gtk.x86_64.tar.gz)
                          2. KNIME Analytics Platform Update site (pattern: UpdateSite_latestXY.zip)
                          3. KNIME Store Update Site (pattern: UpdateSite_Store_latestXY.zip)
                          4. KNIME-on-Spark Patch Update Site (pattern: KNIMEonSpark_latestXY.zip)
  --license           License file to include, that contains licenses for KNIME Big Data Extensions.
  --help              Print this help.

You have to specify exactly one of --version, --basefiles or --help.

This script builds a KNIME AP installation file (tar.gz) for 64bit Linux, that has a the KNIME Spark Executor
as well as a configurable list of features installed and possibly includes a license.

You can use this script for example to prepare the KNIME installation on Hadoop cluster nodes that are supposed
to run KNIME-on-Spark.
"

[ $# -lt "$MIN_ARGS" ] && { echo "Not enough arguments provided." ; echo "${USAGE}" ; exit 1 ; }
[ $# -gt "$MAX_ARGS" ] && { echo "Too many arguments provided." ; echo "${USAGE}" ; exit 1 ; }

DO=""
HELP=""
VERSION=""
BASEFILES=""
LICENSE=""

for var in "$@" ; do
    if [ "$var" = "--version" ] ; then
      DO="version"
    elif [ "$var" = "--basefiles" ] ; then
      DO="basefiles"
    elif [ "$var" = "--help" ] ; then
      echo "${USAGE}" ; exit 1
    elif [ "$var" = "--license" ] ; then
      DO="license"
    else
      if [ "$DO" = "version" ] ; then
        VERSION="$var"
      elif [ "$DO" = "basefiles" ] ; then
        BASEFILES="$var"
      elif [ "$DO" = "license" ] ; then
       LICENSE="$var"
      else
	echo "Could not parse arguments."
        echo "${USAGE}"
        exit 1
      fi
      DO=""
    fi
done

# sanity checking
[ -z "${VERSION}" -a -z "${BASEFILES}"  ] && { echo "You have to use either --version or --basefiles." ; echo "${USAGE}" ; exit 1 ; }
[ -n "${VERSION}" -a -n "${BASEFILES}"  ] && { echo "You can only use either --version or --basefiles." ; echo "${USAGE}" ; exit 1 ; }
[ -n "${LICENSE}" -a ! -r "${LICENSE}" ] && { echo "License file ${LICENSE} not readable." ; exit 1 ; }
if [ -n "${VERSION}" ] ; then
  [[ ! "${VERSION}" =~ [0-9]+\.[0-9]\.[0-9]+ ]] && { echo "Invalid KNIME version: ${VERSION}" ; echo "${USAGE}" ; exit 1 ; }
fi
[ -n "${BASEFILES}" -a ! -d "${BASEFILES}" ] &&  { echo "Invalid basefiles directory: ${BASEFILES}" ; echo "${USAGE}" ; exit 1 ; }

# create a temp dir for basefiles if not set
if [ -z "${BASEFILES}" ] ; then
  BASEFILES="$( mktemp -d -p ./ basefiles.XXX )"
fi

# here we set
#  AP_FILE (absolute path of .tar.gz)
#  UPDATE_URL (URL pointing to update site)
#  STORE_URL (URL pointing to store update site)
#  KNOSP_URL (URL pointing to KNIME-on-Spark update site .zip)
pushd "${BASEFILES}" >/dev/null
if [ -n "${VERSION}" ] ; then
  # download knime and set update site as well as store update site URL
  MAJOR_VERSION="""$( echo $VERSION | cut -d . -f 1-2 )"""
  
  AP_URL="https://download.knime.org/analytics-platform/linux/knime_${VERSION}.linux.gtk.x86_64.tar.gz"
  UPDATE_URL="http://update.knime.org/analytics-platform/${MAJOR_VERSION}/"
  STORE_URL="http://update.knime.org/store/${MAJOR_VERSION}/"

  wget "${AP_URL}"
  [ $? != 0 ] && { echo "Failed to download KNIME Analytics Platform from ${AP_URL}" ; exit 1 ; }
  AP_FILE="$(basename ${AP_URL})"
  
  knosp_dl_url="""https://download.knime.org/store/3.3/KNIMEonSpark_latest$( echo "${MAJOR_VERSION}" | tr -d . ).zip"""
  wget "${knosp_dl_url}"
  [ $? != 0 ] && { echo "Failed to download KNIME-on-Spark update site from ${knosp_dl_url}" ; exit 1 ; }
  KNOSP_URL="""jar:file://$(readlink -f $(basename "${knosp_dl_url}"))!/"""
else
  # Resolve file names and set update site as well as store update site URL
  AP_FILE=$(ls *.tar.gz | egrep -o 'knime_[0-9]+\.[0-9]+\.[0-9]+\.linux\.gtk\.x86_64\.tar\.gz')
  [ -z ${AP_FILE} ] && { echo "Could not find KNIME AP .tar.gz for 64bit Linux in ${BASEFILES}" ; exit 1 ; }
  [[ ! ${AP_FILE} =~ ^knime_[0-9]+\.[0-9]+\.[0-9]+\.linux\.gtk\.x86_64\.tar\.gz$ ]] && { echo "Found ${AP_FILE} in ${BASEFILES}. Please remove all but one file." ; exit 1 ; }

  UPDATE_ZIP=$(ls *.zip | egrep -o 'UpdateSite_latest[0-9]+.zip')
  [ -z ${UPDATE_ZIP} ] && { echo "Could not find KNIME Update Site (.zip) in ${BASEFILES}" ; exit 1 ; }
  [[ ! ${UPDATE_ZIP} =~ ^UpdateSite_latest[0-9]+.zip$ ]] && { echo "Found ${UPDATE_ZIP} in ${BASEFILES}. Please remove all but one file." ; exit 1 ; }
  UPDATE_URL="jar:file://$(readlink -f ${UPDATE_ZIP})!/"

  STORE_ZIP=$(ls *.zip | egrep -o 'UpdateSite_Store_latest[0-9]+.zip')
  [ -z ${STORE_ZIP} ] && { echo "Could not find KNIME Store Update Site (.zip) in ${BASEFILES}" ; exit 1 ; }
  [[ ! ${STORE_ZIP} =~ ^UpdateSite_Store_latest[0-9]+.zip$ ]] && { echo "Found ${STORE_ZIP} in ${BASEFILES}. Please remove all but one file." ; exit 1 ; }
  STORE_URL="jar:file://$(readlink -f ${STORE_ZIP})!/"

  KNOSP_ZIP=$(ls *.zip | egrep -o 'KNIMEonSpark_latest[0-9]+.zip')
  [ -z ${KNOSP_ZIP} ] && { echo "Could not find KNIME-on-Spark Update Site (.zip) in ${BASEFILES}" ; exit 1 ; }
  [[ ! ${KNOSP_ZIP} =~ ^KNIMEonSpark_latest[0-9]+.zip$ ]] && { echo "Found ${KNOSP_ZIP} in ${BASEFILES}. Please remove all but one file." ; exit 1 ; }
  KNOSP_URL="""jar:file://$(readlink -f "${KNOSP_ZIP}")!/"""
fi
AP_FILE="$(readlink -f ${AP_FILE})"
popd >/dev/null

echo "##### Assembling KNIME build with KNIME-on-Spark from these files:"
echo "      KNIME release build: ${AP_FILE}"
echo "      KNIME Update Site: ${UPDATE_URL}"
echo "      KNIME Store Update Site: ${STORE_URL}"
echo "      KNIME-on-Spark Update Site: ${KNOSP_URL}"
echo

KNIMEDIR="""$( tar tzf "${AP_FILE}" | head -1 | tr -d "/" )"""
echo "#### Extracting KNIME release build ${AP_FILE} to current working directory"
tar xzf "${AP_FILE}"
echo "-Djava.awt.headless=true" >>"${KNIMEDIR}/knime.ini"
echo

pushd "${KNIMEDIR}" >/dev/null
echo "#### Installing KNIME Spark Executor and streaming extensions"
./knime -data workspace -nosplash --launcher.suppressErrors -application org.eclipse.equinox.p2.director \
 -repository "${UPDATE_URL}" \
 -repository "${STORE_URL}" \
 -installIU 'com.knime.features.bigdata.spark.feature.group,org.knime.features.core.streaming.feature.group'

[ $? != 0 ] && { echo "Failure. Exiting." ; exit 1 ; }
echo

if [ -n "${ADDITIONAL_FEATURES}" ] ; then
  echo "#### Installing additional features"
  ./knime -data workspace -nosplash --launcher.suppressErrors -application org.eclipse.equinox.p2.director \
   -repository "${UPDATE_URL}" \
   -repository "${STORE_URL}" \
   -installIU "${ADDITIONAL_FEATURES}"

 [ $? != 0 ] && { echo "Failure. Exiting." ; exit 1 ; }
fi
echo

echo "#### Installing KNIME-on-Spark feature patches"
./knime -data workspace -nosplash --launcher.suppressErrors -application org.eclipse.equinox.p2.director \
 -repository "${KNOSP_URL}" \
 -installIU "org.knime.features.corepatch.feature.group,org.knime.features.corestreamingpatch.feature.group,org.knime.features.sparkpatch.feature.group"
[ $? != 0 ] && { echo "Failure. Exiting." ; exit 1 ; }
echo

popd >/dev/null

if [ -n "${LICENSE}" ] ; then
  echo "#### Copying ${LICENSE}"
  cp "${LICENSE}" "${KNIMEDIR}/licenses/"
  echo
fi

echo "#### Creating tar file with KNIME-on-Spark"
KNOSP_AP_FILE="$(readlink -f knosp_$(basename ${AP_FILE}))"
tar czf "${KNOSP_AP_FILE}" "${KNIMEDIR}"
[ $? != 0 ] && { echo "Failure. Exiting." ; exit 1 ; }
rm -Rf "${KNIMEDIR}"
echo "Successfully created ${KNOSP_AP_FILE}"
