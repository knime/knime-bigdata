#!/bin/bash
#
# Set REPOSITORY in ENV to overwrite default git repository URL
#
get_abs_script_dir() {
  pushd . >/dev/null
  cd "$(dirname $0)"
  local abs_script_dir="$(pwd)"
  popd  >/dev/null
  echo "${abs_script_dir}"
}
SCRIPT_DIR="$(get_abs_script_dir)"

MIN_ARGS=0  # Min number of args
MAX_ARGS=1  # Max number of args
USAGE="Usage: $(basename $0) [<env>]
  <env>   Name of the environment in buildfiles/env/ to build"


if [ $# -lt "$MIN_ARGS" ] ; then
  echo "${USAGE}"
  exit 1
fi

if [ $# -gt "$MAX_ARGS" ] ; then
  echo "${USAGE}"
  exit 1
fi

if [ $# = 1 ] ; then
  export SINGLE_ENV_TO_BUILD="$1"
  [ -d "$SCRIPT_DIR/buildfiles/env/$SINGLE_ENV_TO_BUILD" ] || { echo "Build environment $SINGLE_ENV_TO_BUILD does not exist. Exiting." ; exit 1 ; }
fi

WORK_DIR="$SCRIPT_DIR/target/"
mkdir -p "$WORK_DIR"

function finish {
    popd
}
trap finish EXIT

function do_build {
        ENV="$1"
        ENVDIR="$SCRIPT_DIR/buildfiles/env/$ENV/"
        echo "$ENVDIR/buildsettings.sh"

        # ensure the necessary files are in env dir
        [ -f "$ENVDIR/buildsettings.sh" ] || { echo "Environment ${ENV} does not provide a buildsettings.sh. Exiting." ; exit 1 ; }
        [ -f "$ENVDIR/runsettings.sh" ] || { echo "Environment ${ENV} does not provide a runsettings.sh. Exiting." ; exit 1 ; }
        BRANCH=$(find "$ENVDIR"/ -type f -iname "*.branch" -exec basename '{}' .branch \; )
        [ -n "$BRANCH" ] || { echo "Environment ${ENV} does specify a git branch to build. Exiting." ; exit 1 ; }
        CONF=$(find "$ENVDIR"/ -type f -iname "*.conf" -exec basename '{}' \; )
        [ -n "$CONF" ] || { echo "Environment ${ENV} does not set the environment.conf to use. Exiting." ; exit 1 ; }
        CONF="$SCRIPT_DIR/buildfiles/templates/$CONF.tmpl"
        [ -f "$CONF" ] || { echo "Environment ${ENV} references a template file ${CONF}.tmpl that does not exist. Exiting." ; exit 1 ; }

        # init JSLINKNAME and JSUSER
        JSLINKNAME=$(find "$ENVDIR"/ -type f -iname "*.jslinkname" -exec basename '{}' .jslinkname \; )
        JSLINKNAME=${JSLINKNAME:-spark-job-server}
        JSUSER=$(find "$ENVDIR"/ -type f -iname "*.jsuser" -exec basename '{}' .jsuser \; )
        JSUSER=${JSUSER:-spark-job-server}

        # copy buildsettings and make dummy .conf
        cp "$ENVDIR/buildsettings.sh" "config/$ENV.sh"
        touch "config/$ENV.conf" # create dummy conf

        # switch to branch
        echo "Building environment $ENV with branch $BRANCH"
        git checkout "$BRANCH" || return 1

        # build
        bin/server_package.sh "$ENV" || return 1

        # unpack .tar.gz so we can modify
        tar xzf "spark-job-server-$ENV.tar.gz"

        # runsetting.sh
        cp "$ENVDIR/runsettings.sh" "spark-job-server-$ENV/settings.sh" # copy runsettings.sh
        sed -i "s/%JSLINKNAME%/${JSLINKNAME}/g" "spark-job-server-$ENV/settings.sh"
        sed -i "s/%JSUSER%/${JSUSER}/g" "spark-job-server-$ENV/settings.sh"

        # environment.conf
        rm "spark-job-server-$ENV/$ENV.conf" # delete dummy conf
        cp "${CONF}" "spark-job-server-$ENV/environment.conf"
        sed -i "s/%JSLINKNAME%/${JSLINKNAME}/g" "spark-job-server-$ENV/environment.conf"
        sed -i "s/%JSUSER%/${JSUSER}/g" "spark-job-server-$ENV/environment.conf"

        # shiro.ini templates
        cp ./config/shiro.ini.*.template "spark-job-server-$ENV/"

        # build.version
        echo "Commit $(git show --format='format:%H' | head -1) on branch $BRANCH built on $(date --rfc-3339=seconds)" >"spark-job-server-$ENV/build.version"

        # spark-job-server-init.d
        if [ -f "${ENVDIR}/spark-job-server-init.d" ] ; then
          cp "${ENVDIR}/spark-job-server-init.d" "spark-job-server-$ENV/"
        else
          cp "$SCRIPT_DIR/buildfiles/templates/spark-job-server-init.d" "spark-job-server-$ENV/"
        fi
        sed -i "s/%JSLINKNAME%/${JSLINKNAME}/g" "spark-job-server-$ENV/spark-job-server-init.d"
        sed -i "s/%JSUSER%/${JSUSER}/g" "spark-job-server-$ENV/spark-job-server-init.d"

        # README and LICENSE
        cp "$SCRIPT_DIR/buildfiles/LICENSE" "spark-job-server-$ENV/"
        cp "$SCRIPT_DIR/buildfiles/README" "spark-job-server-$ENV/"

        # pack it up again
        tar czf ../"spark-job-server-$ENV.tar.gz" --owner root --group root "spark-job-server-$ENV/"
        rm -Rf "spark-job-server-$ENV/"

        if [ -f "$ENVDIR/publish-api" ] ; then
          git apply "${SCRIPT_DIR}/buildfiles/jobserverapi-publish-local.patch" || { echo "Failed to apply publish patch. Exiting." ; exit 1 ; }
          sbt job-server-api/publish
          git reset --hard
        fi
        sbt clean
}


if [ -z "$REPOSITORY" ]; then
  REPOSITORY="git@github.com:bjoernlohrmann/spark-jobserver.git"
  # REPOSITORY="git@github.com:dersascha/spark-jobserver.git"
  # REPOSITORY="/home/bjoern/knime/bigdata/spark-jobserver"
fi
echo "Using repository: $REPOSITORY"

pushd ${WORK_DIR} >/dev/null
echo "Using directory ${WORK_DIR}. Please delete after build."
git clone $REPOSITORY || { echo "Failed to clone git repository. Exiting." ; exit 1 ; }
cd spark-jobserver

if [ -n "$SINGLE_ENV_TO_BUILD" ] ; then
  echo "Building environment $SINGLE_ENV_TO_BUILD"
  do_build "$SINGLE_ENV_TO_BUILD"  >"${WORK_DIR}/spark-job-server-${SINGLE_ENV_TO_BUILD}.buildlog" 2>&1
else
  find "$SCRIPT_DIR/buildfiles/env/" -maxdepth 1 -type d ! -name env -exec basename "{}" \; | while read ENVNAME ; do
    echo "Building environment $ENVNAME"
    do_build "$ENVNAME"  >"${WORK_DIR}/spark-job-server-${ENVNAME}.buildlog" 2>&1
  done
fi
