#!/bin/bash
#
# Set REPOSITORY in ENV to overwrite default git repository URL
#
MIN_ARGS=0  # Min number of args
MAX_ARGS=1  # Max number of args
USAGE="Usage: $(basename $0) [<env>]
  <env>   Name of the environment in buildfiles/env/ to build"

if [ -z "$REPOSITORY" ]; then
  REPOSITORY="git@github.com:bjoernlohrmann/spark-jobserver.git"
  # REPOSITORY="git@github.com:dersascha/spark-jobserver.git"
  # REPOSITORY="/home/bjoern/knime/bigdata/spark-jobserver"
fi

echo "Using repository: $REPOSITORY"

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
fi

get_abs_script_dir() {
  pushd . >/dev/null
  cd "$(dirname $0)"
  local abs_script_dir="$(pwd)"
  popd  >/dev/null
  echo "${abs_script_dir}"
}
SCRIPT_DIR="$(get_abs_script_dir)"

WORK_DIR="$SCRIPT_DIR/target/"
mkdir -p "$WORK_DIR"

function finish {
    popd
}
trap finish EXIT

function do_build {
        ENV="$1"

        # copy config files
        ENVDIR="$SCRIPT_DIR/buildfiles/env/$ENV/"
        cp "$ENVDIR/buildsettings.sh" "config/$ENV.sh"
        touch "config/$ENV.conf" # create dummy conf

        # switch to branch
        BRANCH=$(find "$SCRIPT_DIR/buildfiles/env/$ENV/" -type f -iname "*.branch" -exec basename '{}' .branch \; )
        echo "Building environment $ENV with branch $BRANCH"
        git checkout "$BRANCH" || return 1

        # build
        bin/server_package.sh "$ENV" || return 1

        # modify tar.gz file: add build version file as well as init.d script and set owner/group of all files to root
        tar xzf "spark-job-server-$ENV.tar.gz"
        cp "$ENVDIR/runsettings.sh" "spark-job-server-$ENV/settings.sh" # copy runsettings.sh
        rm "spark-job-server-$ENV/$ENV.conf" # delete dummy conf
        cp "$SCRIPT_DIR/buildfiles/environment.conf" "spark-job-server-$ENV/" # copy real conf
        cp ./config/shiro.ini.*.template "spark-job-server-$ENV/" # copy shiro.ini templates
        echo "Commit $(git show --format='format:%H' | head -1) on branch $BRANCH built on $(date --rfc-3339=seconds)" >"spark-job-server-$ENV/build.version"
        cp "$SCRIPT_DIR/buildfiles/spark-job-server-init.d" "spark-job-server-$ENV/"
        cp "$SCRIPT_DIR/buildfiles/LICENSE" "spark-job-server-$ENV/"
        cp "$SCRIPT_DIR/buildfiles/README" "spark-job-server-$ENV/"
        tar czf ../"spark-job-server-$ENV.tar.gz" --owner root --group root "spark-job-server-$ENV/"
        rm -Rf "spark-job-server-$ENV/"

        if [ -f "$SCRIPT_DIR/buildfiles/env/$ENV/publish-api" ] ; then
          git apply "${SCRIPT_DIR}/buildfiles/jobserverapi-publish-local.patch" || { echo "Failed to apply publish patch. Exiting." ; exit 1 ; }
          sbt job-server-api/publish
          git reset --hard
        fi
        sbt clean
}

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
