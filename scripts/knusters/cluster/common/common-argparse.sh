#
# Common argument parsing code
#

source "${SCRIPT_DIR}/common/common.sh"

MIN_ARGS=0  # Min number of args
MAX_ARGS=3  # Max number of args
USAGE="Usage: $(basename $0) [<name> [<environment> [<owner>]]]
  <name>         Cluster name, defaults to $DEFAULT_CNAME.
  <environment>  Cluster environment, defaults to $DEFAULT_CENV.
  <owner>        Cluster owner, defaults to $DEFAULT_COWNER."

if [ $# -lt "$MIN_ARGS" ]
then
  echo "${USAGE}"
  exit $E_BADARGS
fi

if [ $# -gt "$MAX_ARGS" ]
then
  echo "${USAGE}"
  exit $E_BADARGS
fi

CNAME=${1-$DEFAULT_CNAME}
CENV=${2-$DEFAULT_CENV}
COWNER=${3-$DEFAULT_COWNER}

define_locations "${CNAME}" "${CENV}" "${COWNER}"

