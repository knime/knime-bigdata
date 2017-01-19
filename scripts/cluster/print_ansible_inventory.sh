#!/bin/bash


get_abs_script_dir() {
  pushd . >/dev/null
  cd "$(dirname $0)"
  local abs_script_dir="$(pwd)"
  popd  >/dev/null
  echo "${abs_script_dir}"
}
SCRIPT_DIR="$(get_abs_script_dir)"

source "${SCRIPT_DIR}/common/common-argparse.sh"

# ##############################
# Refresh cluster json metadata
# ##############################

"${SCRIPT_DIR}"/refresh-cluster-public-ips "${CNAME}" "${CENV}" "${COWNER}" >/dev/null || exit 1

echo "[${CNAME}]"
echo "master-${CNAME} ansible_host=$(<${MASTER_HOSTS})"
i=1
while read worker ; do
  echo "worker${i}-${CNAME} ansible_host=${worker}"
  i=$[i+1]
done < ${WORKER_HOSTS}
