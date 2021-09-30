#!/bin/sh
export FLOWVARS=${WORKSPACE}/workflow-tests/flowvariables-local-bde.csv

bd_temp_dir="${TEMP}/testing-workspace/tmp/bigdata-tests"
mkdir -p "${bd_temp_dir}"
sedi "s|tmp.local.parent,.*\$|tmp.local.parent,${bd_temp_dir}|g" "${FLOWVARS}"
sedi "s|tmp.remote.parent,.*\$|tmp.remote.parent,${bd_temp_dir}|g" "${FLOWVARS}"

echo "------- flowvariables.csv -------"
cat $FLOWVARS
echo "------- flowvariables.csv -------"
