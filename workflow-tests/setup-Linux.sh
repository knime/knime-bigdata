#!/bin/sh
export FLOWVARS=${WORKSPACE}/workflow-tests/flowvariables-local-bde.csv

bd_temp_dir="${TEMP}/testing-workspace/tmp/bigdata-tests"
bd_temp_dir_hive="${TEMP}/testing-workspace/tmp/bigdata-tests/hive"

rm -rf "${bd_temp_dir}"
mkdir -p "${bd_temp_dir}"
sedi "s|tmp.local.parent,.*\$|tmp.local.parent,${bd_temp_dir}|g" "${FLOWVARS}"
sedi "s|tmp.remote.parent,.*\$|tmp.remote.parent,${bd_temp_dir}|g" "${FLOWVARS}"

mkdir "${bd_temp_dir_hive}"
sedi "s|spark.local.hiveDataFolder,.*\$|spark.local.hiveDataFolder,${bd_temp_dir_hive}|g" "${FLOWVARS}"

echo "------- flowvariables.csv -------"
cat $FLOWVARS
echo "------- flowvariables.csv -------"
