#!/bin/sh
export FLOWVARS=${WORKSPACE}/workflow-tests/flowvariables-local-bde.csv
export LOCAL_BDE=true

bd_temp_dir="${TEMP}/testing-workspace/tmp/bigdata-tests"
rm -rf "${bd_temp_dir}"
mkdir -p "${bd_temp_dir}"
sedi "s|hiveDataFolder,.*\$|hiveDataFolder,${bd_temp_dir}/hive|g" "${FLOWVARS}"

tar -xf "${WORKSPACE}/workflow-tests/local-bde-test-data.tbz2" --directory "${TEMP}/testing-workspace/tmp/bigdata-tests"
