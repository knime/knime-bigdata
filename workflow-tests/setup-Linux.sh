export FLOWVARS=${WORKSPACE}/workflow-tests/flowvariables-local-bde.csv

mkdir -p /tmp/bigdata-tests/
tar -xf ${WORKSPACE}/workflow-tests/local-bde-test-data.tbz2 --directory /tmp/bigdata-tests