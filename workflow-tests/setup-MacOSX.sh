export FLOWVARS=${WORKSPACE}/workflow-tests/flowvariables-local-bde.csv

rm -rf /tmp/bigdata-tests
mkdir -p /tmp/bigdata-tests/
tar -xf ${WORKSPACE}/workflow-tests/local-bde-test-data.tbz2 --directory /tmp/bigdata-tests