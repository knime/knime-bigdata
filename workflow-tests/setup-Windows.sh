export FLOWVARS=$(path ${WORKSPACE}/workflow-tests/flowvariables-local-bde.csv)

rm -rf /cygdrive/c/tmp/bigdata-tests
mkdir -p /cygdrive/c/tmp/bigdata-tests/
tar -xf $(cygpath -u "${WORKSPACE}/workflow-tests/local-bde-test-data.tbz2") --directory /cygdrive/c/tmp/bigdata-tests
