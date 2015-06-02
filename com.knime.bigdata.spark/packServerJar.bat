cp lib\knimeSparkScalaClient.jar resources\knimeJobs.jar
cd bin

jar uvf ..\resources\knimeJobs.jar com\knime\bigdata\spark\jobserver\jobs\*.class com\knime\bigdata\spark\jobserver\server\*.class com\knime\bigdata\spark\jobserver\server\transformation\*.class

cd ..

jar uvf resources\knimeJobs.jar conf\hortonworks\hive\hive-site.xml