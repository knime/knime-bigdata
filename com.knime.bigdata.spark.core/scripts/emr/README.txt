These scripts are part of the Amazon EMR support of KNIME Spark Executor. They provide automated
jobserver installation when spinning up an EMR cluster.

These scripts are NOT meant to be shipped with the KNIME Spark Executor extensions, 
thus they should not be part of the plugins created by jenkins builds!

To deploy them, they need to be uploaded to http://download.knime.com/store/3.2 (or similar), 
from where they are fetched during the EMR cluster installation process.

