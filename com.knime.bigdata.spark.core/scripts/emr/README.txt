These scripts are part of the Amazon EMR support of KNIME Spark Executor. They provide:

- automated jobserver installation when spinning up an EMR cluster.
- automated KNIME installation on EMR workers (for KNIME-on-Spark)

These scripts are NOT meant to be shipped with the KNIME Spark Executor extensions, 
thus they should not be part of the plugins created by jenkins builds!

To deploy them, they need to be uploaded to http://download.knime.com/store/3.3 (or similar), 
from where they are fetched during the EMR cluster installation process.

Also take a look at the knime-cloud-deployment repository where you find AWS
CloudFormation templates that put the pieces together to create an EMR cluster.
