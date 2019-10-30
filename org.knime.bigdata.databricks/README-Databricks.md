# A note about Databricks REST APIs used in this implementation

The node uses a mix of the REST 1.2 and 2.0 API:

  * REST 1.2: [context](src/org/knime/bigdata/databricks/rest/contexts/ContextsAPI.java) and [commands](src/org/knime/bigdata/databricks/rest/commands/CommandsAPI.java)
  * REST 2.0: [cluster](src/org/knime/bigdata/databricks/rest/clusters/ClusterAPI.java), [libraries](src/org/knime/bigdata/databricks/rest/libraries/LibrariesAPI.java) and [DBFS](src/org/knime/bigdata/databricks/rest/dbfs/DBFSAPI.java)

The REST 2.0 API contains a job API to execute Spark jobs.
Each job spawns a Spark execution context/REPL.
This takes ~8s to launch a job and eats a lot of memory.
A driver on a 8GB VM can only handle 3-4 jobs in parallel and then crashes with an out of memory exception.
This makes the REST 2.0 jobs API very impractical for interactive programs like KNIME.

The REST 1.2 API contains a Spark execution context API that works like a remote Spark REPL.
The Create Databricks Environment Node uses an uniq Spark context ID in KNIME (like the Livy node) and creates such a Spark execution context for each of them on the Databricks cluster.
Launching a Spark execution context takes the same ~8s and memory, but executing jobs only takes a very short time and the context can handle many jobs in parallel.
The context gets destroyed on reset or dispose.

The REST 2.0 API contains DBFS, cluster and library APIs that are not available (anymore) in 1.2 API.


# A note about Databricks cluster auto start feature and the JDBC connection

Each invocation of the HTTP/JDBC connection initiates a cluster start!

  1. The node should [not restore](src/org/knime/bigdata/spark/core/databricks/node/create/DatabricksSparkContextCreatorNodeModel.java#lines-365) the DB session from a saved workflow.
  2. The DB connection should not send a session termination on [close](src/org/knime/bigdata/database/databricks/DatabricksDBConnectionWrapper.java#lines-93).
