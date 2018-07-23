package org.knime.bigdata.spark.local.wrapper;

import java.util.Map;
import java.util.Set;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.local.context.LocalSparkSerializationUtil;

/**
 * Interface to be used on the OSGI-side of local Spark to manage the lifecycle and access a local Spark context.
 * 
 * Instances of this class cannot be anymore after invoking {@link #destroy()}.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public interface LocalSparkWrapper {

    /**
     * Starts a new local Spark context.
     * 
     * @param name A name for the Spark context for display purposes.
     * @param workerThreads The number of worker threads to use in Spark.
     * @param sparkConf A map with key-value pairs to inject into the actual SparkConf.
     * @param enableHiveSupport Whether to support HiveQL or just SparkSQL.
     * @param startThriftserver Whether to start Spark Thriftserver or not.
     * @param thriftserverPort The TCP port on which Spark Thriftserver shall listen for JDBC connections. May be -1,
     *            which results in the port being chosen randomly if necessary.
     * @param hiveDataFolder Where to store the MetastoreDB and warehouse of Spark Thriftserver. If null, a temporary
     *            location will be chosen ad-hoc.
     * @throws KNIMESparkException if something goes wrong while creating the Spark context.
     */
    void openSparkContext(String name, int workerThreads, Map<String, String> sparkConf, boolean enableHiveSupport,
        boolean startThriftserver, int thriftserverPort, String hiveDataFolder) throws KNIMESparkException;

    /**
     * Shuts the Spark context down and releases the resources it holds.
     * 
     * @throws KNIMESparkException if something goes wrong while destroying the Spark context.
     */
    void destroy() throws KNIMESparkException;

    /**
     * Runs a job described by the given map. Both the ingoing as well as the resulting map are assumed to be the output
     * of {@link LocalSparkSerializationUtil#serializeToPlainJavaTypes(Map)}.
     * 
     * @param localSparkInputMap The serialized job input.
     * @return The serialized job output.
     */
    Map<String, Object> runJob(Map<String, Object> localSparkInputMap);

	/**
	 * @return a set of IDs of all named objects currently held in the local Spark context.
	 */
    Set<String> getNamedObjects();

    /**
     * Deletes the named objects for the given IDs.
     * 
     * @param namedObjects The IDs of the named objects to delete.
     */
	void deleteNamedObjects(final Set<String> namedObjects);

	/**
	 * 
	 * @return The URL of the Spark WebUI represented as a String.
	 */
	String getSparkWebUIUrl();
	
    /**
     * 
     * @return the TCP port that Hiveserver2 (i.e. Spark Thriftserver) is listening on, or -1 if it has not been
     *         started.
     */
	int getHiveserverPort();
}