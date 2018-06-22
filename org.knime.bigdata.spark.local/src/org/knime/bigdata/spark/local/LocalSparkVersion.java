package org.knime.bigdata.spark.local;

import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark2_3.api.Spark_2_3_CompatibilityChecker;

/**
 * Provides all constants with respect to the Spark version supported by local
 * Spark in a central location. These constants will be adjusted whenever local
 * Spark is updated to a new Spark version.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LocalSparkVersion {

	/**
	 * The current and only Spark version supported by local Spark.
	 */
	public final static SparkVersion SUPPORTED_SPARK_VERSION = SparkVersion.V_2_3;

	/**
	 * A compatibility checker for the Spark version currently supported by
	 * local Spark.
	 */
	public final static CompatibilityChecker VERSION_CHECKER = Spark_2_3_CompatibilityChecker.INSTANCE;

}
