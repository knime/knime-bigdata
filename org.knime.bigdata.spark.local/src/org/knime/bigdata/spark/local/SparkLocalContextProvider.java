/**
 * 
 */
package org.knime.bigdata.spark.local;

import java.net.URI;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;

/**
 * Spark context provider that provides local Spark.
 * 
 * @author Oleg Yasnev, KNIME GmbH
 */
public class SparkLocalContextProvider implements SparkContextProvider<LocalSparkContextConfig> {

    public static final String LOCAL_SPARK_CONTEXT_ID_SCHEME = "localSpark";


    /**
     * {@inheritDoc}
     */
    @Override
    public CompatibilityChecker getChecker() {
        return LocalSparkVersion.VERSION_CHECKER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(SparkVersion sparkVersion) {
        return getChecker().supportSpark(sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return getChecker().getSupportedSparkVersions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContext<LocalSparkContextConfig> createContext(SparkContextID contextID) {
        return new LocalSparkContext(contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSupportedScheme() {
        return LOCAL_SPARK_CONTEXT_ID_SCHEME;
    }

    /**
     * {@inheritDoc}
     */
	@Override
	public String toPrettyString(SparkContextID contextID) {
		URI uri = URI.create(contextID.toString());

		if (!uri.getScheme().equals(LOCAL_SPARK_CONTEXT_ID_SCHEME)) {
			throw new IllegalArgumentException("Unspported scheme: " + contextID.getScheme());
		}

		return String.format(String.format("Local Spark Context (%s)", uri.getHost()));
	}
}
