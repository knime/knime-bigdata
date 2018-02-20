/**
 *
 */
package org.knime.bigdata.spark.core.sparkjobserver;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.sparkjobserver.context.JobserverSparkContext;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Spark context provider that provides local Spark.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class JobserverSparkContextProvider implements SparkContextProvider<JobServerSparkContextConfig> {

    /**
     * Scheme for Spark Jobserver-specific {@link SparkContextID}s.
     */
    public static final String JOBSERVER_SPARK_CONTEXT_ID_SCHEME = "jobserver";

    /**
     * {@inheritDoc}
     */
    @Override
    public CompatibilityChecker getChecker() {
        return AllVersionCompatibilityChecker.INSTANCE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
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
    public SparkContext<JobServerSparkContextConfig> createContext(final SparkContextID contextID) {
        return new JobserverSparkContext(contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSupportedScheme() {
        return JOBSERVER_SPARK_CONTEXT_ID_SCHEME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toPrettyString(final SparkContextID contextID) {
        URI uri = URI.create(contextID.toString());

        if (!uri.getScheme().equals(JOBSERVER_SPARK_CONTEXT_ID_SCHEME)) {
            throw new IllegalArgumentException("Unspported scheme: " + contextID.getScheme());
        }

        StringBuilder b = new StringBuilder();
        b.append("Spark Jobserver Context ");
        b.append(String.format("(Host and Port: %s:%d, ", uri.getHost(), uri.getPort()));
        b.append(String.format("Context Name: %s)", uri.getPath().substring(1)));
        return b.toString();
    }

    /**
     * {@inheritDoc}
     */
	@Override
	public Optional<SparkContext<JobServerSparkContextConfig>> createDefaultSparkContextIfPossible() {
		// this implementation currently always creates a context, because this
		// is the only context provider that can provide the default Spark
		// context.

		final JobServerSparkContextConfig defaultConfig = new JobServerSparkContextConfig();
		final SparkContext<JobServerSparkContextConfig> defaultSparkContext = new JobserverSparkContext(
				defaultConfig.getSparkContextID());
		defaultSparkContext.ensureConfigured(defaultConfig, true);
		return Optional.of(defaultSparkContext);
	}
}
