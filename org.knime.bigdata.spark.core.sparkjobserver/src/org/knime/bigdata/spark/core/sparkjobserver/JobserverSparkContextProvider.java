/**
 *
 */
package org.knime.bigdata.spark.core.sparkjobserver;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
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
    public SparkContextIDScheme getSupportedScheme() {
        return SparkContextIDScheme.SPARK_JOBSERVER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toPrettyString(final SparkContextID contextID) {
        if (contextID.getScheme() != SparkContextIDScheme.SPARK_JOBSERVER) {
            throw new IllegalArgumentException("Unspported scheme: " + contextID.getScheme());
        }

        URI uri = URI.create(contextID.toString());
        return String.format("Spark Context %s on Spark Jobserver %s:%d", uri.getHost(), uri.getPort(), uri.getPath().substring(1));
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
