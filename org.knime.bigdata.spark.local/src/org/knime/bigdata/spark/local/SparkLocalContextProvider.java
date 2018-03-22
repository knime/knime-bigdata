/**
 * 
 */
package org.knime.bigdata.spark.local;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
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
    public SparkContextIDScheme getSupportedScheme() {
        return SparkContextIDScheme.SPARK_LOCAL;
    }

    /**
     * {@inheritDoc}
     */
	@Override
	public String toPrettyString(SparkContextID contextID) {
		if (contextID.getScheme() != SparkContextIDScheme.SPARK_LOCAL) {
			throw new IllegalArgumentException("Unsupported scheme: " + contextID.getScheme().toString());
		}

		final URI uri = contextID.asURI();
		return String.format(String.format("Local Spark Context (%s)", uri.getHost()));
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public Optional<SparkContext<LocalSparkContextConfig>> createDefaultSparkContextIfPossible() {
		// currently, local Spark never provides the default Spark context.
		return Optional.empty();
	}
}
