/**
 *
 */
package org.knime.bigdata.spark.core.livy;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.livy.context.LivySparkContext;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Spark context provider that provides Apache Livy connectivity.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContextProvider implements SparkContextProvider<LivySparkContextConfig> {

    /**
     * {@inheritDoc}
     */
    @Override
    public CompatibilityChecker getChecker() {
        return LivyPlugin.LIVY_SPARK_VERSION_CHECKER;
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
    public SparkContext<LivySparkContextConfig> createContext(final SparkContextID contextID) {
        return new LivySparkContext(contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextIDScheme getSupportedScheme() {
        return SparkContextIDScheme.SPARK_LIVY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toPrettyString(final SparkContextID contextID) {
		try {
			
	        if (contextID.getScheme() != SparkContextIDScheme.SPARK_LIVY) {
	            throw new IllegalArgumentException("Unsupported scheme: " + contextID.getScheme());
	        }
	        
	        final URI uri = contextID.asURI();
	        return String.format("%s on Apache Livy server at %s:d", uri.getFragment(), uri.getHost(), uri.getPort());

		} catch (IllegalArgumentException e) {
			// should never happen
			throw new RuntimeException(e);
		}
    }

    /**
     * {@inheritDoc}
     */
	@Override
	public Optional<SparkContext<LivySparkContextConfig>> createDefaultSparkContextIfPossible() {
		// currently, the Livy connector never provides the default Spark context.
		return Optional.empty();
	}
}
