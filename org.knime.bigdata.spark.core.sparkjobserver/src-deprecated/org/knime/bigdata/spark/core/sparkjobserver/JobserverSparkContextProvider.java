/**
 *
 */
package org.knime.bigdata.spark.core.sparkjobserver;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.bigdata.spark.core.sparkjobserver.context.JobserverSparkContext;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

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
        return JobserverPlugin.JOBSERVER_SPARK_VERSION_CHECKER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkVersion getHighestSupportedSparkVersion() {
        return JobserverPlugin.HIGHEST_SUPPORTED_SPARK_VERSION;
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

        final URI uri = contextID.asURI();
        return String.format("Spark Context %s on Spark Jobserver %s:%d", uri.getPath().substring(1), uri.getHost(),
            uri.getPort());
    }

    @Override
    public SparkContextID createTestingSparkContextID(final Map<String, FlowVariable> flowVariables)
        throws InvalidSettingsException {

        final String jobServerUrl = TestflowVariable.getString(TestflowVariable.SPARK_SJS_URL, flowVariables);
        final String contextName = TestflowVariable.getString(TestflowVariable.SPARK_SJS_CONTEXTNAME, flowVariables);
        return SparkContextID.fromConnectionDetails(jobServerUrl, contextName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobServerSparkContextConfig createTestingSparkContextConfig(final SparkContextID contextId,
        final Map<String, FlowVariable> flowVariables, final String fsConnectionId) {

        final String jobServerUrl = TestflowVariable.getString(TestflowVariable.SPARK_SJS_URL, flowVariables);
        final boolean authentication =
            !TestflowVariable.stringEquals(TestflowVariable.SPARK_SJS_AUTHMETHOD, "NONE", flowVariables);

        String user =
            (authentication) ? TestflowVariable.getString(TestflowVariable.SPARK_SJS_USERNAME, flowVariables) : "dummy";
        String password =
            (authentication) ? TestflowVariable.getString(TestflowVariable.SPARK_SJS_PASSWORD, flowVariables) : "dummy";

        final Duration receiveTimeout =
            Duration.ofSeconds(TestflowVariable.getInt(TestflowVariable.SPARK_SJS_RECEIVETIMEOUT, flowVariables));
        final int jobCheckFrequency = 1;
        final SparkVersion sparkVersion =
            SparkVersion.fromLabel(TestflowVariable.getString(TestflowVariable.SPARK_VERSION, flowVariables));
        final String contextName = TestflowVariable.getString(TestflowVariable.SPARK_SJS_CONTEXTNAME, flowVariables);
        final boolean deleteObjectsOnDispose = true;
        final boolean overrideSparkSettings =
            TestflowVariable.isTrue(TestflowVariable.SPARK_SETTINGSOVERRIDE, flowVariables);
        final Map<String, String> customSparkSettings = SparkPreferenceValidator
            .parseSettingsString(TestflowVariable.getString(TestflowVariable.SPARK_SETTINGSCUSTOM, flowVariables));

        return new JobServerSparkContextConfig(jobServerUrl, authentication, user, password, receiveTimeout,
            jobCheckFrequency, sparkVersion, contextName, deleteObjectsOnDispose, overrideSparkSettings,
            customSparkSettings);
    }
}
