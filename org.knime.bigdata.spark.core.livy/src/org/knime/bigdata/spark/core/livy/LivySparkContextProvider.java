/**
 *
 */
package org.knime.bigdata.spark.core.livy;

import java.net.URI;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.livy.context.LivySparkContext;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConnInfoConfig;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextFileSystemConfig;
import org.knime.bigdata.spark.core.livy.node.create.LivySparkContextCreatorNodeSettings;
import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.util.context.create.TimeSettings.TimeShiftStrategy;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Spark context provider that provides Apache Livy connectivity.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContextProvider implements SparkContextProvider<LivySparkContextConfig> {

    /**
     * The highest Spark version currently supported by the Apache Livy integration.
     */
    public static final SparkVersion HIGHEST_SUPPORTED_SPARK_VERSION;

    static {
        SparkVersion currHighest = LivyPlugin.LIVY_SPARK_VERSION_CHECKER.getSupportedSparkVersions().iterator().next();
        for (SparkVersion curr : LivyPlugin.LIVY_SPARK_VERSION_CHECKER.getSupportedSparkVersions()) {
            if (currHighest.compareTo(curr) < 0) {
                currHighest = curr;
            }
        }
        HIGHEST_SUPPORTED_SPARK_VERSION = currHighest;
    }

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
    public SparkVersion getHighestSupportedSparkVersion() {
        return HIGHEST_SUPPORTED_SPARK_VERSION;
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
            return String.format("%s on Apache Livy server", uri.getHost());

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

    /**
     * {@inheritDoc}
     */
    @Override
    public LivySparkContextConfig createTestingSparkContextConfig(final Map<String, FlowVariable> flowVariables,
        final PortObjectSpec fsPortObjectSpec) throws InvalidSettingsException {

        final SparkVersion sparkVersion =
            SparkVersion.fromLabel(TestflowVariable.getString(TestflowVariable.SPARK_VERSION, flowVariables));

        final String livyUrl = TestflowVariable.getString(TestflowVariable.SPARK_LIVY_URL, flowVariables);
        final AuthenticationType authenticationType =
            AuthenticationType.valueOf(TestflowVariable.getString(TestflowVariable.SPARK_LIVY_AUTHMETHOD, flowVariables));
        String stagingAreaFolder = null;

        final boolean setStagingAreaFolder = TestflowVariable.isTrue(TestflowVariable.SPARK_LIVY_SETSTAGINGAREAFOLDER, flowVariables);
        if (setStagingAreaFolder) {
            stagingAreaFolder = TestflowVariable.getString(TestflowVariable.SPARK_LIVY_STAGINGAREAFOLDER, flowVariables);
        }

        final int connectTimeoutSeconds = TestflowVariable.getInt(TestflowVariable.SPARK_LIVY_CONNECTTIMEOUT, flowVariables);
        final int responseTimeoutSeconds =
                TestflowVariable.getInt(TestflowVariable.SPARK_LIVY_RESPONSETIMEOUT, flowVariables);
        final int jobCheckFrequencySeconds = 1;

        final boolean useCustomSparkSettings =
                TestflowVariable.isTrue(TestflowVariable.SPARK_SETTINGSOVERRIDE, flowVariables);

        Map<String, String> customSparkSettings = Collections.emptyMap();
        if (useCustomSparkSettings) {
            customSparkSettings = SparkPreferenceValidator
                .parseSettingsString(TestflowVariable.getString(TestflowVariable.SPARK_SETTINGSCUSTOM, flowVariables));
        }

        final SparkContextID sparkContextId =
            LivySparkContextCreatorNodeSettings.createSparkContextID("testflowContext");

        final TimeShiftStrategy timeShiftStrategy = TimeShiftStrategy.NONE;
        final ZoneId timeShiftZoneId = null;
        final boolean failOnDifferentClusterTimeZone = false;

        if (fsPortObjectSpec instanceof ConnectionInformationPortObjectSpec) {
            final ConnectionInformation remoteFsConnectionInfo =
                ((ConnectionInformationPortObjectSpec)fsPortObjectSpec).getConnectionInformation();
            return new LivySparkContextConnInfoConfig(sparkVersion, livyUrl, authenticationType, stagingAreaFolder,
                connectTimeoutSeconds, responseTimeoutSeconds, jobCheckFrequencySeconds, customSparkSettings,
                sparkContextId, remoteFsConnectionInfo, timeShiftStrategy, timeShiftZoneId, failOnDifferentClusterTimeZone);
        } else if (fsPortObjectSpec instanceof FileSystemPortObjectSpec) {
            final String fsId = ((FileSystemPortObjectSpec)fsPortObjectSpec).getFileSystemId();

            return new LivySparkContextFileSystemConfig(sparkVersion, livyUrl, authenticationType, stagingAreaFolder,
                connectTimeoutSeconds, responseTimeoutSeconds, jobCheckFrequencySeconds, customSparkSettings,
                sparkContextId, fsId, timeShiftStrategy, timeShiftZoneId, failOnDifferentClusterTimeZone);
        } else {
            throw new InvalidSettingsException("Unknown file system port spec.");
        }
    }

}

