/**
 * 
 */
package org.knime.bigdata.spark.local;

import java.net.URI;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;
import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings;
import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.SQLSupport;
import org.knime.bigdata.spark.node.util.context.create.time.TimeSettings.TimeShiftStrategy;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Spark context provider that provides local Spark.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
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
    public SparkVersion getHighestSupportedSparkVersion() {
        return LocalSparkVersion.VERSION_CHECKER.getSupportedSparkVersions().iterator().next();
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

    @Override
    public SparkContextID createTestingSparkContextID(final Map<String, FlowVariable> flowVariables)
        throws InvalidSettingsException {

        final String contextName = TestflowVariable.getString(TestflowVariable.SPARK_LOCAL_CONTEXTNAME, flowVariables);
        return LocalSparkContextSettings.createSparkContextID(contextName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalSparkContextConfig createTestingSparkContextConfig(final SparkContextID contextID,
        final Map<String, FlowVariable> flowVariables, final String fsId) {

        final String contextName = TestflowVariable.getString(TestflowVariable.SPARK_LOCAL_CONTEXTNAME, flowVariables);
        final int numberOfThreads = TestflowVariable.getInt(TestflowVariable.SPARK_LOCAL_THREADS, flowVariables);
        final boolean deleteObjectsOnDispose = true;
        final boolean useCustomSparkSettings =
            TestflowVariable.isTrue(TestflowVariable.SPARK_SETTINGSOVERRIDE, flowVariables);
        final Map<String, String> customSparkSettings = SparkPreferenceValidator
            .parseSettingsString(TestflowVariable.getString(TestflowVariable.SPARK_SETTINGSCUSTOM, flowVariables));
        final TimeShiftStrategy timeShiftStrategy = TimeShiftStrategy.NONE;
        final ZoneId timeShiftZoneId = null;
        final boolean failOnDifferentTimeZone = false;
        boolean enableHiveSupport = TestflowVariable.stringEquals(TestflowVariable.SPARK_LOCAL_SQLSUPPORT,
            SQLSupport.HIVEQL_WITH_JDBC.getActionCommand(), flowVariables)
            || TestflowVariable.stringEquals(TestflowVariable.SPARK_LOCAL_SQLSUPPORT,
                SQLSupport.HIVEQL_ONLY.getActionCommand(), flowVariables);
        final boolean startThriftserver = TestflowVariable.stringEquals(TestflowVariable.SPARK_LOCAL_SQLSUPPORT,
            SQLSupport.HIVEQL_WITH_JDBC.getActionCommand(), flowVariables);
        final int thriftserverPort =
            TestflowVariable.getInt(TestflowVariable.SPARK_LOCAL_THRIFTSERVERPORT, flowVariables);
        final boolean useHiveDataFolder =
            TestflowVariable.isTrue(TestflowVariable.SPARK_LOCAL_USEHIVEDATAFOLDER, flowVariables);
        final String hiveDataFolder = useHiveDataFolder ? //
            TestflowVariable.getString(TestflowVariable.SPARK_LOCAL_HIVEDATAFOLDER, flowVariables) : null;

        return new LocalSparkContextConfig(contextID, contextName, numberOfThreads, deleteObjectsOnDispose,
            useCustomSparkSettings, customSparkSettings, timeShiftStrategy, timeShiftZoneId, failOnDifferentTimeZone,
            enableHiveSupport, startThriftserver, thriftserverPort, useHiveDataFolder, hiveDataFolder);
    }
}
