/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 03.07.2015 by koetter
 *   Changes on 07.06.2016 by Sascha Wolke:
 *     - fields added: jobServerUrl, authentication, sparkJobLogLevel, overrideSparkSettings, customSparkSettings
 *     - protocol+host+port migrated into jobServerUrl
 *     - authentication flag added
 *     - deleteRDDsOnDispose renamed to deleteObjectsOnDispose
 *     - memPerNode migrated into overrideSparkSettings+customSparkSettings
 */
package org.knime.bigdata.spark.local.node.create;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;
import org.knime.bigdata.spark.node.util.context.create.time.TimeSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.KNIMERuntimeContext;

/**
 * Settings class for the "Create Local Big Data Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @see LocalEnvironmentCreatorNodeModel
 * @see LocalEnvironmentCreatorNodeDialog
 */
public class LocalSparkContextSettings {

    private static final NodeLogger LOG = NodeLogger.getLogger(LocalSparkContextSettings.class);

    /**
     * Enum to model what happens when the KNIME workflow is closed.
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    public enum OnDisposeAction implements ButtonGroupEnumInterface {

            /**
             * Destroy context on dispose.
             */
            DESTROY_CTX,
            /**
             * Delete named objects on dispose.
             */
            DELETE_DATA,

            /**
             * Do nothing on dispose.
             */
            DO_NOTHING;

        @Override
        public String getText() {
            switch (this) {
                case DESTROY_CTX:
                    return "Destroy Spark context";
                case DELETE_DATA:
                    return "Delete Spark DataFrames";
                default:
                    return "Do nothing";
            }
        }

        @Override
        public String getActionCommand() {
            return this.toString();
        }

        @Override
        public String getToolTip() {
            return null;
        }

        @Override
        public boolean isDefault() {
            return this == DELETE_DATA;
        }
    }

    /**
     * Enum to model which type of SQL to support with local Spark.
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    public enum SQLSupport implements ButtonGroupEnumInterface {

            /**
             * Only provide the basic SparkSQL dialect.
             */
            SPARK_SQL_ONLY,

            /**
             * Provide full HiveQL dialect, but do not start Spark Thriftserver (for JDBC).
             */
            HIVEQL_ONLY,

            /**
             * Provide full HiveQL dialect and start Spark Thriftserver to allow JDBC connections to be made.
             */
            HIVEQL_WITH_JDBC;

        @Override
        public String getText() {
            switch (this) {
                case SPARK_SQL_ONLY:
                    return "Spark SQL only";
                case HIVEQL_ONLY:
                    return "HiveQL";
                default:
                    return "HiveQL and provide JDBC connection";
            }
        }

        @Override
        public String getActionCommand() {
            return this.toString();
        }

        @Override
        public String getToolTip() {
            return null;
        }

        @Override
        public boolean isDefault() {
            return this == HIVEQL_WITH_JDBC;
        }
    }

    /**
     * Enum to model how the working directory of the local file system port should be selected.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public enum WorkingDirMode implements ButtonGroupEnumInterface {

        /**
         * Manual working directory selection.
         */
        MANUAL,

        /**
         * Home directory of the user executing the workflow.
         */
        USER_HOME,

        /**
         * Data area of the workflow.
         */
        WORKFLOW_DATA_AREA;

        @Override
        public String getText() {
            switch (this) {
                case USER_HOME:
                    return "Home directory of the current user";
                case WORKFLOW_DATA_AREA:
                    return "Current workflow data area";
                default:
                    return "Manual:";
            }
        }

        @Override
        public String getActionCommand() {
            return this.toString();
        }

        @Override
        public String getToolTip() {
            return null;
        }

        @Override
        public boolean isDefault() {
            return this == MANUAL;
        }
    }

    private static final String DEFAULT_CONTEXT_NAME = "knimeSparkContext";

    private static final WorkingDirMode DEFAULT_WORKING_DIR_MODE = WorkingDirMode.MANUAL;

    private static final String DEFAULT_MANUAL_WORKING_DIR = System.getProperty("user.home");

    private static final String DEFAULT_CUSTOM_SPARK_SETTINGS = "spark.jars: /path/to/some.jar\nspark.sql.shuffle.partitions: 100\n";

    private final boolean m_hasWorkingDirectorySetting;

    // Spark context settings
    private final SettingsModelString m_contextName = new SettingsModelString("contextName", DEFAULT_CONTEXT_NAME);

    private final SettingsModelInteger m_numberOfThreads =
        new SettingsModelIntegerBounded("numberOfThreads", 2, 1, Integer.MAX_VALUE);

    private final SettingsModelString m_onDisposeAction = new SettingsModelString("onDisposeAction",
        LocalSparkContextSettings.OnDisposeAction.DELETE_DATA.getActionCommand());

    private final SettingsModelBoolean m_overrideSparkSettings =
        new SettingsModelBoolean("overrideSparkSettings", KNIMEConfigContainer.overrideSparkSettings());

    private final SettingsModelString m_customSparkSettings =
        new SettingsModelString("customSparkSettings", DEFAULT_CUSTOM_SPARK_SETTINGS);

    // SQL settings
    private final SettingsModelString m_sqlSupport =
        new SettingsModelString("sqlSupport", SQLSupport.HIVEQL_WITH_JDBC.getActionCommand());

    private final SettingsModelBoolean m_useHiveDataFolder = new SettingsModelBoolean("useHiveDataFolder", false);

    private final SettingsModelString m_hiveDataFolder = new SettingsModelString("hiveDataFolder", "");

    private final SettingsModelBoolean m_hideExistsWarning = new SettingsModelBoolean("hideExistsWarning", false);

    private final TimeSettings m_timeShiftSettings = new TimeSettings();

    private final SettingsModelString m_workingDirectoryMode =
        new SettingsModelString("workingDirectoryMode", DEFAULT_WORKING_DIR_MODE.getActionCommand());

    private final SettingsModelString m_manualWorkingDirectory =
        new SettingsModelString("workingDirectory", DEFAULT_MANUAL_WORKING_DIR);

    /**
     * Default constructor.
     *
     * @param hasWorkingDirectorySetting {@code true} if the working directory settings should be used
     */
    public LocalSparkContextSettings(final boolean hasWorkingDirectorySetting) {
        m_hasWorkingDirectorySetting = hasWorkingDirectorySetting;
        updateEnabledness();
    }

    /**
     * @return the settings model for the the context name.
     * @see #getContextName()
     */
    protected SettingsModelString getContextNameModel() {
        return m_contextName;
    }

    /**
     * @return a unique, human-readable name for the local Spark context.
     */
    public String getContextName() {
        return m_contextName.getStringValue();
    }

    /**
     *
     * @return the settings model for the number of worker threads in local Spark.
     * @see #getNumberOfThreads()
     */
    protected SettingsModelInteger getNumberOfThreadsModel() {
        return m_numberOfThreads;
    }

    /**
     *
     * @return the number of worker threads in local Spark.
     */
    public int getNumberOfThreads() {
        return m_numberOfThreads.getIntValue();
    }

    /**
     *
     * @return the action to take when the KNIME workflow is closed.
     */
    public OnDisposeAction getOnDisposeAction() {
        return OnDisposeAction.valueOf(m_onDisposeAction.getStringValue());
    }

    /**
     *
     * @return settings model for the action to take when the KNIME workflow is closed.
     * @see #getOnDisposeAction()
     */
    public SettingsModelString getOnDisposeActionModel() {
        return m_onDisposeAction;
    }

    /**
     *
     * @return settings model for whether to use custom spark settings or not.
     * @see #useCustomSparkSettings()
     */
    protected SettingsModelBoolean getUseCustomSparkSettingsModel() {
        return m_overrideSparkSettings;
    }

    /**
     *
     * @return settings model that says which custom spark settings to use.
     * @see #getCustomSparkSettings()
     */
    protected SettingsModelString getCustomSparkSettingsModel() {
        return m_customSparkSettings;
    }

    /**
     *
     * @return strings that contains the custom spark settings to use.
     */
    public String getCustomSparkSettingsString() {
        return m_customSparkSettings.getStringValue();
    }

    /**
     * Parses the custom Spark settings string (see {@link #getCustomSparkSettingsString()}) into a map and returns it.
     *
     * @return the parsed custom Spark settings as a map.
     */
    public Map<String, String> getCustomSparkSettings() {
        return SparkPreferenceValidator.parseSettingsString(getCustomSparkSettingsString());
    }

    /**
     *
     * @return whether to use custom spark settings or not.
     */
    public boolean useCustomSparkSettings() {
        return m_overrideSparkSettings.getBooleanValue();
    }

    /**
     *
     * @return settings model that says which type of SQL support to use in local Spark.
     * @see #getSQLSupport()
     */
    public SettingsModelString getSqlSupportModel() {
        return m_sqlSupport;
    }

    /**
     *
     * @return which type of SQL support to use in local Spark.
     */
    public SQLSupport getSQLSupport() {
        return SQLSupport.valueOf(m_sqlSupport.getStringValue());
    }

    /**
     * @return {@code true} if hive support is enabled
     */
    public boolean isHiveEnabled() {
        return getSQLSupport() != SQLSupport.SPARK_SQL_ONLY;
    }

    /**
     *
     * @return settings model that says whether or not to use a custom folder for the Hive Metastore DB and warehouse.
     * @see #useHiveDataFolder()
     */
    protected SettingsModelBoolean getUseHiveDataFolderModel() {
        return m_useHiveDataFolder;
    }

    /**
     *
     * @return whether or not to use a custom folder for the Hive Metastore DB and warehouse.
     */
    public boolean useHiveDataFolder() {
        return m_useHiveDataFolder.getBooleanValue();
    }

    /**
     *
     * @return settings model that says which folder to use for the Hive Metastore DB and warehouse.
     * @see #getHiveDataFolder()
     */
    protected SettingsModelString getHiveDataFolderModel() {
        return m_hiveDataFolder;
    }

    /**
     *
     * @return which folder to use for the Hive Metastore DB and warehouse.
     */
    public String getHiveDataFolder() {
        return m_hiveDataFolder.getStringValue();
    }

    /**
     *
     * @return settings model for whether to warn when the local Spark context already exists prior to trying to create
     *         it.
     * @see #hideExistsWarning()
     */
    protected SettingsModelBoolean getHideExistsWarningModel() {
        return m_hideExistsWarning;
    }

    /**
     *
     * @return whether to warn when the local Spark context already exists prior to trying to create it.
     */
    public boolean hideExistsWarning() {
        return m_hideExistsWarning.getBooleanValue();
    }

    /**
     * @return the time shift settings model
     */
    public TimeSettings getTimeShiftSettings() {
        return m_timeShiftSettings;
    }

    /**
     * @return the working directory mode
     */
    public WorkingDirMode getWorkingDirectoryMode() {
        return WorkingDirMode.valueOf(m_workingDirectoryMode.getStringValue());
    }

    /**
     * @return the working directory mode model
     */
    public SettingsModelString getWorkingDirectoryModeModel() {
        return m_workingDirectoryMode;
    }

    /**
     * @return the manual working directory of the file system connection
     */
    public String getManualWorkingDirectory() {
        return m_manualWorkingDirectory.getStringValue();
    }

    /**
     * @return the manual working directory settings model
     */
    public SettingsModelString getManualWorkingDirectoryModel() {
        return m_manualWorkingDirectory;
    }

    /**
     * @return the {@link SparkContextID} derived from the configuration settings.
     */
    public SparkContextID getSparkContextID() {
        return createSparkContextID(getContextName());
    }

    /**
     * Static method to derive a {@link SparkContextID} for the given chosen context name.
     *
     * @param ctxName User-defined name for the local Spark context.
     * @return the corresponding {@link SparkContextID}.
     */
    public final static SparkContextID createSparkContextID(final String ctxName) {
        String suffix = ctxName;
        if (KNIMERuntimeContext.INSTANCE.runningInServerContext()) {
            final Optional<String> wfUser = NodeContext.getWorkflowUser();
            if (wfUser.isPresent()) {
                LOG.info(String.format("Running on KNIME Server. Prefixing local Spark context workflow user %s",
                    wfUser.get()));
                suffix = wfUser.get() + "-" + ctxName;
            } else {
                LOG.warn("Running on KNIME Server but no workflow user is present in node context.");
            }
        }

        return new SparkContextID(SparkContextIDScheme.SPARK_LOCAL + "://" + suffix);
    }

    /**
     * @param settings the NodeSettingsWO to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_contextName.saveSettingsTo(settings);
        m_numberOfThreads.saveSettingsTo(settings);
        m_onDisposeAction.saveSettingsTo(settings);
        m_overrideSparkSettings.saveSettingsTo(settings);
        m_customSparkSettings.saveSettingsTo(settings);

        m_sqlSupport.saveSettingsTo(settings);
        m_useHiveDataFolder.saveSettingsTo(settings);
        m_hiveDataFolder.saveSettingsTo(settings);

        m_hideExistsWarning.saveSettingsTo(settings);

        m_timeShiftSettings.saveSettingsTo(settings.addNodeSettings("timeshift"));

        if (m_hasWorkingDirectorySetting) {
            m_workingDirectoryMode.saveSettingsTo(settings);
            m_manualWorkingDirectory.saveSettingsTo(settings);
        }
    }

    /**
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {

        m_contextName.validateSettings(settings);
        m_numberOfThreads.validateSettings(settings);
        m_onDisposeAction.validateSettings(settings);
        m_overrideSparkSettings.validateSettings(settings);
        if (m_overrideSparkSettings.getBooleanValue()) {
            m_customSparkSettings.validateSettings(settings);
        }

        m_sqlSupport.validateSettings(settings);
        m_useHiveDataFolder.validateSettings(settings);
        if (m_useHiveDataFolder.getBooleanValue()) {
            m_hiveDataFolder.validateSettings(settings);
        }

        m_hideExistsWarning.validateSettings(settings);

        if (settings.containsKey("timeshift")) { // added in 4.2
            m_timeShiftSettings.validateSettings(settings.getNodeSettings("timeshift"));
        }

        if (m_hasWorkingDirectorySetting) {
            if (settings.containsKey("workingDirectoryMode")) { // added in 4.3.1
                m_workingDirectoryMode.validateSettings(settings);
            }
            m_manualWorkingDirectory.validateSettings(settings);
        }

        final LocalSparkContextSettings tmpSettings = new LocalSparkContextSettings(m_hasWorkingDirectorySetting);
        tmpSettings.loadSettingsFrom(settings);
        tmpSettings.validateDeeper();
    }

    /**
     * Validate current settings values.
     *
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateDeeper() throws InvalidSettingsException {
        final List<String> errors = new ArrayList<>();

        SparkPreferenceValidator.validateSparkContextName(getContextName(), errors);
        SparkPreferenceValidator.validateCustomSparkSettings(useCustomSparkSettings(), getCustomSparkSettingsString(),
            errors);

        if (useHiveDataFolder()) {
            final File hiveDataFolder = new File(getHiveDataFolder());
            if (!hiveDataFolder.exists()) {
                errors.add("Hive data folder does not exist.");
            } else if (!hiveDataFolder.isDirectory()) {
                errors.add("The configured Hive data folder exists but is not a folder.");
            } else if (!hiveDataFolder.canWrite()) {
                errors.add("Cannot write to the configured Hive data folder.");
            }
        }

        if (m_hasWorkingDirectorySetting && getWorkingDirectoryMode() == WorkingDirMode.MANUAL
                && StringUtils.isBlank(getManualWorkingDirectory())) {
            errors.add("Working directory required.");
        }

        if (!errors.isEmpty()) {
            throw new InvalidSettingsException(SparkPreferenceValidator.mergeErrors(errors));
        }
    }

	/**
	 * @param settings
	 *            the NodeSettingsRO to read from.
	 * @throws InvalidSettingsException
	 *             if the settings are invalid.
	 */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_contextName.loadSettingsFrom(settings);
        m_numberOfThreads.loadSettingsFrom(settings);
        m_onDisposeAction.loadSettingsFrom(settings);
        m_overrideSparkSettings.loadSettingsFrom(settings);
        m_customSparkSettings.loadSettingsFrom(settings);

        m_sqlSupport.loadSettingsFrom(settings);
        m_useHiveDataFolder.loadSettingsFrom(settings);
        m_hiveDataFolder.loadSettingsFrom(settings);

        m_hideExistsWarning.loadSettingsFrom(settings);

        if (settings.containsKey("timeshift")) { // added in 4.2
            m_timeShiftSettings.loadSettingsFrom(settings.getNodeSettings("timeshift"));
        }

        if (m_hasWorkingDirectorySetting) {
            if (settings.containsKey("workingDirectoryMode")) { // added in 4.3.1
                m_workingDirectoryMode.loadSettingsFrom(settings);
            }
            m_manualWorkingDirectory.loadSettingsFrom(settings);
        }

        updateEnabledness();
    }

    /**
     * Updates the enabledness of the underlying settings models.
     */
    public void updateEnabledness() {
        m_customSparkSettings.setEnabled(m_overrideSparkSettings.getBooleanValue());

        final boolean hiveQLEnabled = isHiveEnabled();
        m_useHiveDataFolder.setEnabled(hiveQLEnabled);
        m_hiveDataFolder.setEnabled(hiveQLEnabled && useHiveDataFolder());

        m_timeShiftSettings.updateEnabledness();
    }

    /**
     * @return a new {@link LocalSparkContextConfig} derived from the current settings.
     */
    public LocalSparkContextConfig createContextConfig() {
        boolean enableHiveSupport = false;
        boolean startThriftserver = false;

        if (getSQLSupport() == SQLSupport.HIVEQL_WITH_JDBC) {
            enableHiveSupport = true;
            startThriftserver = true;
        } else if (getSQLSupport() == SQLSupport.HIVEQL_ONLY) {
            enableHiveSupport = true;
        }

        return new LocalSparkContextConfig(
            // Spark
            getSparkContextID(),
            getContextName(),
            getNumberOfThreads(),
            getOnDisposeAction() == OnDisposeAction.DELETE_DATA,
            useCustomSparkSettings(),
            getCustomSparkSettings(),
            m_timeShiftSettings.getTimeShiftStrategy(),
            m_timeShiftSettings.getFixedTimeZoneId(),
            m_timeShiftSettings.failOnDifferentClusterTZ(),

            // Hive
            enableHiveSupport,
            startThriftserver,
            -1,
            useHiveDataFolder(),
            useHiveDataFolder() ? getHiveDataFolder() : null);
    }
}
