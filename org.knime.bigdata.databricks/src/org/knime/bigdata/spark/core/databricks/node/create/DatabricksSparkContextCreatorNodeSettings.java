/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.core.databricks.node.create;

import static org.apache.commons.lang3.StringUtils.stripToEmpty;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_HOST;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_PORT;
import static org.knime.database.driver.URLTemplates.resolveDriverUrl;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_DRIVER_URL_TEMPLATE_IS_INVALID;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_HOST_IS_NOT_DEFINED;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_PORT_IS_NOT_DEFINED;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.database.databricks.DatabricksDBDriverLocator;
import org.knime.bigdata.dbfs.filehandler.DBFSRemoteFileHandler;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.databricks.DatabricksSparkContextProvider;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContext;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.database.DBType;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.node.connector.DBSessionSettings;
import org.knime.database.node.datatype.mapping.SettingsModelDatabaseDataTypeMapping;
import org.knime.database.util.BlankTokenValueException;
import org.knime.database.util.NestedTokenException;
import org.knime.database.util.NoSuchTokenException;
import org.knime.database.util.StringTokenException;
import org.knime.datatype.mapping.DataTypeMappingDirection;

/**
 * Settings class for the "Create Spark Context (Databricks)" node.
 *
 * This class contains in addition methods to create:
 *   - {@link SparkContextID} from a instance
 *   - {@link DatabricksSparkContextConfig} from a instance
 *   - DBFS {@link ConnectionInformation} from a instance
 *   - a instance from flowvariables
 *
 * @author Sascha Wolke, KNIME GmbH
 * @see DatabricksSparkContextCreatorNodeModel
 * @see DatabricksSparkContextCreatorNodeDialog
 */
public class DatabricksSparkContextCreatorNodeSettings extends DBSessionSettings {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DatabricksSparkContextCreatorNodeSettings.class);

    private static final DBType DB_TYPE = Databricks.DB_TYPE;

    private static final int DEFAULT_PORT = 443;

    private static final DBSQLDialectFactory DEFAULT_DIALECT_FACTORY =
            DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(DB_TYPE);

    /**
     * This is an invisible setting used to allow the node settings to evolve over time. It allows us to detect the
     * current version and take appropriate measures to update the settings in a controlled manner.
     */
    private final SettingsModelInteger m_settingsVersion = new SettingsModelInteger("settingsVersion", 1);

    private final SettingsModelString m_sparkVersion = new SettingsModelString("sparkVersion",
        DatabricksSparkContextProvider.HIGHEST_SUPPORTED_SPARK_VERSION.getLabel());

    private final SettingsModelString m_url = new SettingsModelString("url", "https://");

    private final SettingsModelString m_clusterId = new SettingsModelString("clusterId", "");

    private final SettingsModelString m_workspaceId = new SettingsModelString("workspaceId", "");

    private final SettingsModelAuthentication m_authentication =
        new SettingsModelAuthentication("authentication", AuthenticationType.PWD);

    private final SettingsModelBoolean m_setStagingAreaFolder = new SettingsModelBoolean("setStagingAreaFolder", false);

    private final SettingsModelString m_stagingAreaFolder = new SettingsModelString("stagingAreaFolder", "");

    private final SettingsModelIntegerBounded m_connectionTimeout =
        new SettingsModelIntegerBounded("connectionTimeout", 30, 0, Integer.MAX_VALUE);

    private final SettingsModelIntegerBounded m_receiveTimeout =
        new SettingsModelIntegerBounded("receiveTimeout", 60, 0, Integer.MAX_VALUE);

    private final SettingsModelIntegerBounded m_jobCheckFrequency =
        new SettingsModelIntegerBounded("jobCheckFrequency", 1, 1, Integer.MAX_VALUE);

    private final SettingsModelDatabaseDataTypeMapping m_externalToKnimeMappingConfig =
        new SettingsModelDatabaseDataTypeMapping("external_to_knime_mapping",
            DataTypeMappingDirection.EXTERNAL_TO_KNIME);

    private final SettingsModelDatabaseDataTypeMapping m_knimeToExternalMappingConfig =
        new SettingsModelDatabaseDataTypeMapping("knime_to_external_mapping",
            DataTypeMappingDirection.KNIME_TO_EXTERNAL);

    /**
     * Terminate the cluster in {@link DatabricksSparkContext#destroy}.
     */
    private final SettingsModelBoolean m_terminateClusterOnDestroy =
        new SettingsModelBoolean("terminateClusterOnDestroy", false);

    /**
     * Constructor.
     */
    public DatabricksSparkContextCreatorNodeSettings() {
        super();
        setDBType(DB_TYPE.getId());
        setDialect(DEFAULT_DIALECT_FACTORY.getId());
        setDriver(DatabricksDBDriverLocator.getLatestSimbaOrHiveDriverID());
    }

    /**
     * Updates the enabledness of the underlying settings models.
     */
    public void updateEnabledness() {
        m_stagingAreaFolder.setEnabled(m_setStagingAreaFolder.getBooleanValue());
    }

    /**
     * @return the settings model for the Spark version to assume.
     */
    protected SettingsModelString getSparkVersionModel() {
        return m_sparkVersion;
    }

    /**
     * @return the {@link SparkVersion} to assume.
     */
    public SparkVersion getSparkVersion() {
        return SparkVersion.fromLabel(m_sparkVersion.getStringValue());
    }

    /**
     * @return the settings model of the databricks instance URL
     */
    protected SettingsModelString getDatabricksInstanceURLModel() {
        return m_url;
    }

    /**
     * @return the Databricks instance URL.
     */
    public String getDatabricksInstanceURL() {
        return m_url.getStringValue();
    }

    /**
     * @return the settings model for the Databricks cluster ID.
     * @see #getClusterId()
     */
    protected SettingsModelString getClusterIdModel() {
        return m_clusterId;
    }

    /**
     * @return the Databricks cluster ID.
     */
    public String getClusterId() {
        return m_clusterId.getStringValue();
    }

    /**
     * @return the settings model for the Databricks workspace ID.
     * @see #getClusterId()
     */
    protected SettingsModelString getWorkspaceIdModel() {
        return m_workspaceId;
    }

    /**
     * @return the Databricks workspace ID, might be empty on AWS.
     */
    public String getWorkspaceId() {
        return m_workspaceId.getStringValue();
    }

    /**
     * @return the settings model for the authentication
     */
    protected SettingsModelAuthentication getAuthenticationModel() {
        return m_authentication;
    }

    /**
     * @return settings model for whether a staging area folder has been set.
     */
    protected SettingsModelBoolean getSetStagingAreaFolderModel() {
        return m_setStagingAreaFolder;
    }

    /**
     * @return true, when a staging area folder has been set.
     */
    public boolean isStagingAreaFolderSet() {
        return m_setStagingAreaFolder.getBooleanValue();
    }

    /**
     * @return settings model for staging area folder to use
     */
    protected SettingsModelString getStagingAreaFolderModel() {
        return m_stagingAreaFolder;
    }

    /**
     * @return the folder to use for the staging area
     */
    public String getStagingAreaFolder() {
        return m_stagingAreaFolder.getStringValue();
    }

    /**
     * @return settings model for the TCP socket connection timeout in seconds when connecting to Databricks.
     */
    protected SettingsModelIntegerBounded getConnectionTimeoutModel() {
        return m_connectionTimeout;
    }

    /**
     *
     * @return the TCP socket connection timeout in seconds when connected to Databricks.
     */
    public int getConnectionTimeout() {
        return m_connectionTimeout.getIntValue();
    }

    /**
     *
     * @return settings model for the HTTP receive timeout in seconds when talking to Databricks.
     */
    protected SettingsModelIntegerBounded getReceiveTimeoutModel() {
        return m_receiveTimeout;
    }

    /**
     *
     * @return the HTTP receive timeout in seconds when talking to Databricks.
     */
    public int getReceiveTimeout() {
        return m_receiveTimeout.getIntValue();
    }

    /**
     *
     * @return settings model for the Spark job status polling frequency in seconds.
     */
    protected SettingsModelIntegerBounded getJobCheckFrequencyModel() {
        return m_jobCheckFrequency;
    }

    /**
     *
     * @return the Spark job status polling frequency in seconds.
     */
    public int getJobCheckFrequency() {
        return m_jobCheckFrequency.getIntValue();
    }

    /**
     * @return model for the terminate cluster on context destroy setting
     */
    protected SettingsModelBoolean getTerminateClusterOnDestroyModel() {
        return m_terminateClusterOnDestroy;
    }

    /**
     * @return <code>true</code> if cluster should be terminated on context destroy
     */
    public boolean terminateClusterOnDestroy() {
        return m_terminateClusterOnDestroy.getBooleanValue();
    }

    /**
     * @return database external to KNIME mapping config model
     */
    public SettingsModelDatabaseDataTypeMapping getExternalToKnimeMappingModel() {
        return m_externalToKnimeMappingConfig;
    }

    /**
     * @return database KNIME to external mapping config model
     */
    public SettingsModelDatabaseDataTypeMapping getKnimeToExternalMappingModel() {
        return m_knimeToExternalMappingConfig;
    }

    @Override
    public String getDBUrl() throws InvalidSettingsException {
        final Optional<DBDriverWrapper> driver = DBDriverRegistry.getInstance().getDriver(getDriver());
        if (!driver.isPresent()) {
            return null;
        }
        final URI uri = URI.create(getDatabricksInstanceURL());
        final Map<String, String> variableValues = new HashMap<>();
        variableValues.put(VARIABLE_NAME_HOST, stripToEmpty(uri.getHost()));
        variableValues.put(VARIABLE_NAME_PORT, String.valueOf(uri.getPort() > 0 ? uri.getPort() : DEFAULT_PORT));
        try {
            return resolveDriverUrl(driver.get().getURLTemplate(), variableValues, variableValues);
        } catch (final BlankTokenValueException exception) {
            final String token = exception.getToken();
            String message = exception.getMessage();
            if (token != null) {
                switch (token) {
                    case VARIABLE_NAME_HOST:
                        message = DATABASE_HOST_IS_NOT_DEFINED;
                        break;
                    case VARIABLE_NAME_PORT:
                        message = DATABASE_PORT_IS_NOT_DEFINED;
                        break;
                    default:
                        LOGGER.codingWithFormat(
                            "There is no alternative error message for the blank mandatory token: \"%s\"", token);
                }
            }
            throw new InvalidSettingsException(message, exception);
        } catch (final NestedTokenException exception) {
            final String token = exception.getToken();
            throw new InvalidSettingsException(
                "The token " + (token == null ? null : '"' + token + '"') + " has illegally nested content.",
                exception);
        } catch (final NoSuchTokenException exception) {
            final String token = exception.getToken();
            throw new InvalidSettingsException((token == null ? null : '"' + token + '"')
                + " is not a valid driver URL template token. Please refer to the node documentation for the available"
                + " URL template tokens depending on the chosen settings.", exception);
        } catch (final StringTokenException exception) {
            throw new InvalidSettingsException(DATABASE_DRIVER_URL_TEMPLATE_IS_INVALID, exception);
        }
    }

    /**
     * Saves the the settings of this instance to the given {@link NodeSettingsWO}.
     *
     * @param settings the NodeSettingsWO to write to.
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);

        m_settingsVersion.saveSettingsTo(settings);

        m_sparkVersion.saveSettingsTo(settings);
        m_url.saveSettingsTo(settings);
        m_clusterId.saveSettingsTo(settings);
        m_workspaceId.saveSettingsTo(settings);
        m_authentication.saveSettingsTo(settings);

        m_setStagingAreaFolder.saveSettingsTo(settings);
        m_stagingAreaFolder.saveSettingsTo(settings);

        m_connectionTimeout.saveSettingsTo(settings);
        m_receiveTimeout.saveSettingsTo(settings);
        m_jobCheckFrequency.saveSettingsTo(settings);

        m_terminateClusterOnDestroy.saveSettingsTo(settings);

        m_externalToKnimeMappingConfig.saveSettingsTo(settings);
        m_knimeToExternalMappingConfig.saveSettingsTo(settings);
    }

    /**
     * Validates the settings in the given {@link NodeSettingsRO}.
     *
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);

        m_settingsVersion.validateSettings(settings);

        m_sparkVersion.validateSettings(settings);
        m_url.validateSettings(settings);
        m_clusterId.validateSettings(settings);
        m_workspaceId.validateSettings(settings);
        m_authentication.validateSettings(settings);

        m_setStagingAreaFolder.validateSettings(settings);
        if (m_setStagingAreaFolder.getBooleanValue()) {
            m_stagingAreaFolder.validateSettings(settings);
        }

        m_connectionTimeout.validateSettings(settings);
        m_receiveTimeout.validateSettings(settings);
        m_jobCheckFrequency.validateSettings(settings);

        m_terminateClusterOnDestroy.validateSettings(settings);

        m_externalToKnimeMappingConfig.validateSettings(settings);
        m_knimeToExternalMappingConfig.validateSettings(settings);

        final DatabricksSparkContextCreatorNodeSettings tmpSettings = new DatabricksSparkContextCreatorNodeSettings();
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

        if (StringUtils.isBlank(getDatabricksInstanceURL())) {
            errors.add("The Databricks deployment URL must not be empty.");
        } else {
            try {
                final URL uri = new URL(getDatabricksInstanceURL());

                if (StringUtils.isBlank(uri.getProtocol()) || !uri.getProtocol().equalsIgnoreCase("https")) {
                    errors.add("HTTPS Protocol in Databricks deployment URL required (only https supported)");
                } else if (StringUtils.isBlank(uri.getHost())) {
                    errors.add("Hostname in Databricks deployment URL required.");
                }

            } catch (MalformedURLException e) {
                errors.add(String.format("Invalid Databricks deployment URL: %s", e.getMessage()));
            }
        }

        if (StringUtils.isBlank(getClusterId())) {
            errors.add("Databricks cluster ID required.");
        }

        if (isStagingAreaFolderSet() && StringUtils.isBlank(getStagingAreaFolder())) {
            errors.add("Staging area folder required if set staging area is selected.");
        }

        if (StringUtils.isBlank(getDriver())) {
            errors.add("JDBC driver required (see DB Port -> Driver in configuration dialog)");
        }

        if (!errors.isEmpty()) {
            throw new InvalidSettingsException(SparkPreferenceValidator.mergeErrors(errors));
        }
    }

    /**
     * Validate that configured driver exists.
     * @throws InvalidSettingsException if driver is not registered
     */
    public void validateDriverRegistered() throws InvalidSettingsException {
        if (!DBDriverRegistry.getInstance().getDriver(getDriver()).isPresent()) {
            throw new InvalidSettingsException("Unable to find DB driver: " + getDriver());
        }
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);

        m_settingsVersion.loadSettingsFrom(settings);

        m_sparkVersion.loadSettingsFrom(settings);
        m_url.loadSettingsFrom(settings);
        m_clusterId.loadSettingsFrom(settings);
        m_workspaceId.loadSettingsFrom(settings);
        m_authentication.loadSettingsFrom(settings);

        m_setStagingAreaFolder.loadSettingsFrom(settings);
        m_stagingAreaFolder.loadSettingsFrom(settings);

        m_connectionTimeout.loadSettingsFrom(settings);
        m_receiveTimeout.loadSettingsFrom(settings);
        m_jobCheckFrequency.loadSettingsFrom(settings);

        m_terminateClusterOnDestroy.loadSettingsFrom(settings);

        m_externalToKnimeMappingConfig.loadSettingsFrom(settings);
        m_knimeToExternalMappingConfig.loadSettingsFrom(settings);

        updateEnabledness();
    }

    /**
     * Utility function to generate a Databricks {@link SparkContextID}. This should act as the single source of truth when
     * generating IDs for Databricks Spark contexts.
     *
     * @param uniqueId A unique ID for the context. It is the responsibility of the caller to ensure uniqueness.
     * @return a new {@link SparkContextID}
     */
    public static SparkContextID createSparkContextID(final String uniqueId) {
        return new SparkContextID(String.format("%s://%s", SparkContextIDScheme.SPARK_DATABRICKS, uniqueId));
    }

    /**
     * @param contextId The ID of the Spark context for which to create the config object.
     * @param connInfo connection info for staging area
     * @param credentialsProvider credentials provider to use
     * @return a new {@link DatabricksSparkContextConfig} derived from the current settings.
     * @throws InvalidSettingsException on unknown authentication method or empty username or password
     */
    public DatabricksSparkContextConfig createContextConfig(final SparkContextID contextId,
        final ConnectionInformation connInfo, final CredentialsProvider credentialsProvider) throws InvalidSettingsException {

        final String username = getUsername(credentialsProvider);
        final String password = getPassword(credentialsProvider);

        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            throw new InvalidSettingsException("Username and password required.");
        }

        final String stagingArea = isStagingAreaFolderSet() ? getStagingAreaFolder() : null;

        if (username.equalsIgnoreCase("token")) {
            return new DatabricksSparkContextConfig(getSparkVersion(), getDatabricksInstanceURL(), getClusterId(),
                password, stagingArea, terminateClusterOnDestroy(), getConnectionTimeout(), getReceiveTimeout(),
                getJobCheckFrequency(), contextId, connInfo);
        } else {
            return new DatabricksSparkContextConfig(getSparkVersion(), getDatabricksInstanceURL(), getClusterId(),
                username, password, stagingArea, terminateClusterOnDestroy(), getConnectionTimeout(),
                getReceiveTimeout(), getJobCheckFrequency(), contextId, connInfo);
        }
    }

    /**
     * Create a DBFS connection from this settings.
     *
     * @param credentialsProvider Provider for the credentials
     * @return The DBFS connection information object
     * @throws InvalidSettingsException
     */
    public ConnectionInformation createDBFSConnectionInformation(final CredentialsProvider credentialsProvider)
        throws InvalidSettingsException {

        final URI uri = URI.create(getDatabricksInstanceURL());
        final ConnectionInformation connectionInformation = new ConnectionInformation();
        connectionInformation.setProtocol(DBFSRemoteFileHandler.DBFS_PROTOCOL.getName());
        connectionInformation.setHost(uri.getHost());
        connectionInformation.setPort(uri.getPort() > 0 ? uri.getPort() : DEFAULT_PORT);

        final String username = getUsername(credentialsProvider);
        final String password = getPassword(credentialsProvider);

        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            throw new InvalidSettingsException("Username and password required.");
        }

        if (username.equalsIgnoreCase("token")) {
            connectionInformation.setUseToken(true);
            connectionInformation.setToken(password);
        } else {
            connectionInformation.setUser(username);
            connectionInformation.setPassword(password);
        }

        connectionInformation.setTimeout(getReceiveTimeout() * 1000);

        return connectionInformation;
    }

    /**
     * @param credentialsProvider to use
     * @return username for connection
     * @throws InvalidSettingsException on unknown authentication type
     */
    public String getUsername(final CredentialsProvider credentialsProvider) throws InvalidSettingsException {
        if (m_authentication.getAuthenticationType() == AuthenticationType.USER_PWD) {
            return m_authentication.getUsername();
        } else if (m_authentication.getAuthenticationType() == AuthenticationType.PWD) {
            return "token";
        } else if (m_authentication.getAuthenticationType() == AuthenticationType.CREDENTIALS) {
            return m_authentication.getUserName(credentialsProvider);
        } else {
            throw new InvalidSettingsException("Unknown authentication method.");
        }
    }

    /**
     * @param credentialsProvider to use
     * @return password for connection
     * @throws InvalidSettingsException on unknown authentication type
     */
    public String getPassword(final CredentialsProvider credentialsProvider) throws InvalidSettingsException {
        if (m_authentication.getAuthenticationType() == AuthenticationType.USER_PWD) {
            return m_authentication.getPassword();
        } else if (m_authentication.getAuthenticationType() == AuthenticationType.PWD) {
            return  m_authentication.getPassword();
        } else if (m_authentication.getAuthenticationType() == AuthenticationType.CREDENTIALS) {
            return m_authentication.getPassword(credentialsProvider);
        } else {
            throw new InvalidSettingsException("Unknown authentication method.");
        }
    }

    /**
     * Create settings model from testing flow variables.
     *
     * @param flowVariables variables to use
     * @return settings model
     * @throws InvalidSettingsException
     */
    public static DatabricksSparkContextCreatorNodeSettings
        fromFlowVariables(final Map<String, FlowVariable> flowVariables) throws InvalidSettingsException {

        final DatabricksSparkContextCreatorNodeSettings settings = new DatabricksSparkContextCreatorNodeSettings();
        settings.m_sparkVersion
            .setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_VERSION, flowVariables));
        settings.m_url.setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_URL, flowVariables));
        settings.m_clusterId
            .setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_CLUSTER_ID, flowVariables));
        settings.m_workspaceId
            .setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_WORKSPACE_ID, flowVariables));

        if (flowVariables.containsKey(TestflowVariable.SPARK_DATABRICKS_TOKEN.getName())) {
            final String token = TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_TOKEN, flowVariables);
            settings.m_authentication.setValues(AuthenticationType.PWD, null, null, token);
        } else {
            final String user = TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_USERNAME, flowVariables);
            final String pass = TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_PASSWORD, flowVariables);
            settings.m_authentication.setValues(AuthenticationType.USER_PWD, null, user, pass);
        }

        settings.m_setStagingAreaFolder.setBooleanValue(
            TestflowVariable.isTrue(TestflowVariable.SPARK_DATABRICKS_SETSTAGINGAREAFOLDER, flowVariables));
        if (settings.isStagingAreaFolderSet()) {
            settings.m_stagingAreaFolder.setStringValue(
                TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_STAGINGAREAFOLDER, flowVariables));
        }

        settings.m_terminateClusterOnDestroy.setBooleanValue(false); // keep context cluster running

        settings.m_connectionTimeout
            .setIntValue(TestflowVariable.getInt(TestflowVariable.SPARK_DATABRICKS_CONNECTIONTIMEOUT, flowVariables));
        settings.m_receiveTimeout
            .setIntValue(TestflowVariable.getInt(TestflowVariable.SPARK_DATABRICKS_RECEIVETIMEOUT, flowVariables));
        settings.m_jobCheckFrequency.setIntValue(1); // one second

        settings.validateDeeper();

        return settings;
    }
}
