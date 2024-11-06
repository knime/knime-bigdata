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

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings.AuthType;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnection;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnectionConfig;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSDescriptorProvider;
import org.knime.bigdata.dbfs.filehandling.node.DbfsConnectorNodeSettings;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextFileSystemConfig;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Settings class for the "Create Spark Context (Databricks)" node using {@link FSConnection} base file system.
 *
 * This class contains in addition methods to create: - {@link SparkContextID} from a instance -
 * {@link DatabricksSparkContextConfig} from a instance - DBFS {@link ConnectionInformation} from a instance - a
 * instance from flowvariables
 *
 * @author Sascha Wolke, KNIME GmbH
 * @see DatabricksSparkContextCreatorNodeModel
 * @see DatabricksSparkContextCreatorNodeDialog
 */
public class DatabricksSparkContextCreatorNodeSettings2 extends AbstractDatabricksSparkContextCreatorNodeSettings {

    private final boolean m_useWorkspaceConnection;

    private final DbfsAuthenticationNodeSettings m_authSettings =
        new DbfsAuthenticationNodeSettings("auth", AuthType.TOKEN);

    private final SettingsModelString m_workingDirectory = new SettingsModelString("workingDirectory", "/");

    /**
     * Constructor.
     */
    DatabricksSparkContextCreatorNodeSettings2(final boolean useWorkspaceConnection) {
        super();
        m_useWorkspaceConnection = useWorkspaceConnection;
    }

    @Override
    protected AbstractDatabricksSparkContextCreatorNodeSettings createTestingInstance() {
        return new DatabricksSparkContextCreatorNodeSettings2(m_useWorkspaceConnection);
    }

    /**
     * @return authentication settings.
     */
    public DbfsAuthenticationNodeSettings getAuthenticationSettings() {
        return m_authSettings;
    }

    /**
     * @return working directory
     */
    String getWorkingDirectory() {
        return m_workingDirectory.getStringValue();
    }

    /**
     * @return working directory settings model
     */
    SettingsModelString getWorkingDirectorySettingsModel() {
        return m_workingDirectory;
    }

    @Override
    public String getDBUrl() throws InvalidSettingsException {
        if (m_useWorkspaceConnection) {
            throw new IllegalArgumentException("Cannot create JDBC URL without workspace connection. This is a bug.");
        }

        return getDBUrl(URI.create(getDatabricksInstanceURL()), getDriver());
    }

    public String getDBUrl(final DatabricksAccessTokenCredential credential) throws InvalidSettingsException {
        if (!m_useWorkspaceConnection) {
            throw new IllegalArgumentException("Cannot create JDBC URL with workspace connection. This is a bug.");
        }

        return getDBUrl(credential.getDatabricksWorkspaceUrl(), getDriver());
    }

    /**
     * Saves settings to the given {@link NodeSettingsWO} (to be called by the node dialog).
     *
     * @param settings
     */
    public void saveSettingsForDialog(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        // auth settings are saved by dialog
        m_workingDirectory.saveSettingsTo(settings);
    }

    /**
     * Saves settings to the given {@link NodeSettingsWO} (to be called by the node model).
     *
     * @param settings
     */
    public void saveSettingsForModel(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        m_authSettings.saveSettingsForModel(settings);
        m_workingDirectory.saveSettingsTo(settings);

    }

    @Override
    public void validateDeeper() throws InvalidSettingsException {
        validateDeeper(m_useWorkspaceConnection);
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);

        if (!m_useWorkspaceConnection) {
            m_authSettings.validateSettings(settings);
        }

        m_workingDirectory.validateSettings(settings);

        final DatabricksSparkContextCreatorNodeSettings2 temp =
            new DatabricksSparkContextCreatorNodeSettings2(m_useWorkspaceConnection);
        temp.loadSettingsForModel(settings);
        temp.validate();
    }

    private void validate() throws InvalidSettingsException {
        if (!m_useWorkspaceConnection) {
            m_authSettings.validate();
        }

        if (StringUtils.isBlank(m_workingDirectory.getStringValue())) {
            throw new InvalidSettingsException("Working directory required.");
        }
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the node dialog).
     *
     * @param settings
     * @throws InvalidSettingsException
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);
        // auth settings are loaded by dialog
        m_workingDirectory.loadSettingsFrom(settings);
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the node model).
     *
     * @param settings
     * @throws InvalidSettingsException
     */
    public void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);
        m_authSettings.loadSettingsForModel(settings);
        m_workingDirectory.loadSettingsFrom(settings);
    }

    /**
     * Create context config using the given workspace connection.
     *
     * @param contextId The ID of the Spark context for which to create the config object.
     * @param fileSystemId Identifies the staging area file system connection.
     * @param spec port spec with timeout settings
     * @param credential credential to authenticate with
     * @return a new {@link DatabricksSparkContextConfig} derived from the current settings.
     */
    public DatabricksSparkContextConfig createContextConfig(final SparkContextID contextId, final String fileSystemId,
        final DatabricksWorkspacePortObjectSpec spec, final DatabricksAccessTokenCredential credential) {

        if (!m_useWorkspaceConnection) {
            throw new IllegalArgumentException(
                "Cannot create context config, workspace connection required. This is a bug.");
        }

        final String stagingArea = isStagingAreaFolderSet() ? getStagingAreaFolder() : null;
        final int connectionTimeout = Math.toIntExact(spec.getConnectionTimeout().toSeconds());
        final int readTimeout = Math.toIntExact(spec.getReadTimeout().toSeconds());

        return new DatabricksSparkContextFileSystemConfig(getSparkVersion(), getClusterId(), credential, stagingArea,
            terminateClusterOnDestroy(), connectionTimeout, readTimeout, getJobCheckFrequency(), contextId,
            fileSystemId);
    }

    /**
     * Create context config from this settings model.
     *
     * @param contextId The ID of the Spark context for which to create the config object.
     * @param fileSystemId Identifies the staging area file system connection.
     * @param credentialsProvider credentials provider to use
     * @return a new {@link DatabricksSparkContextConfig} derived from the current settings.
     */
    public DatabricksSparkContextConfig createContextConfig(final SparkContextID contextId, final String fileSystemId,
        final CredentialsProvider credentialsProvider) {

        if (m_useWorkspaceConnection) {
            throw new IllegalArgumentException(
                "Cannot create context config, unexpected workspace connection provided. This is a bug.");
        }

        final String stagingArea = isStagingAreaFolderSet() ? getStagingAreaFolder() : null;

        if (m_authSettings.useTokenAuth()) {
            final String token = m_authSettings.getToken(credentialsProvider);
            return new DatabricksSparkContextFileSystemConfig(getSparkVersion(), getDatabricksInstanceURL(),
                getClusterId(), token, stagingArea, terminateClusterOnDestroy(), getConnectionTimeout(),
                getReceiveTimeout(), getJobCheckFrequency(), contextId, fileSystemId);
        } else {
            final String username = m_authSettings.getUser(credentialsProvider);
            final String password = m_authSettings.getPassword(credentialsProvider);
            return new DatabricksSparkContextFileSystemConfig(getSparkVersion(), getDatabricksInstanceURL(),
                getClusterId(), username, password, stagingArea, terminateClusterOnDestroy(), getConnectionTimeout(),
                getReceiveTimeout(), getJobCheckFrequency(), contextId, fileSystemId);
        }
    }

    /**
     * Create context config with dummy values.
     *
     * @param contextId The ID of the Spark context for which to create the config object.
     * @return a new {@link DatabricksSparkContextConfig} with dummy settings
     */
    DatabricksSparkContextConfig createDummyContextConfig(final SparkContextID contextId) {
        final String token = "dummy-token";
        final String stagingArea = null;
        final String fileSystemId = "dummy-file-system-id";

        return new DatabricksSparkContextFileSystemConfig(getSparkVersion(), getDatabricksInstanceURL(), getClusterId(),
            token, stagingArea, terminateClusterOnDestroy(), getConnectionTimeout(), getReceiveTimeout(),
            getJobCheckFrequency(), contextId, fileSystemId);
    }

    /**
     * Create a Databricks file system connection spec from this settings.
     *
     * @param fsId file system connection identifier
     * @param config the file system config
     * @return databricks file system port object spec
     * @throws InvalidSettingsException
     */
    public FileSystemPortObjectSpec createFileSystemSpec(final String fsId, final DbfsFSConnectionConfig config) {
        return new FileSystemPortObjectSpec( //
            DbfsFSDescriptorProvider.FS_TYPE.getTypeId(), //
            fsId, //
            config.createFSLocationSpec());
    }

    /**
     * Create a Databricks file system connection from this settings model.
     *
     * @param config the file system config
     * @return The DBFS connection information object
     *
     * @throws IOException
     * @throws InvalidSettingsException
     */
    public FSConnection createDatabricksFSConnection(final DbfsFSConnectionConfig config) throws IOException {
        return new DbfsFSConnection(config);
    }

    /**
     * Create file system config using a workspace connection.
     *
     * @param spec port spec with timeout settings
     * @param credential credential to authenticate with
     * @return file system settings
     */
    public DbfsFSConnectionConfig createDbfsFSConnectionConfig(final DatabricksWorkspacePortObjectSpec spec,
        final DatabricksAccessTokenCredential credential) {

        if (!m_useWorkspaceConnection) {
            throw new IllegalArgumentException(
                "Cannot create FS config, workspace connection required. This is a bug.");
        }

        final DbfsFSConnectionConfig.Builder builder = DbfsFSConnectionConfig.builder() //
            .withCredential(credential) //
            .withWorkingDirectory(getWorkingDirectory()) //
            .withConnectionTimeout(spec.getConnectionTimeout()) //
            .withReadTimeout(spec.getReadTimeout());

        return builder.build();
    }

    /**
     * Create file system config using this settings model.
     *
     * @param credentialsProvider Provider for the credentials
     * @return file system settings
     * @throws InvalidSettingsException
     */
    public DbfsFSConnectionConfig createDbfsFSConnectionConfig(final CredentialsProvider credentialsProvider)
        throws InvalidSettingsException {

        if (m_useWorkspaceConnection) {
            throw new IllegalArgumentException(
                "Cannot create FS config, unexpected workspace connection provided. This is a bug.");
        }

        final DbfsConnectorNodeSettings settings = new DbfsConnectorNodeSettings(false);
        settings.getUrlModel().setStringValue(getDatabricksInstanceURL());
        settings.getWorkingDirectoryModel().setStringValue(getWorkingDirectory());
        settings.getConnectionTimeoutModel().setIntValue(getConnectionTimeout());
        settings.getReadTimeoutModel().setIntValue(getReceiveTimeout());
        final NodeSettings tempSettings = new NodeSettings("ignored");
        m_authSettings.saveSettingsForModel(tempSettings);
        settings.getAuthenticationSettings().loadSettingsForModel(tempSettings);
        return settings.toFSConnectionConfig(credentialsProvider);
    }

    /**
     * Create settings model from testing flow variables.
     *
     * @param flowVariables variables to use
     * @return settings model
     * @throws InvalidSettingsException
     */
    public static DatabricksSparkContextCreatorNodeSettings2
        fromFlowVariables(final Map<String, FlowVariable> flowVariables) throws InvalidSettingsException {

        final DatabricksSparkContextCreatorNodeSettings2 settings =
            new DatabricksSparkContextCreatorNodeSettings2(false);
        settings.m_sparkVersion
            .setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_VERSION, flowVariables));
        settings.m_url.setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_URL, flowVariables));
        settings.m_clusterId
            .setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_CLUSTER_ID, flowVariables));
        settings.m_workspaceId
            .setStringValue(TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_WORKSPACE_ID, flowVariables));

        if (flowVariables.containsKey(TestflowVariable.SPARK_DATABRICKS_TOKEN.getName())) {
            final String token = TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_TOKEN, flowVariables);
            settings.m_authSettings.setAuthType(AuthType.TOKEN);
            settings.m_authSettings.getTokenModel().setStringValue(token);
        } else {
            final String user = TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_USERNAME, flowVariables);
            final String pass = TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_PASSWORD, flowVariables);
            settings.m_authSettings.setAuthType(AuthType.USER_PWD);
            settings.m_authSettings.getUserModel().setStringValue(user);
            settings.m_authSettings.getPasswordModel().setStringValue(pass);
        }

        settings.m_workingDirectory.setStringValue("/");

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
