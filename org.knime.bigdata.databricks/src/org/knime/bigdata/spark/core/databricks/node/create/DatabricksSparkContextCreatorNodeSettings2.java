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
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings.AuthType;
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

    private final DbfsAuthenticationNodeSettings m_authSettings =
        new DbfsAuthenticationNodeSettings("auth", AuthType.TOKEN);

    private final SettingsModelString m_workingDirectory = new SettingsModelString("workingDirectory", "/");

    /**
     * Constructor.
     */
    DatabricksSparkContextCreatorNodeSettings2() {
        super();
    }

    @Override
    protected AbstractDatabricksSparkContextCreatorNodeSettings createTestingInstance() {
        return new DatabricksSparkContextCreatorNodeSettings2();
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
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        m_authSettings.validateSettings(settings);
        m_workingDirectory.validateSettings(settings);

        final DatabricksSparkContextCreatorNodeSettings2 temp = new DatabricksSparkContextCreatorNodeSettings2();
        temp.loadSettingsForModel(settings);
        temp.validate();
    }

    private void validate() throws InvalidSettingsException {
        m_authSettings.validate();

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
     * @param contextId The ID of the Spark context for which to create the config object.
     * @param fileSystemId Identifies the staging area file system connection.
     * @param credentialsProvider credentials provider to use
     * @return a new {@link DatabricksSparkContextConfig} derived from the current settings.
     * @throws InvalidSettingsException on unknown authentication method or empty username or password
     */
    public DatabricksSparkContextConfig createContextConfig(final SparkContextID contextId, final String fileSystemId,
        final CredentialsProvider credentialsProvider) throws InvalidSettingsException {

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
     * Create a Databricks file system connection spec from this settings.
     *
     * @param fsId file system connection identifier
     * @param credentialsProvider Provider for the credentials
     * @return databricks file system port object spec
     * @throws InvalidSettingsException
     */
    public FileSystemPortObjectSpec createFileSystemSpec(final String fsId,
        final CredentialsProvider credentialsProvider) throws InvalidSettingsException {

        return new FileSystemPortObjectSpec( //
            DbfsFSDescriptorProvider.FS_TYPE.getTypeId(), //
            fsId, //
            createDbfsFSConnectionConfig(credentialsProvider).createFSLocationSpec());
    }

    /**
     * Create a Databricks file system connection from this settings.
     *
     * @param credentialsProvider Provider for the credentials
     * @return The DBFS connection information object
     *
     * @throws IOException
     * @throws InvalidSettingsException
     */
    public FSConnection createDatabricksFSConnection(final CredentialsProvider credentialsProvider)
        throws IOException, InvalidSettingsException {

        return new DbfsFSConnection(createDbfsFSConnectionConfig(credentialsProvider));
    }

    private DbfsFSConnectionConfig createDbfsFSConnectionConfig(final CredentialsProvider credentialsProvider)
        throws InvalidSettingsException {

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

        final DatabricksSparkContextCreatorNodeSettings2 settings = new DatabricksSparkContextCreatorNodeSettings2();
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
