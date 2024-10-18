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

import java.net.URI;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.dbfs.filehandler.DBFSRemoteFileHandler;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConnInfoConfig;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Settings class for the "Create Spark Context (Databricks)" node using {@link ConnectionInformation} base remote
 * files.
 *
 * This class contains in addition methods to create: - {@link SparkContextID} from a instance -
 * {@link DatabricksSparkContextConfig} from a instance - DBFS {@link ConnectionInformation} from a instance - a
 * instance from flowvariables
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContextCreatorNodeSettings extends AbstractDatabricksSparkContextCreatorNodeSettings {

    final SettingsModelAuthentication m_authentication =
        new SettingsModelAuthentication("authentication", AuthenticationType.PWD);

    /**
     * Constructor.
     */
    DatabricksSparkContextCreatorNodeSettings() {
        super();
    }

    @Override
    protected AbstractDatabricksSparkContextCreatorNodeSettings createTestingInstance() {
        return new DatabricksSparkContextCreatorNodeSettings();
    }

    /**
     * @return the settings model for the authentication
     */
    protected SettingsModelAuthentication getAuthenticationModel() {
        return m_authentication;
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
            return m_authentication.getPassword();
        } else if (m_authentication.getAuthenticationType() == AuthenticationType.CREDENTIALS) {
            return m_authentication.getPassword(credentialsProvider);
        } else {
            throw new InvalidSettingsException("Unknown authentication method.");
        }
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        m_authentication.saveSettingsTo(settings);
    }

    @Override
    public void validateDeeper() throws InvalidSettingsException {
        validateDeeper(false);
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        m_authentication.validateSettings(settings);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);
        m_authentication.loadSettingsFrom(settings);
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
            return new DatabricksSparkContextConnInfoConfig(getSparkVersion(), getDatabricksInstanceURL(), getClusterId(),
                password, stagingArea, terminateClusterOnDestroy(), getConnectionTimeout(), getReceiveTimeout(),
                getJobCheckFrequency(), contextId, connInfo);
        } else {
            return new DatabricksSparkContextConnInfoConfig(getSparkVersion(), getDatabricksInstanceURL(), getClusterId(),
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
