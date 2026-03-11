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
package org.knime.bigdata.databricks.sparksql.connector;

import static org.knime.database.node.connector.ConnectorMessages.DATABASE_DRIVER_IS_NOT_FOUND;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.database.databricks.DatabricksOAuth2DBConnectionController;
import org.knime.bigdata.database.databricks.DatabricksUserDBConnectionController;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenType;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.clusters.Cluster;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.rest.clusters.ClusterState;
import org.knime.bigdata.databricks.rest.libraries.LibrariesAPI;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObject;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.bigdata.spark.core.util.BackgroundTasks;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.util.exception.ClientErrorAccessException;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.database.DBType;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.driver.URLTemplates;
import org.knime.database.node.connector.SpecificDBConnectorNodeModel;
import org.knime.database.util.StringTokenException;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;

/**
 * Node model for the Databricks Spark SQL Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings({"restriction", "deprecation"})
public class SparkSQLConnectorNodeModel extends SpecificDBConnectorNodeModel<SparkSQLConnectorParameters> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkSQLConnectorNodeModel.class);

    private static final DBType DB_TYPE = Databricks.DB_TYPE;

    private DatabricksAccessTokenCredential m_credential;
    private ClusterAPI m_clusterAPI;
    private LibrariesAPI m_librariesAPI;
    private SparkSQLConnectorParameters m_executedSettings;
    private String m_workspaceId;

    SparkSQLConnectorNodeModel(final WebUINodeConfiguration configuration) {
        super(DB_TYPE, configuration, SparkSQLConnectorParameters.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
        final SparkSQLConnectorParameters settings) throws InvalidSettingsException {

        if (inSpecs[0] != null) {
            if (inSpecs[0] instanceof DatabricksWorkspacePortObjectSpec) {
                final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inSpecs[0];
                if (spec.isPresent()) {
                    try {
                        // ensure there is a Databricks access token credential
                        spec.resolveCredential(DatabricksAccessTokenCredential.class);
                    } catch (final NoSuchCredentialException ex) {
                        throw new InvalidSettingsException(ex.getMessage(), ex);
                    }
                }
            } else {
                throw new InvalidSettingsException(
                    "Incompatible input connection. Connect the Databricks Workspace Connector output port.");
            }
        }

        return super.configure(inSpecs, settings);
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final SparkSQLConnectorParameters settings) throws Exception {

        final String clusterId = settings.m_clusterId;
        final DatabricksWorkspacePortObjectSpec spec = ((DatabricksWorkspacePortObject)inObjects[0]).getSpec();

        m_credential = spec.resolveCredential(DatabricksAccessTokenCredential.class);
        m_clusterAPI = DatabricksRESTClient //
            .create(m_credential, ClusterAPI.class, spec.getReadTimeout(), spec.getConnectionTimeout());
        m_librariesAPI = DatabricksRESTClient //
            .create(m_credential, LibrariesAPI.class, spec.getReadTimeout(), spec.getConnectionTimeout());
        m_executedSettings = settings;

        // Verify the cluster exists and retrieve its info
        getClusterInfo(m_clusterAPI, clusterId);

        // Ensure the cluster is running (starts it if terminated), waiting until it reaches RUNNING state
        // Cluster cannot realistically be started through JDBC as it takes quite long to start, leading to
        // a connect timeout.
        ClusterUtil.ensureClusterRunning(m_clusterAPI, m_librariesAPI, clusterId, exec);

        m_workspaceId = getWorkspaceId(m_clusterAPI, clusterId);

        return super.execute(inObjects, exec, settings);
    }

    private static void getClusterInfo(final ClusterAPI clusterAPI, final String clusterId)
        throws Exception {

        try {
            clusterAPI.getCluster(clusterId);
        } catch (final Exception e) {
            throw handleClusterAccessError(e);
        }
    }

    /**
     * Retrieves the workspace/org ID by reading the {@code x-databricks-org-id} response header from the cluster API.
     * This is required to construct the httpPath for the JDBC connection.
     *
     * @param clusterAPI the cluster API client
     * @param clusterId the cluster ID
     * @return the workspace/org ID, or empty string if not available (e.g. on AWS with workspace ID 0)
     */
    private static String getWorkspaceId(final ClusterAPI clusterAPI, final String clusterId)
        throws Exception {

        try (final Response resp = clusterAPI.getClusterResponse(clusterId)) {
            return Optional.ofNullable(resp.getHeaderString("x-databricks-org-id")).orElse("");
        } catch (final Exception e) {
            throw handleClusterAccessError(e);
        }
    }

    private static Exception handleClusterAccessError(final Exception e) throws Exception {
        if (e instanceof NotFoundException) {
            return new Exception("Cluster not found. Verify connected workspace and selected cluster.", e);
        } else if (e instanceof ClientErrorAccessException) {
            return new Exception("Unable to access cluster: " + e.getMessage(), e);
        } else if (e instanceof IOException) {
            return new Exception("Unable to connect to cluster: " + e.getMessage(), e);
        } else {
            return e;
        }
    }

    /**
     * Build the JDBC URL from the driver's URL template and the Databricks workspace URL host.
     */
    private String getJdbcUrl(final SparkSQLConnectorParameters settings,
        final DatabricksAccessTokenCredential credential) throws InvalidSettingsException {

        final String sessionDriver = getDriverId(DB_TYPE, settings);
        final DBDriverWrapper driver = DBDriverRegistry.getInstance()//
            .getDriver(sessionDriver)//
            .orElseThrow(
                () -> new InvalidSettingsException(String.format(DATABASE_DRIVER_IS_NOT_FOUND, sessionDriver)));

        try {
            final String host = credential.getDatabricksWorkspaceUrl().getHost();
            final Map<String, String> variableValues =
                Map.of(
                URLTemplates.VARIABLE_NAME_HOST, host,
                URLTemplates.VARIABLE_NAME_PORT, "443");
            return URLTemplates.resolveDriverUrl(driver.getURLTemplate(), variableValues, variableValues);
        } catch (final StringTokenException e) {
            throw new InvalidSettingsException("Unable to resolve JDBC URL from driver template.", e);
        }
    }

    @Override
    protected DBConnectionController createConnectionController(final List<PortObject> inObjects,
        final SparkSQLConnectorParameters settings, final ExecutionMonitor exec) throws InvalidSettingsException {

        final String clusterId = settings.m_clusterId;
        final ClusterAPI clusterAPI = m_clusterAPI;

        // Create a cluster status provider that checks if the cluster is running
        final DatabricksClusterStatusProvider clusterStatus =
            () -> clusterAPI.getCluster(clusterId).state == ClusterState.RUNNING;

        // Build the JDBC URL from the driver template and the workspace URL host
        final String jdbcUrl = getJdbcUrl(settings, m_credential);

        final String httpPath = getHttpPath(clusterId, m_workspaceId);
        final String description = String.format("cluster=\"%s\"", clusterId);

        if (m_credential.getDatabricksTokenType() == DatabricksAccessTokenType.PERSONAL_ACCESS_TOKEN) {
            final String user = "token";
            final String password = getPersonalAccessToken(m_credential);
            return new DatabricksUserDBConnectionController(jdbcUrl, httpPath, clusterStatus, description, user,
                password);
        } else {
            return new DatabricksOAuth2DBConnectionController(jdbcUrl, httpPath, clusterStatus, description,
                m_credential);
        }
    }

    private static String getPersonalAccessToken(final DatabricksAccessTokenCredential credential)
        throws InvalidSettingsException {
        try {
            return credential.getAccessToken();
        } catch (final IOException e) {
            throw new InvalidSettingsException(
                "Unable to load personal access token from input connection. Restart predecessor nodes.", e);
        }
    }

    private static String getHttpPath(final String clusterId, final String workspaceId) {
        if (workspaceId == null || workspaceId.isBlank()) {
            return String.format("sql/protocolv1/o/%s/%s", 0, clusterId);
        } else {
            return String.format("sql/protocolv1/o/%s/%s", workspaceId, clusterId);
        }
    }

    @Override
    protected DBConnectionController createConnectionController(final SparkSQLConnectorParameters modelSettings)
        throws InvalidSettingsException {
        // empty dummy implementation since we are already overriding the calling method in parent class
        throw new UnsupportedOperationException();
    }

    @Override
    protected void validateSessionInfoSettings(final SparkSQLConnectorParameters modelSettings)
        throws InvalidSettingsException {
        // nothing to do here
    }

    @Override
    protected void reset() {
        terminateClusterIfConfigured();
        clearFields();
        super.reset();
    }

    @Override
    protected void onDispose() {
        terminateClusterIfConfigured();
        clearFields();
        super.onDispose();
    }

    private void clearFields() {
        m_credential = null;
        m_clusterAPI = null;
        m_librariesAPI = null;
        m_executedSettings = null;
        m_workspaceId = null;
    }

    private void terminateClusterIfConfigured() {
        if (m_executedSettings == null || !m_executedSettings.m_terminateClusterOnDisconnect) {
            return;
        }

        final ClusterAPI clusterAPI = m_clusterAPI;
        final String clusterId = m_executedSettings.m_clusterId;

        BackgroundTasks.run(() -> {
            try {
                clusterAPI.delete(Cluster.create(clusterId));
                LOGGER.info("Terminated Databricks cluster " + clusterId);
            } catch (Exception e) {
                LOGGER.warn("Failed to terminate Databricks cluster " + clusterId + ": " + e.getMessage(), e);
            }
        });
    }
}
