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
package org.knime.bigdata.databricks.sparksql;

import static org.knime.datatype.mapping.DataTypeMappingDirection.EXTERNAL_TO_KNIME;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.database.databricks.DatabricksDBDriverLocator;
import org.knime.bigdata.database.databricks.DatabricksOAuth2DBConnectionController;
import org.knime.bigdata.database.databricks.DatabricksUserDBConnectionController;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenType;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.rest.clusters.ClusterState;
import org.knime.bigdata.databricks.rest.libraries.LibrariesAPI;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObject;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.util.exception.ClientErrorAccessException;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.database.DBType;
import org.knime.database.VariableContext;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.datatype.mapping.DBDestination;
import org.knime.database.datatype.mapping.DBSource;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.driver.URLTemplates;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionCache;
import org.knime.database.session.DBSessionID;
import org.knime.database.session.DBSessionInformation;
import org.knime.database.session.impl.DefaultDBSessionInformation;
import org.knime.database.util.StringTokenException;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

import jakarta.ws.rs.NotFoundException;

/**
 * Node model for the Databricks Spark SQL Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings({"restriction", "deprecation"})
public class SparkSQLConnectorNodeModel extends WebUINodeModel<SparkSQLConnectorSettings> {

    private class NodeModelVariableContext implements VariableContext {

        @Override
        public ICredentials getCredentials(final String id) {
            return getCredentialsProvider().get(id);
        }

        @Override
        public Collection<String> getCredentialsIds() {
            return getCredentialsProvider().listNames();
        }

        @Override
        @Deprecated
        public Map<String, FlowVariable> getInputFlowVariables() {
            return getAvailableInputFlowVariables();
        }

        @Override
        public Map<String, FlowVariable> getInputFlowVariables(final VariableType<?>[] types) {
            return getAvailableFlowVariables(types);
        }

    }

    private static final DBType DB_TYPE = Databricks.DB_TYPE;

    private final VariableContext m_variableContext = new NodeModelVariableContext();

    private DBSessionInformation m_sessionInfo;


    SparkSQLConnectorNodeModel(final WebUINodeConfiguration configuration) {
        super(configuration, SparkSQLConnectorSettings.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
        final SparkSQLConnectorSettings settings) throws InvalidSettingsException {

        if (inSpecs[0] != null) {
            if (inSpecs[0] instanceof DatabricksWorkspacePortObjectSpec spec) {
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

        settings.validateOnConfigure();

        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final SparkSQLConnectorSettings settings) throws Exception {

        final var clusterId = settings.m_clusterId;
        final var spec = ((DatabricksWorkspacePortObject)inObjects[0]).getSpec();

        final var credential = spec.resolveCredential(DatabricksAccessTokenCredential.class);

        final var clusterAPI = DatabricksRESTClient //
            .create(credential, ClusterAPI.class, spec.getReadTimeout(), spec.getConnectionTimeout());
        final var librariesAPI = DatabricksRESTClient //
            .create(credential, LibrariesAPI.class, spec.getReadTimeout(), spec.getConnectionTimeout());

        // Verify the cluster exists and retrieve its info
        getClusterInfo(clusterAPI, clusterId);

        // Ensure the cluster is running (starts it if terminated), waiting until it reaches RUNNING state
        ClusterUtil.ensureClusterRunning(clusterAPI, librariesAPI, clusterId, exec);

        // Retrieve the workspace/org ID from the cluster response header
        final var workspaceId = getWorkspaceId(clusterAPI, clusterId);

        // Create a cluster status provider that checks if the cluster is running
        final DatabricksClusterStatusProvider clusterStatus =
            () -> clusterAPI.getCluster(clusterId).state == ClusterState.RUNNING;

        // Build the JDBC URL from the driver template and the workspace URL host
        final var driver = getDriver();
        final var jdbcUrl = getJdbcUrl(driver, credential);

        // Create the DB connection controller using the cluster's thrift/JDBC endpoint
        final var dbController =
            createConnectionController(jdbcUrl, clusterStatus, clusterId, workspaceId, credential);

        m_sessionInfo = createSessionInfo(driver, dbController);
        final var session = registerSession(exec);

        final DBTypeMappingService<? extends DBSource, ? extends DBDestination> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType());

        return new PortObject[]{new DBSessionPortObject(session.getSessionInformation(), //
            DataTypeMappingConfigurationData //
                .from(mappingService.createDefaultMappingConfiguration(KNIME_TO_EXTERNAL)), //
            DataTypeMappingConfigurationData //
                .from(mappingService.createDefaultMappingConfiguration(EXTERNAL_TO_KNIME)))};
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

        try (final var resp = clusterAPI.getClusterResponse(clusterId)) {
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

    private static DBDriverWrapper getDriver() throws InvalidSettingsException {
        final String driverId = DatabricksDBDriverLocator.getLatestSimbaOrHiveDriverID();
        return DBDriverRegistry.getInstance().getDriver(driverId)
            .orElseThrow(() -> new InvalidSettingsException("Unable to find DB driver: " + driverId));
    }

    /**
     * Build the JDBC URL from the driver's URL template and the Databricks workspace URL host.
     */
    private static String getJdbcUrl(final DBDriverWrapper driver,
        final DatabricksAccessTokenCredential credential) throws InvalidSettingsException {

        try {
            final String host = credential.getDatabricksWorkspaceUrl().getHost();
            final Map<String, String> variableValues = Map.of(
                URLTemplates.VARIABLE_NAME_HOST, host,
                URLTemplates.VARIABLE_NAME_PORT, "443");
            return URLTemplates.resolveDriverUrl(driver.getURLTemplate(), variableValues, variableValues);
        } catch (final StringTokenException e) {
            throw new InvalidSettingsException("Unable to resolve JDBC URL from driver template.", e);
        }
    }

    /**
     * Creates a DB connection controller for the cluster's thrift/JDBC endpoint. Handles both personal access token
     * and OAuth2 authentication. The httpPath is constructed using the format {@code sql/protocolv1/o/<orgId>/<clusterId>}.
     */
    private static DBConnectionController createConnectionController(final String jdbcUrl,
        final DatabricksClusterStatusProvider clusterStatus, final String clusterId, final String workspaceId,
        final DatabricksAccessTokenCredential credential) throws InvalidSettingsException {

        final var httpPath = getHttpPath(clusterId, workspaceId);
        final var description = String.format("cluster=\"%s\"", clusterId);

        if (credential.getDatabricksTokenType() == DatabricksAccessTokenType.PERSONAL_ACCESS_TOKEN) {
            final var user = "token";
            final var password = getPersonalAccessToken(credential);
            return new DatabricksUserDBConnectionController(jdbcUrl, httpPath, clusterStatus, description, user,
                password);
        } else {
            return new DatabricksOAuth2DBConnectionController(jdbcUrl, httpPath, clusterStatus, description,
                credential);
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

    private static DBSessionInformation createSessionInfo(final DBDriverWrapper driver,
        final DBConnectionController connectionController) {

        final String dialectId = DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(DB_TYPE).getId();
        final Map<String, ? extends Serializable> attributeValues = Collections.emptyMap();
        return new DefaultDBSessionInformation(DB_TYPE, dialectId, new DBSessionID(), driver.getDriverDefinition(),
            connectionController, attributeValues);
    }

    private DBSession registerSession(final ExecutionMonitor monitor) throws CanceledExecutionException, SQLException {
        Objects.requireNonNull(m_sessionInfo, "m_sessionInfo must not be null");

        final DBSession session = DBSessionCache.getInstance().getOrCreate(m_sessionInfo, m_variableContext, monitor);
        session.validate(monitor);
        return session;
    }

    @Override
    protected void onDispose() {
        destroySession();
    }

    @Override
    protected void reset() {
        destroySession();
    }

    private void destroySession() {
        if (m_sessionInfo != null) {
            m_sessionInfo.getConnectionController().cleanup();
            DBSessionCache.getInstance().destroy(m_sessionInfo.getID());
            m_sessionInfo = null;
        }
    }
}
