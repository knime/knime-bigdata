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
package org.knime.bigdata.databricks.sqlwarehouse;

import static org.knime.datatype.mapping.DataTypeMappingDirection.EXTERNAL_TO_KNIME;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.database.databricks.DatabricksDBDriverLocator;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseAPI;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseInfo;
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
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionCache;
import org.knime.database.session.DBSessionID;
import org.knime.database.session.DBSessionInformation;
import org.knime.database.session.impl.DefaultDBSessionInformation;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

import jakarta.ws.rs.NotFoundException;

/**
 * Connector node for the Databricks SQL Warehouse.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
public class SQLWarehouseConnectorNodeModel extends WebUINodeModel<SQLWarehouseConnectorSettings> {

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

    SQLWarehouseConnectorNodeModel(final WebUINodeConfiguration configuration) {
        super(configuration, SQLWarehouseConnectorSettings.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
        final SQLWarehouseConnectorSettings settings)
        throws InvalidSettingsException {

        if (inSpecs[0] != null) {
            final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inSpecs[0];
            if (spec.isPresent()) {
                try {
                    // ensure there is a Databricks access token credential
                    spec.resolveCredential(DatabricksAccessTokenCredential.class);
                } catch (final NoSuchCredentialException ex) {
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
            }
        }

        if (getSettings().isPresent()) {
            getSettings().get().validate();
        } else {
            throw new InvalidSettingsException("Please select a SQL Warehouse in the configuration dialog.");
        }

        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final SQLWarehouseConnectorSettings settings) throws Exception {

        final String id = getSettings().orElseThrow().m_warehouseId;

        final DatabricksWorkspacePortObjectSpec spec = ((DatabricksWorkspacePortObject)inObjects[0]).getSpec();
        final DatabricksAccessTokenCredential credential =
            spec.resolveCredential(DatabricksAccessTokenCredential.class);

        final DBDriverWrapper driver = getDriver();
        final DBConnectionController controller =
            createConnectionController(id, credential, driver, spec.getReadTimeout(), spec.getConnectionTimeout());
        m_sessionInfo = createSessionInfo(driver, controller);
        final DBSession session = registerSession(exec);

        final DBTypeMappingService<? extends DBSource, ? extends DBDestination> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType());

        return new PortObject[]{new DBSessionPortObject(session.getSessionInformation(), //
            DataTypeMappingConfigurationData //
                .from(mappingService.createDefaultMappingConfiguration(KNIME_TO_EXTERNAL)), //
            DataTypeMappingConfigurationData //
                .from(mappingService.createDefaultMappingConfiguration(EXTERNAL_TO_KNIME)))};
    }

    private static DBDriverWrapper getDriver() throws InvalidSettingsException {
        final String driverId = DatabricksDBDriverLocator.getLatestSimbaOrHiveDriverID();
        return DBDriverRegistry.getInstance().getDriver(driverId)
            .orElseThrow(() -> new InvalidSettingsException("Unable to find DB driver: " + driverId));
    }

    private static DBConnectionController createConnectionController(final String id,
        final DatabricksAccessTokenCredential credential, final DBDriverWrapper driver, final Duration receiveTimeout,
        final Duration connectionTimeout) throws InvalidSettingsException {

        final SQLWarehouseAPI client = DatabricksRESTClient //
            .create(credential, SQLWarehouseAPI.class, receiveTimeout, connectionTimeout);
        final SQLWarehouseInfo info = getWarehouseInfo(client, id);
        final DatabricksClusterStatusProvider clusterStatus = () -> client.getWarehouse(id).isClusterRunning();

        return SQLWarehouseDBControllerFactory.createController(driver, info, credential, clusterStatus);
    }

    private static SQLWarehouseInfo getWarehouseInfo(final SQLWarehouseAPI client, final String id)
        throws InvalidSettingsException {

        try {
            return client.getWarehouse(id);
        } catch (final NotFoundException e) {
            throw new InvalidSettingsException(
                "Warehouse not found. Verify connected Workspace and selected warehouse.", e);
        } catch (final IOException e) {
            throw new InvalidSettingsException("Unable to connect to Warehouse.", e);
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
