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
package org.knime.bigdata.testing.node.create.utils;

import static org.knime.bigdata.testing.node.create.utils.CreateTestSparkContextPortUtil.isLocalSparkWithThriftserver;

import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.database.databricks.testing.DatabricksTestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.database.hive.testing.TestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.local.db.LocalHiveConnectionController;
import org.knime.bigdata.spark.local.testing.LocalHiveTestingConnectionSettingsFactory;
import org.knime.bigdata.testing.node.create.AbstractCreateTestEnvironmentNodeModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.database.DBType;
import org.knime.database.DBTypeRegistry;
import org.knime.database.VariableContext;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.connection.UserDBConnectionController;
import org.knime.database.datatype.mapping.DBDestination;
import org.knime.database.datatype.mapping.DBSource;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.node.connector.DBSessionSettings;
import org.knime.database.node.connector.server.ServerDBConnectorSettings;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionCache;
import org.knime.database.session.DBSessionID;
import org.knime.database.session.DBSessionInformation;
import org.knime.database.session.impl.DefaultDBSessionInformation;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

/**
 * Utility to create {@link DBSessionInformation} based Hive output ports.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateTestDBSessionPortUtil implements CreateTestPortUtil {

    /**
     * Output test port type of this utility.
     */
    public static final PortType PORT_TYPE = DBSessionPortObject.TYPE;

    private final AbstractCreateTestEnvironmentNodeModel m_nodeModel;

    private final NodeLogger m_logger;

    private final VariableContext m_variableContext;

    private DBSessionInformation m_sessionInfo;

    /**
     * Default constructor.
     *
     * @param nodeModel calling node model
     * @param variableContext variables context of calling node model
     */
    public CreateTestDBSessionPortUtil(final AbstractCreateTestEnvironmentNodeModel nodeModel,
        final VariableContext variableContext) {

        m_logger = nodeModel.getNodeLogger();
        m_nodeModel = nodeModel;
        m_variableContext = variableContext;
    }

    @Override
    public PortObjectSpec configure(final SparkContextIDScheme sparkScheme,
        final Map<String, FlowVariable> flowVars) {

        if (sparkScheme == SparkContextIDScheme.SPARK_LOCAL && !isLocalSparkWithThriftserver(flowVars)) {
            return InactiveBranchPortObjectSpec.INSTANCE;
        } else {
            return null;
        }
    }

    @Override
    public PortObject execute(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars,
        final ExecutionContext exec, final CredentialsProvider credentialsProvider) throws Exception {

        final DBSessionSettings hiveSettings;
        final DBConnectionController connectionController;

        switch (sparkScheme) {
            case SPARK_LOCAL:
                if (isLocalSparkWithThriftserver(flowVars)) {
                    hiveSettings = LocalHiveTestingConnectionSettingsFactory.createDBConnectorSettings(flowVars);
                    connectionController = new LocalHiveConnectionController(hiveSettings.getDBUrl());

                } else {
                    return InactiveBranchPortObject.INSTANCE;
                }
                break;
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                hiveSettings = TestingDatabaseConnectionSettingsFactory.createHiveSettings(flowVars);
                final SettingsModelAuthentication authentication =
                    ((ServerDBConnectorSettings)hiveSettings).getAuthenticationModel();
                connectionController = new UserDBConnectionController(hiveSettings.getDBUrl(),
                    authentication.getAuthenticationType(), authentication.getUserName(credentialsProvider),
                    authentication.getPassword(credentialsProvider), authentication.getCredential(),
                    credentialsProvider);
                break;
            case SPARK_DATABRICKS:
                hiveSettings = DatabricksTestingDatabaseConnectionSettingsFactory.createSettings(flowVars);
                connectionController = DatabricksTestingDatabaseConnectionSettingsFactory
                    .createDBConnectionController(hiveSettings.getDBUrl(), flowVars);
                break;
            default:
                throw new InvalidSettingsException("Spark context ID scheme not supported: " + sparkScheme);
        }

        final DBSession session = registerSession(exec, hiveSettings, connectionController);
        final DBType dbType = session.getDBType();
        final DBTypeMappingService<? extends DBSource, ? extends DBDestination> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(dbType);
        final DataTypeMappingConfiguration<SQLType> knimeToExternal =
            mappingService.newDefaultKnimeToExternalMappingConfiguration();
        final DataTypeMappingConfiguration<SQLType> externalToKnime =
            mappingService.newDefaultExternalToKnimeMappingConfiguration();

        return new DBSessionPortObject(session.getID(), DataTypeMappingConfigurationData.from(knimeToExternal),
            DataTypeMappingConfigurationData.from(externalToKnime));
    }

    private DBSession registerSession(final ExecutionMonitor monitor, final DBSessionSettings settings,
        final DBConnectionController connectionController)
        throws CanceledExecutionException, SQLException, InvalidSettingsException {

        m_sessionInfo = createSessionInfo(settings, connectionController);
        Objects.requireNonNull(m_sessionInfo, "m_sessionInfo must not be null");
        final DBSession session = DBSessionCache.getInstance().getOrCreate(m_sessionInfo, m_variableContext, monitor);
        session.validate(monitor);
        return session;
    }

    /**
     * Creates the description of the session to be created and registered.
     *
     * @return the {@link DBSessionInformation} describing the session to be created and registered.
     * @throws InvalidSettingsException
     */
    private DBSessionInformation createSessionInfo(final DBSessionSettings settings,
        final DBConnectionController connectionController) throws InvalidSettingsException {

        final DBSessionID sessionID = new DBSessionID();
        final DBType dbType = DBTypeRegistry.getInstance().getRegisteredDBType(settings.getDBType())
            .orElseThrow(() -> new InvalidSettingsException("Unknown DB type: " + settings.getDBType()));

        final String settingsDialect = settings.getDialect();
        final Optional<DBSQLDialectFactory> dialectFactory =
            DBSQLDialectRegistry.getInstance().getFactory(dbType, settingsDialect);
        final String dialectId;
        if (dialectFactory.isPresent()) {
            dialectId = dialectFactory.get().getId();
        } else {
            dialectId = DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(dbType).getId();
            if (StringUtils.isNotEmpty(settingsDialect)) {
                m_nodeModel
                    .setNodeWarning("The selected SQL dialect could not be found and the default is used instead.");
                m_logger.warnWithFormat(
                    "The selected SQL dialect [%s] could not be found and the default [%s] is used instead.",
                    settingsDialect, dialectId);
            }
        }

        final String sessionDriver = settings.getDriver();
        final DBDriverRegistry driverRegistry = DBDriverRegistry.getInstance();
        final DBDriverWrapper driver = driverRegistry.getDriver(sessionDriver).orElseGet(() -> {
            final DBDriverWrapper substitute = driverRegistry.getLatestDriver(dbType);
            if (StringUtils.isNotEmpty(sessionDriver)) {
                m_nodeModel.setNodeWarning("The selected driver could not be found and the latest "
                    + "for the chosen database type is used instead.");
                m_logger.warnWithFormat(
                    "The selected driver [%s] could not be found and the latest [%s] is used instead.", sessionDriver,
                    substitute.getDriverDefinition().getId());
            }
            return substitute;
        });

        return new DefaultDBSessionInformation(dbType, dialectId, sessionID, driver.getDriverDefinition(),
            connectionController, settings.getAttributeValues());
    }

    @Override
    public void onDispose() {
        destroySession();
    }

    @Override
    public void reset() {
        destroySession();
    }

    private void destroySession() {
        if (m_sessionInfo != null) {
            DBSessionCache.getInstance().destroy(m_sessionInfo.getID());
            m_sessionInfo = null;
        }
    }
}
