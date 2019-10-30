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
 */
package org.knime.bigdata.testing.node.create;

import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.database.databricks.testing.DatabricksTestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.database.hive.Hive;
import org.knime.bigdata.database.hive.testing.TestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.local.db.LocalHiveConnectionController;
import org.knime.bigdata.spark.local.testing.LocalHiveTestingConnectionSettingsFactory;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
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
 * Node model for the "Create Big Data Test Environment" node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateTestEnvironmentNodeModel2 extends AbstractCreateTestEnvironmentNodeModel {
    
    private static final String LOCAL_SPARK_HIVE_DIALECT = "hive";
    private static final String LOCAL_SPARK_HIVE_DRIVER = "hive";
    

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
        public Map<String, FlowVariable> getInputFlowVariables() {
            return getAvailableInputFlowVariables();
        }

    }

    private final VariableContext m_variableContext = new NodeModelVariableContext();

    private DBSessionInformation m_sessionInfo;

    /**
     * Constructor.
     */
    CreateTestEnvironmentNodeModel2() {
        super(new PortType[]{}, new PortType[]{DBSessionPortObject.TYPE, ConnectionInformationPortObject.TYPE,
            SparkContextPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec createHivePortSpec(final SparkContextIDScheme sparkScheme,
        final Map<String, FlowVariable> flowVars) throws InvalidSettingsException {

        if (sparkScheme == SparkContextIDScheme.SPARK_LOCAL && !isLocalSparkWithThriftserver(flowVars)) {
            return InactiveBranchPortObjectSpec.INSTANCE;
        } else {
            return null;
        }
    }

    @Override
    protected PortObject createHivePort(final ExecutionContext exec, final SparkContextIDScheme sparkScheme,
        final Map<String, FlowVariable> flowVars) throws Exception {

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
                final CredentialsProvider credentialsProvider = getCredentialsProvider();
                connectionController = new UserDBConnectionController(hiveSettings.getDBUrl(),
                    authentication.getAuthenticationType(),
                    authentication.getUserName(credentialsProvider),
                    authentication.getPassword(credentialsProvider),
                    authentication.getCredential(), credentialsProvider);
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

    private DBSession registerSession(final ExecutionMonitor monitor, final DBSessionSettings settings, final DBConnectionController connectionController)
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
    private DBSessionInformation createSessionInfo(final DBSessionSettings settings, final DBConnectionController connectionController) throws InvalidSettingsException {

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
                setWarningMessage("The selected SQL dialect could not be found and the default is used instead.");
                LOGGER.warnWithFormat(
                    "The selected SQL dialect [%s] could not be found and the default [%s] is used instead.",
                    settingsDialect, dialectId);
            }
        }

        final String sessionDriver = settings.getDriver();
        final DBDriverRegistry driverRegistry = DBDriverRegistry.getInstance();
        final DBDriverWrapper driver = driverRegistry.getDriver(sessionDriver).orElseGet(() -> {
            final DBDriverWrapper substitute = driverRegistry.getLatestDriver(dbType);
            if (StringUtils.isNotEmpty(sessionDriver)) {
                setWarningMessage("The selected driver could not be found and the latest "
                        + "for the chosen database type is used instead.");
                LOGGER.warnWithFormat(
                    "The selected driver [%s] could not be found and the latest [%s] is used instead.", sessionDriver,
                    substitute.getDriverDefinition().getId());
            }
            return substitute;
        });

        return new DefaultDBSessionInformation(dbType, dialectId, sessionID, driver.getDriverDefinition(),
            connectionController, settings.getAttributeValues());
    }

    @Override
    protected void onDisposeInternal() {
        super.onDisposeInternal();
        if (m_sessionInfo != null) {
            DBSessionCache.getInstance().destroy(m_sessionInfo.getID());
            m_sessionInfo = null;
        }
    }
}
