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
 *   Created on 29.05.2019 by Mareike
 */
package org.knime.bigdata.spark.local.node.create;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;
import org.knime.bigdata.spark.local.db.LocalHiveConnectionController;
import org.knime.bigdata.spark.local.db.LocalHiveConnectorSettings;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.database.DBType;
import org.knime.database.DBTypeRegistry;
import org.knime.database.VariableContext;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.datatype.mapping.DBDestination;
import org.knime.database.datatype.mapping.DBSource;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.node.connector.DBSessionInternalsSerializer;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionID;
import org.knime.database.session.DBSessionInformation;
import org.knime.database.session.impl.DBSessionCache;
import org.knime.database.session.impl.DefaultDBSessionInformation;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

/**
 * Node model for the "Create Local Big Data Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class LocalEnvironmentCreatorNodeModel2 extends AbstractLocalEnvironmentCreatorNodeModel {

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

    private LocalHiveConnectorSettings m_hiveSettings;

    private DBSessionInformation m_sessionInfo;

    /**
     * Constructor.
     */
    LocalEnvironmentCreatorNodeModel2() {
        super(new PortType[]{}, new PortType[]{DBSessionPortObject.TYPE, ConnectionInformationPortObject.TYPE,
            SparkContextPortObject.TYPE});
    }

    @Override
    protected PortObject createDBPort(final ExecutionContext exec, final int hiveserverPort)
            throws CanceledExecutionException, SQLException, InvalidSettingsException {

        m_hiveSettings = new LocalHiveConnectorSettings(hiveserverPort);
        m_sessionInfo = createSessionInfo();
        final DBSession session = registerSession(exec);
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

    private DBSession registerSession(final ExecutionMonitor monitor) throws CanceledExecutionException, SQLException {
        Objects.requireNonNull(m_sessionInfo, "m_sessionInfo must not be null");
        final DBSession session = DBSessionCache.getInstance().getOrCreate(m_sessionInfo, m_variableContext, monitor);
        session.validate(monitor);
        return session;
    }

    /**
     * Creates the description of the session to be created and registered.
     *
     * @return the {@link DBSessionInformation} describing the session to be created and registered.
     * @throws InvalidSettingsException if any of the settings is not valid.
     */
    private DBSessionInformation createSessionInfo() throws InvalidSettingsException {

        final Optional<DBType> dbTypeOpt = DBTypeRegistry.getInstance().getRegisteredDBType(m_hiveSettings.getDBType());
        final DBType dbType = dbTypeOpt.orElseGet(() -> {
            final DBType substitute = DBType.DEFAULT;
            setWarningMessage("The built-in DB type could not be found and the default is used instead.");
            LOGGER.warnWithFormat(
                "The built-in DB type [%s] could not be found and the default [%s] is used instead.",
                m_hiveSettings.getDBType(), substitute.getId());
            return substitute;
        });

        final Optional<DBSQLDialectFactory> dialectFactory =
                DBSQLDialectRegistry.getInstance().getFactory(dbType, m_hiveSettings.getDialect());
        final String dialectId = dialectFactory.orElseGet(() -> {
            final DBSQLDialectFactory substitute = DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(dbType);
            setWarningMessage("The built-in SQL dialect could not be found and the default is used instead.");
            LOGGER.warnWithFormat(
                "The built-in SQL dialect [%s] could not be found and the default [%s] is used instead.",
                m_hiveSettings.getDialect(), substitute.getId());
            return substitute;
        }).getId();

        final DBDriverWrapper driver =
                DBDriverRegistry.getInstance().getDriver(m_hiveSettings.getDriver()).orElseGet(() -> {
            final DBDriverWrapper substitute = DBDriverRegistry.getInstance().getLatestDriver(dbType);
            setWarningMessage("The built-in driver could not be found and the latest "
                    + "for the chosen database type is used instead.");
            LOGGER.warnWithFormat("The built-in driver [%s] could not be found and the latest [%s] is used instead.",
                m_hiveSettings.getDriver(), substitute.getDriverDefinition().getId());
            return substitute;
        });

        final DBConnectionController connectionController = createConnectionController(m_hiveSettings.getDBUrl());
        return new DefaultDBSessionInformation(dbType, dialectId, new DBSessionID(), driver.getDriverDefinition(),
            connectionController, m_hiveSettings.getAttributeValues());
    }

    private static DBConnectionController createConnectionController(final String jdbcURL) {
        return new LocalHiveConnectionController(jdbcURL);
    }

    @Override
    protected void onDisposeInternal() {
        super.onDisposeInternal();
        destroySession();
    }

    @Override
    protected void resetInternal() {
        super.resetInternal();
        destroySession();
    }

    private void destroySession() {
        if (m_sessionInfo != null) {
            DBSessionCache.getInstance().destroy(m_sessionInfo.getID());
            m_sessionInfo = null;
            m_hiveSettings = null;
        }
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        super.loadAdditionalInternals(nodeInternDir, exec);

        try {
            final SparkContextID contextID = m_settings.getSparkContextID();
            final LocalSparkContext sparkContext =
                (LocalSparkContext)SparkContextManager.<LocalSparkContextConfig> getOrCreateSparkContext(contextID);
            final LocalSparkContextConfig sparkContextConfig = m_settings.createContextConfig();

            if (sparkContextConfig.startThriftserver() && sparkContext.getStatus() == SparkContextStatus.OPEN) {
                m_hiveSettings = new LocalHiveConnectorSettings(sparkContext.getHiveserverPort());
                m_sessionInfo = DBSessionInternalsSerializer.loadFromInternals(nodeInternDir,
                    t -> createConnectionController(m_hiveSettings.getDBUrl()));
                registerSession(exec);
            }
        } catch (final Exception exception) {
            throw new IOException(exception.getMessage(), exception);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        if (m_sessionInfo != null) {
            DBSessionInternalsSerializer.saveToInternals(m_sessionInfo, nodeInternDir);
        }
    }
}
