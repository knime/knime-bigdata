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
package org.knime.bigdata.spark.local.node.create.utils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Objects;
import java.util.Optional;

import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;
import org.knime.bigdata.spark.local.db.LocalHiveConnectionController;
import org.knime.bigdata.spark.local.db.LocalHiveConnectorSettings;
import org.knime.bigdata.spark.local.node.create.AbstractLocalEnvironmentCreatorNodeModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
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
import org.knime.database.session.DBSessionCache;
import org.knime.database.session.DBSessionID;
import org.knime.database.session.DBSessionInformation;
import org.knime.database.session.impl.DefaultDBSessionInformation;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

/**
 * Utility to create {@link DBSessionPortObject} based file system output ports.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateDBSessionPortUtil implements CreateLocalBDEPortUtil {

    /**
     * Output port type of this utility.
     */
    public static final PortType PORT_TYPE = DBSessionPortObject.TYPE;

    private final AbstractLocalEnvironmentCreatorNodeModel m_nodeModel;

    private final NodeLogger m_logger;

    private final VariableContext m_variableContext;

    private LocalHiveConnectorSettings m_hiveSettings;

    private DBSessionInformation m_sessionInfo;

    /**
     * Default constructor.
     *
     * @param nodeModel calling node model
     * @param variableContext variables context of calling node model
     */
    public CreateDBSessionPortUtil(final AbstractLocalEnvironmentCreatorNodeModel nodeModel, final VariableContext variableContext) {
        m_logger = nodeModel.getNodeLogger();
        m_nodeModel = nodeModel;
        m_variableContext = variableContext;
    }

    @Override
    public PortObjectSpec configure() {
        return null;
    }

    @Override
    public PortObject execute(final LocalSparkContext sparkContext, final ExecutionContext exec) throws Exception {
        final int hiveserverPort = sparkContext.getHiveserverPort();
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
            m_nodeModel.setNodeWarning("The built-in DB type could not be found and the default is used instead.");
            m_logger.warnWithFormat(
                "The built-in DB type [%s] could not be found and the default [%s] is used instead.",
                m_hiveSettings.getDBType(), substitute.getId());
            return substitute;
        });

        final Optional<DBSQLDialectFactory> dialectFactory =
                DBSQLDialectRegistry.getInstance().getFactory(dbType, m_hiveSettings.getDialect());
        final String dialectId = dialectFactory.orElseGet(() -> {
            final DBSQLDialectFactory substitute = DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(dbType);
            m_nodeModel.setNodeWarning("The built-in SQL dialect could not be found and the default is used instead.");
            m_logger.warnWithFormat(
                "The built-in SQL dialect [%s] could not be found and the default [%s] is used instead.",
                m_hiveSettings.getDialect(), substitute.getId());
            return substitute;
        }).getId();

        final DBDriverWrapper driver =
                DBDriverRegistry.getInstance().getDriver(m_hiveSettings.getDriver()).orElseGet(() -> {
            final DBDriverWrapper substitute = DBDriverRegistry.getInstance().getLatestDriver(dbType);
            m_nodeModel.setNodeWarning("The built-in driver could not be found and the latest "
                    + "for the chosen database type is used instead.");
            m_logger.warnWithFormat("The built-in driver [%s] could not be found and the latest [%s] is used instead.",
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
            m_hiveSettings = null;
        }
    }

    @Override
    public void loadInternals(final File nodeInternDir, final ExecutionMonitor exec, final LocalSparkContext sparkContext,
        final LocalSparkContextConfig sparkContextConfig) throws IOException, CanceledExecutionException {

        try {
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

    @Override
    public void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        if (m_sessionInfo != null) {
            DBSessionInternalsSerializer.saveToInternals(m_sessionInfo, nodeInternDir);
        }
    }

}
