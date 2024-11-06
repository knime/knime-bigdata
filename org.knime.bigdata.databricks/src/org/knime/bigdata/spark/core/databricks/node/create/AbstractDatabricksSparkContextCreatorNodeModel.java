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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.util.BackgroundTasks;
import org.knime.bigdata.spark.node.util.context.DestroyAndDisposeSparkContextTask;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.VariableType;
import org.knime.database.DBType;
import org.knime.database.DBTypeRegistry;
import org.knime.database.VariableContext;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.datatype.mapping.DBDestination;
import org.knime.database.datatype.mapping.DBSource;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.node.datatype.mapping.SettingsModelDatabaseDataTypeMapping;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionCache;
import org.knime.database.session.DBSessionID;
import org.knime.database.session.DBSessionInformation;
import org.knime.database.session.impl.DefaultDBSessionInformation;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.filehandling.core.util.CheckedExceptionSupplier;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

/**
 * Node model of the "Create Spark Context (Databricks)" node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public abstract class AbstractDatabricksSparkContextCreatorNodeModel<T extends AbstractDatabricksSparkContextCreatorNodeSettings> extends SparkNodeModel {

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

    final VariableContext m_variableContext = new NodeModelVariableContext();

    final T m_settings;

    /**
     * Created using the {@link DatabricksSparkContextCreatorNodeSettings#m_uniqueContextId} during the configure phase.
     */
    protected SparkContextID m_sparkContextId;

    /**
     * Contains DB port session if enabled in settings or <code>null</code>.
     */
    protected DBSessionInformation m_sessionInfo;

    /**
     * Constructor.
     *
     * @param inPortTypes The input port types.
     * @param outPortTypes The output port types.
     */
    AbstractDatabricksSparkContextCreatorNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final T settings) {

        super(inPortTypes, outPortTypes);
        m_settings = settings;
        resetContextID();
    }

    private void resetContextID() {
        // An ID that is unique to this node model instance, i.e. no two instances of this node model
        // have the same value here. Additionally, it's value changes during reset.
        final String uniqueContextId = UUID.randomUUID().toString();
        m_sparkContextId = AbstractDatabricksSparkContextCreatorNodeSettings.createSparkContextID(uniqueContextId);
    }

    /**
     * This method ensures that the default type mapping of the given {@link DataTypeMappingService} is copied into the
     * node model's {@link DataTypeMappingConfiguration} if they are empty which is the case when a new node is created.
     * The node dialog itself prevents the user from removing all type mappings so they can only be empty during
     * initialization.
     *
     * @throws InvalidSettingsException if the type mapping settings are not valid.
     */
    protected void initializeTypeMapping() throws InvalidSettingsException {
        final DBTypeMappingService<? extends DBSource, ? extends DBDestination> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(Databricks.DB_TYPE);

        final SettingsModelDatabaseDataTypeMapping externalToKnimeMappingConfig =
            m_settings.getExternalToKnimeMappingModel();
        final DataTypeMappingConfiguration<SQLType> externalToKnimeConfiguration =
            externalToKnimeMappingConfig.getDataTypeMappingConfiguration(mappingService);
        if (externalToKnimeConfiguration.getTypeRules().isEmpty()
            && externalToKnimeConfiguration.getNameRules().isEmpty()) {
            externalToKnimeMappingConfig
                .setDataTypeMappingConfiguration(mappingService.newDefaultExternalToKnimeMappingConfiguration());
        }
        if (!externalToKnimeConfiguration.isValid()) {
            throw new InvalidSettingsException("The input data type mapping configuration is not valid.");
        }

        final SettingsModelDatabaseDataTypeMapping knimeToExternalMappingConfig =
            m_settings.getKnimeToExternalMappingModel();
        final DataTypeMappingConfiguration<SQLType> knimeToExternalConfiguration =
            knimeToExternalMappingConfig.getDataTypeMappingConfiguration(mappingService);
        if (knimeToExternalConfiguration.getTypeRules().isEmpty()
            && knimeToExternalConfiguration.getNameRules().isEmpty()) {
            knimeToExternalMappingConfig
                .setDataTypeMappingConfiguration(mappingService.newDefaultKnimeToExternalMappingConfiguration());
        }
        if (!knimeToExternalConfiguration.isValid()) {
            throw new InvalidSettingsException("The output data type mapping configuration is not valid.");
        }
    }

    DBSessionPortObject createDBPort(final ExecutionContext exec,
        final CheckedExceptionSupplier<DBConnectionController, InvalidSettingsException> controllerSupplier)
        throws InvalidSettingsException, CanceledExecutionException, SQLException {

        m_sessionInfo = createSessionInfo(controllerSupplier);
        final DBSession session = registerSession(exec);
        final DBType dbType = session.getDBType();
        final DBTypeMappingService<? extends DBSource, ? extends DBDestination> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(dbType);
        final DataTypeMappingConfiguration<SQLType> externalToKnime =
            m_settings.getExternalToKnimeMappingModel().getDataTypeMappingConfiguration(mappingService);
        final DataTypeMappingConfiguration<SQLType> knimeToExternal =
            m_settings.getKnimeToExternalMappingModel().getDataTypeMappingConfiguration(mappingService);

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
    private DBSessionInformation createSessionInfo(
        final CheckedExceptionSupplier<DBConnectionController, InvalidSettingsException> controllerSupplier)
        throws InvalidSettingsException {

        final DBType dbType = DBTypeRegistry.getInstance().getRegisteredDBType(m_settings.getDBType())
            .orElseThrow(() -> new InvalidSettingsException("Unable to find DB Type: " + m_settings.getDBType()));
        final String dialectId = DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(dbType).getId();
        final DBDriverWrapper driver = DBDriverRegistry.getInstance().getDriver(m_settings.getDriver())
            .orElseThrow(() -> new InvalidSettingsException("Unable to find DB driver: " + m_settings.getDriver()));

        final DBConnectionController connectionController = controllerSupplier.get();
        return new DefaultDBSessionInformation(dbType, dialectId, new DBSessionID(), driver.getDriverDefinition(),
            connectionController, m_settings.getAttributeValues());
    }

    @Override
    protected void onDisposeInternal() {
        super.onDisposeInternal();
        BackgroundTasks.run(new DestroyAndDisposeSparkContextTask(m_sparkContextId));
        destroySession();
    }

    @Override
    protected void resetInternal() {
        super.resetInternal();
        BackgroundTasks.run(new DestroyAndDisposeSparkContextTask(m_sparkContextId));
        resetContextID();
        destroySession();
    }

    private void destroySession() {
        if (m_sessionInfo != null) {
            DBSessionCache.getInstance().destroy(m_sessionInfo.getID());
            m_sessionInfo = null;
        }
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        // this is only to avoid errors about an unconfigured spark context. the spark context configured here is
        // has and never will be opened because m_sparkContextId has a new and unique value.
        final String previousContextID = Files
            .readAllLines(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"), Charset.forName("UTF-8")).get(0);
        createDummyContext(previousContextID);

        // Do not restore the DB session here to avoid a cluster start
        setWarningMessage("State restored from disk. Reset this node to start a cluster and Spark execution context.");
    }

    protected abstract void createDummyContext(final String previousContextID);

    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        // see loadAdditionalInternals() for why we are doing this
        Files.write(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"),
            m_sparkContextId.toString().getBytes(Charset.forName("UTF-8")));
    }

}
