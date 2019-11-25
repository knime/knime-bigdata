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

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.database.databricks.DatabricksDBConnectionController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContext;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.spark.core.util.BackgroundTasks;
import org.knime.bigdata.spark.node.util.context.create.DestroyAndDisposeSparkContextTask;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
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
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

/**
 * Node model of the "Create Spark Context (Databricks)" node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContextCreatorNodeModel extends SparkNodeModel {

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

    private final DatabricksSparkContextCreatorNodeSettings m_settings = new DatabricksSparkContextCreatorNodeSettings();

    /**
     * Created using the {@link DatabricksSparkContextCreatorNodeSettings#m_uniqueContextId} during the configure phase.
     */
    private SparkContextID m_sparkContextId;

    /**
     * Contains DB port session if enabled in settings or <code>null</code>.
     */
    private DBSessionInformation m_sessionInfo;

    /**
     * Constructor.
     */
    DatabricksSparkContextCreatorNodeModel() {
        super(new PortType[0], new PortType[]{DBSessionPortObject.TYPE, ConnectionInformationPortObject.TYPE,
            SparkContextPortObject.TYPE});
        resetContextID();
    }

    private void resetContextID() {
        // An ID that is unique to this node model instance, i.e. no two instances of this node model
        // have the same value here. Additionally, it's value changes during reset.
        final String uniqueContextId = UUID.randomUUID().toString();
        m_sparkContextId = DatabricksSparkContextCreatorNodeSettings.createSparkContextID(uniqueContextId);
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_settings.validateDeeper();

        initializeTypeMapping();
        m_settings.validateDriverRegistered();
        m_settings.validate(m_variableContext);

        final ConnectionInformation connInfo = m_settings.createDBFSConnectionInformation(getCredentialsProvider());
        configureSparkContext(m_sparkContextId, connInfo, m_settings, getCredentialsProvider());

        final PortObjectSpec sparkPortSpec;
        if (m_settings.isCreateSparkContextSet()) {
            sparkPortSpec = new SparkContextPortObjectSpec(m_sparkContextId);
        } else {
            sparkPortSpec = InactiveBranchPortObjectSpec.INSTANCE;
        }

        return new PortObjectSpec[]{null, new ConnectionInformationPortObjectSpec(connInfo), sparkPortSpec};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final ConnectionInformation connInfo = m_settings.createDBFSConnectionInformation(getCredentialsProvider());

        // configure context
        exec.setProgress(0.1, "Configuring Databricks Spark context");
        configureSparkContext(m_sparkContextId, connInfo, m_settings, getCredentialsProvider());

        // start cluster
        exec.setProgress(0.2, "Starting cluster on Databricks");
        final DatabricksSparkContext sparkContext = (DatabricksSparkContext)SparkContextManager
            .<DatabricksSparkContextConfig> getOrCreateSparkContext(m_sparkContextId);
        sparkContext.startCluster(exec);

        // create spark context
        final PortObject sparkPortObject;
        if (m_settings.isCreateSparkContextSet()) {
            exec.setProgress(0.5, "Creating context");
            sparkContext.ensureOpened(true, exec.createSubProgress(0.9));
            sparkPortObject = new SparkContextPortObject(m_sparkContextId);
        } else {
            sparkPortObject = InactiveBranchPortObject.INSTANCE;
        }

        // open the JDBC connection AFTER starting the cluster, otherwise Databricks returns 503... (cluster starting)
        exec.setProgress(0.9, "Configuring Databricks DB connection");
        final PortObject dbPortObject = createDBPort(exec, sparkContext.getClusterStatusHandler());

        return new PortObject[]{
            dbPortObject,
            new ConnectionInformationPortObject(new ConnectionInformationPortObjectSpec(connInfo)),
            sparkPortObject};
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

    private DBSessionPortObject createDBPort(final ExecutionContext exec,
        final DatabricksClusterStatusProvider clusterStatus)
        throws InvalidSettingsException, CanceledExecutionException, SQLException {

        m_sessionInfo = createSessionInfo(clusterStatus);
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
    private DBSessionInformation createSessionInfo(final DatabricksClusterStatusProvider clusterStatus)
        throws InvalidSettingsException {

        final DBType dbType = DBTypeRegistry.getInstance().getRegisteredDBType(m_settings.getDBType())
            .orElseThrow(() -> new InvalidSettingsException("Unable to find DB Type: " + m_settings.getDBType()));
        final String dialectId = DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(dbType).getId();
        final DBDriverWrapper driver = DBDriverRegistry.getInstance().getDriver(m_settings.getDriver())
            .orElseThrow(() -> new InvalidSettingsException("Unable to find DB driver: " + m_settings.getDriver()));

        final DBConnectionController connectionController = createConnectionController(clusterStatus);
        return new DefaultDBSessionInformation(dbType, dialectId, new DBSessionID(), driver.getDriverDefinition(),
            connectionController, m_settings.getAttributeValues());
    }

    private DBConnectionController createConnectionController(final DatabricksClusterStatusProvider clusterStatus)
        throws InvalidSettingsException {

        final String username = m_settings.getUsername(getCredentialsProvider());
        final String password = m_settings.getPassword(getCredentialsProvider());
        return new DatabricksDBConnectionController(m_settings.getDBUrl(), clusterStatus, m_settings.getClusterId(),
            m_settings.getWorkspaceId(), username, password);
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

    /**
     * Internal method to ensure that the given Spark context is configured.
     *
     * @param sparkContextId Identifies the Spark context to configure.
     * @param connInfo
     * @param settings The settings from which to configure the context.
     * @param credentialsProvider credentials provider to use
     */
    protected static void configureSparkContext(final SparkContextID sparkContextId,
        final ConnectionInformation connInfo, final DatabricksSparkContextCreatorNodeSettings settings,
        final CredentialsProvider credentialsProvider) {

        try {
            final SparkContext<DatabricksSparkContextConfig> sparkContext =
                SparkContextManager.getOrCreateSparkContext(sparkContextId);
            final DatabricksSparkContextConfig config =
                settings.createContextConfig(sparkContextId, connInfo, credentialsProvider);

            final boolean configApplied = sparkContext.ensureConfigured(config, true);
            if (!configApplied) {
                // this should never ever happen
                throw new RuntimeException("Failed to apply Spark context settings.");
            }
        } catch (final InvalidSettingsException e) {
            // this should never ever happen
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        super.loadAdditionalInternals(nodeInternDir, exec);

        try {
            ConnectionInformation dummyConnInfo = new ConnectionInformation();

            // this is only to avoid errors about an unconfigured spark context. the spark context configured here is
            // has and never will be opened because m_sparkContextId has a new and unique value.
            final String previousContextID = Files
                .readAllLines(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"), Charset.forName("UTF-8")).get(0);
            configureSparkContext(new SparkContextID(previousContextID), dummyConnInfo, m_settings,
                getCredentialsProvider());

            // Do not restore the DB session here to avoid a cluster start
            setWarningMessage("State restored from disk. Reset this node to start a cluster and Spark execution context.");

        } catch (final Exception exception) {
            throw new IOException(exception.getMessage(), exception);
        }
    }

    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        // see loadAdditionalInternals() for why we are doing this
        Files.write(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"),
            m_sparkContextId.toString().getBytes(Charset.forName("UTF-8")));
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }
}
