package org.knime.bigdata.spark.local.node.create;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;
import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.OnDisposeAction;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;

/**
 * Abstract class for the LocalEnvironment node creator model
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public abstract class AbstractLocalEnvironmentCreatorNodeModel extends SparkNodeModel {

    /**
     * Logger for the node execution
     */
    protected static final NodeLogger LOGGER = NodeLogger.getLogger(LocalEnvironmentCreatorNodeModel.class);

    /**
     * Settings for the local Spark context
     */
    protected final LocalSparkContextSettings m_settings = new LocalSparkContextSettings();

    /**
     * Id of the last opened context
     */
    protected SparkContextID m_lastContextID;


    /**
     * Creates a SparkNodeModel for the local environment node
     *
     * @param inPortTypes array of input ports
     * @param outPortTypes array of outputPorts
     */
    protected AbstractLocalEnvironmentCreatorNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkContextID newContextID = m_settings.getSparkContextID();
        final SparkContext<LocalSparkContextConfig> sparkContext =
                SparkContextManager.getOrCreateSparkContext(newContextID);
        final LocalSparkContextConfig config = m_settings.createContextConfig();

        m_settings.validateDeeper();

        if ((m_lastContextID != null) && !m_lastContextID.equals(newContextID)
                && (SparkContextManager.getOrCreateSparkContext(m_lastContextID).getStatus() == SparkContextStatus.OPEN)) {
            LOGGER.warn("Context ID has changed. Keeping old local Spark context alive and configuring new one!");
        }

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            // this means context was OPEN and we are changing settings that cannot become active without
            // destroying and recreating the remote context. Furthermore the node settings say that
            // there should be a warning about this situation
            setWarningMessage("Local Spark context exists already. Settings were not applied.");
        }

        m_lastContextID = newContextID;

        PortObjectSpec dbPortObjectObjectSpec = null;
        if (!sparkContext.getConfiguration().startThriftserver()) {
            dbPortObjectObjectSpec = InactiveBranchPortObjectSpec.INSTANCE;
        }

        return new PortObjectSpec[]{dbPortObjectObjectSpec, createHDFSConnectionSpec(),
            new SparkContextPortObjectSpec(m_settings.getSparkContextID())};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {

        final SparkContextID contextID = m_settings.getSparkContextID();
        final LocalSparkContext sparkContext =
                (LocalSparkContext)SparkContextManager.<LocalSparkContextConfig> getOrCreateSparkContext(contextID);

        exec.setProgress(0, "Configuring local Spark context");
        final LocalSparkContextConfig config = m_settings.createContextConfig();

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            // this means context was OPEN and we are changing settings that cannot become active without
            // destroying and recreating the remote context. Furthermore the node settings say that
            // there should be a warning about this situation
            setWarningMessage("Local Spark context exists already. Settings were not applied.");
        }

        // try to open the context
        exec.setProgress(0.1, "Creating context");
        sparkContext.ensureOpened(true, exec.createSubProgress(0.9));

        final PortObject dbPortObject;
        if (sparkContext.getConfiguration().startThriftserver()) {
            final int hiveserverPort = sparkContext.getHiveserverPort();
            dbPortObject = createDBPort(exec, hiveserverPort);
        } else {
            dbPortObject = InactiveBranchPortObject.INSTANCE;
        }

        return new PortObject[]{dbPortObject, new ConnectionInformationPortObject(createHDFSConnectionSpec()),
            new SparkContextPortObject(contextID)};
    }

    /**
     * Creates the Port for the database connection
     *
     * @param exec the ExecutionContext to use
     * @param hiveserverPort the HIVE server port
     * @return the PortObject for the database port
     * @throws CanceledExecutionException
     * @throws SQLException
     * @throws InvalidSettingsException
     */
    protected abstract PortObject createDBPort(ExecutionContext exec, int hiveserverPort) throws CanceledExecutionException, SQLException, InvalidSettingsException;


    /**
     * Create the spec for the local hdfs.
     *
     * @return ...
     * @throws InvalidSettingsException ...
     */
    private static ConnectionInformationPortObjectSpec createHDFSConnectionSpec() throws InvalidSettingsException {
        return new ConnectionInformationPortObjectSpec(HDFSLocalConnectionInformation.getInstance());
    }

    @Override
    protected void onDisposeInternal() {
        if (m_settings.getOnDisposeAction() == OnDisposeAction.DESTROY_CTX) {
            final SparkContextID id = m_settings.getSparkContextID();

            try {
                SparkContextManager.ensureSparkContextDestroyed(id);
            } catch (final KNIMESparkException e) {
                LOGGER.debug("Failed to destroy context " + id + " on dispose.", e);
            }
        }
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {

        final SparkContextID contextID = m_settings.getSparkContextID();
        final SparkContext<LocalSparkContextConfig> sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);

        final LocalSparkContextConfig sparkContextConfig = m_settings.createContextConfig();

        final boolean configApplied = sparkContext.ensureConfigured(sparkContextConfig, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            setWarningMessage("Local Spark context exists already. Settings were not applied.");
        }
    }



    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }


}
