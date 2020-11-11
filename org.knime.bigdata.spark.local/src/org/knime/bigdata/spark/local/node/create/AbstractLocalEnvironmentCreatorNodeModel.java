package org.knime.bigdata.spark.local.node.create;

import java.io.File;
import java.io.IOException;

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
import org.knime.bigdata.spark.local.node.create.utils.CreateLocalBDEPortUtil;
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
    protected final LocalSparkContextSettings m_settings;

    /**
     * Id of the last opened context
     */
    protected SparkContextID m_lastContextID;

    /**
     * Creates a SparkNodeModel for the local environment node
     *
     * @param dbPortType database port type
     * @param fsPortType file system port type
     * @param hasWorkingDirectorySetting {@code true} if the file system port requires a working directory
     */
    protected AbstractLocalEnvironmentCreatorNodeModel(final PortType dbPortType, final PortType fsPortType,
        final boolean hasWorkingDirectorySetting) {

        super(new PortType[]{}, new PortType[]{dbPortType, fsPortType, SparkContextPortObject.TYPE});
        m_settings = new LocalSparkContextSettings(hasWorkingDirectorySetting);
    }

    /**
     * @return database output port utility
     */
    protected abstract CreateLocalBDEPortUtil getDatabasePortUtil();

    /**
     * @return file system output port utility
     */
    protected abstract CreateLocalBDEPortUtil getFileSystemPortUtil();

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

        final PortObjectSpec dbPortObjectObjectSpec;
        if (sparkContext.getConfiguration().startThriftserver()) {
            dbPortObjectObjectSpec = getDatabasePortUtil().configure();
        } else {
            dbPortObjectObjectSpec = InactiveBranchPortObjectSpec.INSTANCE;

        }

        return new PortObjectSpec[]{ //
            dbPortObjectObjectSpec, //
            getFileSystemPortUtil().configure(), //
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
            dbPortObject = getDatabasePortUtil().execute(sparkContext, exec);
        } else {
            dbPortObject = InactiveBranchPortObject.INSTANCE;
        }

        return new PortObject[]{ //
            dbPortObject, //
            getFileSystemPortUtil().execute(sparkContext, exec), //
            new SparkContextPortObject(contextID)};
    }

    /**
     * Set a node warning, useful in {@link CreateLocalBDEPortUtil} implementations.
     * @param warningMessage message to show
     */
    public void setNodeWarning(final String warningMessage) {
        setWarningMessage(warningMessage);
    }

    /**
     * Get the node logger, useful in {@link CreateLocalBDEPortUtil} implementations.
     * @return the node logger of this node
     */
    public NodeLogger getNodeLogger() {
        return LOGGER;
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

        getDatabasePortUtil().onDispose();
        getFileSystemPortUtil().onDispose();
    }

    @Override
    protected void resetInternal() {
        getDatabasePortUtil().reset();
        getFileSystemPortUtil().reset();
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {

        final SparkContextID contextID = m_settings.getSparkContextID();
        final LocalSparkContext sparkContext =
            (LocalSparkContext)SparkContextManager.<LocalSparkContextConfig> getOrCreateSparkContext(contextID);
        final LocalSparkContextConfig sparkContextConfig = m_settings.createContextConfig();

        final boolean configApplied = sparkContext.ensureConfigured(sparkContextConfig, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            setWarningMessage("Local Spark context exists already. Settings were not applied.");
        }

        getDatabasePortUtil().loadInternals(nodeInternDir, exec, sparkContext, sparkContextConfig);
        getFileSystemPortUtil().loadInternals(nodeInternDir, exec, sparkContext, sparkContextConfig);
    }

    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        getDatabasePortUtil().saveInternals(nodeInternDir, exec);
        getFileSystemPortUtil().saveInternals(nodeInternDir, exec);
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
