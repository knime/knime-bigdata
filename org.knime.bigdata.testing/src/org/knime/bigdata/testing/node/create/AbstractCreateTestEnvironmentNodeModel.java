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
 *   Created on 03.07.2015 by koetter
 */
package org.knime.bigdata.testing.node.create;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.context.testing.TestingSparkContextConfigFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.testing.FlowVariableReader;
import org.knime.bigdata.testing.node.create.utils.CreateTestPortUtil;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.Node;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Abstract node model for the "Create Big Data Test Environment" node implementations.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH
 */
public abstract class AbstractCreateTestEnvironmentNodeModel extends SparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractCreateTestEnvironmentNodeModel.class);

    private SparkContextConfig m_sparkConfig;

    /**
     * Default constructor.
     *
     * @param dbPortType database port type
     * @param fsPortType file system port type
     */
    protected AbstractCreateTestEnvironmentNodeModel(final PortType dbPortType, final PortType fsPortType) {
        super(new PortType[]{}, new PortType[]{dbPortType, fsPortType, SparkContextPortObject.TYPE});
    }

    /**
     * @return database port utility
     */
    protected abstract CreateTestPortUtil getDatabasePortUtil();

    /**
     * @return file system port utility
     */
    protected abstract CreateTestPortUtil getFileSystemPortUtil();

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final Map<String, FlowVariable> flowVars;
        try {
            flowVars = FlowVariableReader.readFromCsv();
            flowVars.values().stream().forEach(v -> Node.invokePushFlowVariable(this, v));
        } catch (Exception e) {
            throw new InvalidSettingsException("Failed to read flowvariables.csv: " + e.getMessage(), e);
        }

        final SparkContextIDScheme sparkScheme = TestingSparkContextConfigFactory.createContextIDScheme(flowVars);
        final PortObjectSpec dbPortSpec = getDatabasePortUtil().configure(sparkScheme, flowVars);
        final PortObjectSpec fsPortSpec = getFileSystemPortUtil().configure(sparkScheme, flowVars);
        final SparkContextConfig sparkConfig = TestingSparkContextConfigFactory.create(sparkScheme, flowVars, fsPortSpec);
        configureSparkContext(sparkConfig);
        final SparkContextPortObjectSpec sparkPortSpec = new SparkContextPortObjectSpec(sparkConfig.getSparkContextID());

        // everything seems valid, now we can update the node model state and return port object specs
        m_sparkConfig = sparkConfig;

        return new PortObjectSpec[]{dbPortSpec, fsPortSpec, sparkPortSpec};
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void configureSparkContext(final SparkContextConfig config) throws InvalidSettingsException {
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(config.getSparkContextID());
        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied) {
            throw new InvalidSettingsException("Could not apply settings for testing Spark context");
        }
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final Map<String, FlowVariable> flowVars = getAvailableFlowVariables();
        final SparkContextIDScheme sparkScheme = m_sparkConfig.getSparkContextID().getScheme();
        final CredentialsProvider credentialsProvider = getCredentialsProvider();

        // test remote fs first because it is quick and does not require any resources
        exec.setProgress(0, "Opening remote file system connection");
        final PortObject fsPortObject =
            getFileSystemPortUtil().execute(sparkScheme, flowVars, exec, credentialsProvider);

        // then try to open the Spark context
        exec.setProgress(0.1, "Configuring Spark context");
        final SparkContextPortObject sparkPortObject = new SparkContextPortObject(m_sparkConfig.getSparkContextID());
        configureSparkContext(m_sparkConfig);
        exec.setProgress(0.2, "Creating Spark context");
        SparkContextManager.getOrCreateSparkContext(m_sparkConfig.getSparkContextID()).ensureOpened(true, exec.createSubProgress(0.7));

        // finally, open the DB connection
        final PortObject dbPortObject =
            getDatabasePortUtil().execute(sparkScheme, flowVars, exec, credentialsProvider);

        return new PortObject[]{dbPortObject, fsPortObject, sparkPortObject};
    }

    /**
     * Set a node warning, useful in {@link CreateTestPortUtil} implementations.
     * @param warningMessage message to show
     */
    public void setNodeWarning(final String warningMessage) {
        setWarningMessage(warningMessage);
    }

    /**
     * Get the node logger, useful in {@link CreateTestPortUtil} implementations.
     * @return the node logger of this node
     */
    public NodeLogger getNodeLogger() {
        return LOGGER;
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        throw new IOException("Big Data Test Environment can not restored from disk! Please reset the node.");
    }

    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        throw new IOException("Big Data Test Environment can not be saved to disk! Please reset the node.");
    }

    @Override
    protected void onDisposeInternal() {
        getDatabasePortUtil().onDispose();
        getFileSystemPortUtil().onDispose();
    }

    @Override
    protected void resetInternal() {
        getDatabasePortUtil().reset();
        getFileSystemPortUtil().reset();
    }
}
