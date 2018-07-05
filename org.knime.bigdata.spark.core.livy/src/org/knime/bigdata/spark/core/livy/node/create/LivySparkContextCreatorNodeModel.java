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
package org.knime.bigdata.spark.core.livy.node.create;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.livy.context.LivySparkContext;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
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

/**
 * Node model of the "Create Spark Context (Livy)" node.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContextCreatorNodeModel extends SparkNodeModel {

    private final LivySparkContextCreatorNodeSettings m_settings = new LivySparkContextCreatorNodeSettings();

    /**
     * Created using the {@link #m_uniqueContextId} during the configure phase.
     */
    private SparkContextID m_sparkContextId;

    /**
     * Constructor.
     */
    LivySparkContextCreatorNodeModel() {
        super(new PortType[]{ConnectionInformationPortObject.TYPE}, new PortType[]{SparkContextPortObject.TYPE});
        resetContextID();
    }

    private void resetContextID() {
        // An ID that is unique to this node model instance, i.e. no two instances of this node model
        // have the same value here. Additionally, it's value changes during reset.
        final String uniqueContextId = UUID.randomUUID().toString();
        m_sparkContextId = LivySparkContextCreatorNodeSettings.createSparkContextID(uniqueContextId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Remote file handling connection missing");
        }

        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) inSpecs[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();
        if (connInfo == null) {
            throw new InvalidSettingsException("No remote file handling connection information available");
        }

        m_settings.validateDeeper();
        configureSparkContext(m_sparkContextId, connInfo, m_settings);
        return new PortObjectSpec[]{new SparkContextPortObjectSpec(m_sparkContextId)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final ConnectionInformationPortObject object = (ConnectionInformationPortObject) inData[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();

        exec.setProgress(0, "Configuring Livy Spark context");
        configureSparkContext(m_sparkContextId, connInfo, m_settings);

        final LivySparkContext sparkContext =
            (LivySparkContext)SparkContextManager.<LivySparkContextConfig> getOrCreateSparkContext(m_sparkContextId);

        // try to open the context
        exec.setProgress(0.1, "Creating context");
        sparkContext.ensureOpened(true, exec.createSubProgress(0.9));

        return new PortObject[]{new SparkContextPortObject(m_sparkContextId)};
    }

    @Override
    protected void onDisposeInternal() {
        BackgroundTasks.run(new DestroyAndDisposeSparkContextTask(m_sparkContextId));
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        ConnectionInformation dummyConnInfo = new ConnectionInformation();

        // this is only to avoid errors about an unconfigured spark context. the spark context configured here is
        // has and never will be opened because m_uniqueContextId has a new and unique value.
        final String previousContextID = Files
            .readAllLines(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"), Charset.forName("UTF-8")).get(0);
        configureSparkContext(new SparkContextID(previousContextID), dummyConnInfo, m_settings);
    }

    @Override
    protected void saveAdditionalInternals(File nodeInternDir, ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        // see loadAdditionalInternals() for why we are doing this
        Files.write(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"),
            m_sparkContextId.toString().getBytes(Charset.forName("UTF-8")));
    }

    @Override
    protected void resetInternal() {
        BackgroundTasks.run(new DestroyAndDisposeSparkContextTask(m_sparkContextId));
        resetContextID();
    }

    /**
     * Internal method to ensure that the given Spark context is configured.
     * 
     * @param sparkContextId Identifies the Spark context to configure.
     * @param connInfo 
     * @param settings The settings from which to configure the context.
     */
    protected static void configureSparkContext(final SparkContextID sparkContextId,
        ConnectionInformation connInfo, final LivySparkContextCreatorNodeSettings settings) {
        
        final SparkContext<LivySparkContextConfig> sparkContext =
            SparkContextManager.getOrCreateSparkContext(sparkContextId);
        final LivySparkContextConfig config = settings.createContextConfig(sparkContextId, connInfo);

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied) {
            // this should never ever happen
            throw new RuntimeException("Failed to apply Spark context settings.");
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

    /**
     * 
     * @return the current Spark context ID.
     */
    public SparkContextID getSparkContextID() {
        return m_sparkContextId;
    }
}
