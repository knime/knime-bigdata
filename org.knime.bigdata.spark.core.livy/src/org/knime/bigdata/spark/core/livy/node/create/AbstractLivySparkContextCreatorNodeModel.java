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

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.util.BackgroundTasks;
import org.knime.bigdata.spark.node.util.context.DestroyAndDisposeSparkContextTask;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortType;

/**
 * Abstract node model of the "Create Spark Context (Livy)" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
abstract class AbstractLivySparkContextCreatorNodeModel extends SparkNodeModel {

    /**
     * Settings model of this node.
     */
    protected final LivySparkContextCreatorNodeSettings m_settings = new LivySparkContextCreatorNodeSettings();

    /**
     * Identifier of spark context created by this node.
     */
    protected SparkContextID m_sparkContextId;

    /**
     * Constructor.
     * @param inPortTypes The input port types.
     * @param outPortTypes The output port types.
     */
    AbstractLivySparkContextCreatorNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
        resetContextID();
    }

    private void resetContextID() {
        // An ID that is unique to this node model instance, i.e. no two instances of this node model
        // have the same value here. Additionally, it's value changes during reset.
        final String uniqueContextId = UUID.randomUUID().toString();
        m_sparkContextId = LivySparkContextCreatorNodeSettings.createSparkContextID(uniqueContextId);
    }

    @Override
    protected void onDisposeInternal() {
        BackgroundTasks.run(new DestroyAndDisposeSparkContextTask(m_sparkContextId));
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        // this is only to avoid errors about an unconfigured spark context. the spark context configured here is
        // has and never will be opened because m_uniqueContextId has a new and unique value.
        final String previousContextID = Files
            .readAllLines(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"), Charset.forName("UTF-8")).get(0);
        createDummyContext(previousContextID);
    }

    protected abstract void createDummyContext(final String previousContextID);

    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
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
