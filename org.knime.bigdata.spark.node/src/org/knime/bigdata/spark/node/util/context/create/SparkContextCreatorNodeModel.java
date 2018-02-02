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
package org.knime.bigdata.spark.node.util.context.create;

import java.io.File;
import java.io.IOException;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
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

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkContextCreatorNodeModel extends SparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkContextCreatorNodeModel.class);

    private final ContextSettings m_settings = new ContextSettings();

    private SparkContextID m_lastContextID;

    /**
     * Constructor.
     */
    SparkContextCreatorNodeModel() {
        super(new PortType[]{}, new PortType[]{SparkContextPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkContextID newContextID = m_settings.getSparkContextID();
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(newContextID);
        final JobServerSparkContextConfig config = m_settings.createContextConfig(getCredentialsProvider());

        m_settings.validateSettings();

        if (m_lastContextID != null && !m_lastContextID.equals(newContextID)
            && SparkContextManager.getOrCreateSparkContext(m_lastContextID).getStatus() == SparkContextStatus.OPEN) {
            LOGGER.warn("Context ID has changed. Keeping old context alive and configuring new one!");
        }

        boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            // this means context was OPEN and we are changing settings that cannot become active without
            // destroying and recreating the remote context. Furthermore the node settings say that
            // there should be a warning about this situation
            setWarningMessage("Spark context exists already in the cluster. Settings were not applied.");
        }

        m_lastContextID = newContextID;
        return new PortObjectSpec[]{new SparkContextPortObjectSpec(m_settings.getSparkContextID())};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {

        final SparkContextID contextID = m_settings.getSparkContextID();
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);

        exec.setMessage("Configuring Spark context");
        final JobServerSparkContextConfig config = m_settings.createContextConfig(getCredentialsProvider());

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            // this means context was OPEN and we are changing settings that cannot become active without
            // destroying and recreating the remote context. Furthermore the node settings say that
            // there should be a warning about this situation
            setWarningMessage("Spark context exists already in the cluster. Settings were not applied.");
        }

        // try to open the context
        exec.setMessage("Opening Spark context");
        sparkContext.ensureOpened(true);

        return new PortObject[]{new SparkContextPortObject(contextID)};
    }

    @Override
    protected void onDisposeInternal() {
        if (m_settings.deleteContextOnDispose()) {
            final SparkContextID id = m_settings.getSparkContextID();

            try {
                LOGGER.debug("In onDispose() of SparkContextCreateNodeModel. Removing context: " + id);
                SparkContextManager.ensureDestroyedCustomContext(id);
            } catch (KNIMESparkException e) {
                LOGGER.debug("Failed to destroy context " + id + " on dispose.", e);
            }
        }
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {

        final SparkContextID contextID = m_settings.getSparkContextID();
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);

        // credentials are not available during loadInternals(), only during configure and execute
        // see AP-8636
        if (m_settings.getAuthenticateModel().useCredential()) {
            throw new RuntimeException(
                "Could not configure Spark context because credentials are not available. Please reset this node and reexecute.");
        }

        final JobServerSparkContextConfig sparkContextConfig = m_settings.createContextConfig(getCredentialsProvider());
        boolean configApplied = sparkContext.ensureConfigured(sparkContextConfig, true);

        if (!configApplied) {
            setWarningMessage("Spark context exists already in the cluster. Settings were not applied.");
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
