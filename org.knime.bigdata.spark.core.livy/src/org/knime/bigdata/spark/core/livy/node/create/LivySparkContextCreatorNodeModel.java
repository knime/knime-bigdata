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

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.livy.context.LivySparkContext;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.bigdata.spark.core.livy.node.create.LivySparkContextSettings.OnDisposeAction;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
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
class LivySparkContextCreatorNodeModel extends SparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(LivySparkContextCreatorNodeModel.class);

    private final LivySparkContextSettings m_settings = new LivySparkContextSettings();

    private SparkContextID m_lastContextID;

    /**
     * Constructor.
     */
    LivySparkContextCreatorNodeModel() {
        super(new PortType[]{}, new PortType[]{SparkContextPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkContextID newContextID = m_settings.getSparkContextID();
        final SparkContext<LivySparkContextConfig> sparkContext = SparkContextManager.getOrCreateSparkContext(newContextID);
        final LivySparkContextConfig config = m_settings.createContextConfig();

        m_settings.validateDeeper();

        if (m_lastContextID != null && !m_lastContextID.equals(newContextID)
            && SparkContextManager.getOrCreateSparkContext(m_lastContextID).getStatus() == SparkContextStatus.OPEN) {
            LOGGER.warn("Context ID has changed. Keeping old Livy Spark context alive and configuring new one!");
        }

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            // this means context was OPEN and we are changing settings that cannot become active without
            // destroying and recreating the remote context. Furthermore the node settings say that
            // there should be a warning about this situation
            setWarningMessage("Livy Spark context exists already. Settings were not applied.");
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
		final LivySparkContext sparkContext = (LivySparkContext) SparkContextManager
				.<LivySparkContextConfig>getOrCreateSparkContext(contextID);

		exec.setProgress(0, "Configuring Livy Spark context");
        final LivySparkContextConfig config = m_settings.createContextConfig();

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

        return new PortObject[]{new SparkContextPortObject(contextID)};
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
        final SparkContext<LivySparkContextConfig> sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);

        final LivySparkContextConfig sparkContextConfig = m_settings.createContextConfig();

        final boolean configApplied = sparkContext.ensureConfigured(sparkContextConfig, true);
        if (!configApplied && !m_settings.hideExistsWarning()) {
            setWarningMessage("Livy Spark context exists already. Settings were not applied.");
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
