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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JOptionPane;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.ViewUtils;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;

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
        final List<String> warnings = new ArrayList<>();
        final SparkContextID newContextID = m_settings.getSparkContextID();

        m_settings.validateSettings();

        try {
            if (m_lastContextID != null && !m_lastContextID.equals(newContextID)
                    && SparkContextManager.getOrCreateSparkContext(m_lastContextID).getStatus() == SparkContextStatus.OPEN) {

                LOGGER.warn("Context ID has changed. Keeping old context alive and configuring new one!");
                configureContextIfPossible(m_settings);

            } else if (m_lastContextID != null && m_lastContextID.equals(newContextID)) {
                LOGGER.debug("Reconfiguring old context with same ID.");

                if (!SparkContextManager.reconfigureContext(m_lastContextID, m_settings.createContextConfig(getCredentialsProvider()), false)) {
                    final AtomicInteger choice = new AtomicInteger();

                    ViewUtils.invokeAndWaitInEDT(new Runnable() {
                        @Override
                        public void run() {
                            choice.set(JOptionPane.showConfirmDialog(null,
                                "New settings only become active after destroying the existing remote Spark context. "
                                        + "Should the existing context be destroyed?\n\n"
                                        + "WARNING: This deletes all cached data such as RDDs in the context and you have to reset all executed Spark nodes.",
                                "Spark context settings have changed", JOptionPane.OK_CANCEL_OPTION));
                        }
                    });

                    if (choice.get() == JOptionPane.OK_OPTION) {
                        LOGGER.info("Destroying remote context and configuring new one.");
                        SparkContextManager.reconfigureContext(m_lastContextID, m_settings.createContextConfig(getCredentialsProvider()), true);
                    } else {
                        LOGGER.warn("Context reset aborded, no changes applied.");
                    }
                }

            } else {
                LOGGER.debug("Configuring new context.");
                configureContextIfPossible(m_settings);
            }

        } catch (KNIMESparkException e) {
            LOGGER.error("Error while (re)configuring Spark context: " + e.getMessage(), e);
            throw new InvalidSettingsException("Error while (re)configuring Spark context.", e);
        }

        setWarningMessages(warnings);

        m_lastContextID = newContextID;
        return new PortObjectSpec[]{new SparkContextPortObjectSpec(m_settings.getSparkContextID())};
    }

    private void configureContextIfPossible(final ContextSettings settings) {
        final SparkContextID contextID = settings.getSparkContextID();
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);

        if (sparkContext.getStatus() == SparkContextStatus.NEW
            || sparkContext.getStatus() == SparkContextStatus.CONFIGURED) {
            sparkContext.configure(settings.createContextConfig(getCredentialsProvider()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {


        final SparkContextID contextID = m_settings.getSparkContextID();
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);
        final List<String> warnings = new ArrayList<>();

        //try to open the context
        exec.setMessage("Opening Spark context");
        configureContextIfPossible(m_settings);

        switch (sparkContext.getStatus()) {
            case NEW:
                throw new KNIMESparkException(String.format(
                    "Context is in unexpected state %s. Possible reason: Parallel changes are being made to it by another KNIME workflow.",
                    sparkContext.getStatus().toString()));
            case CONFIGURED:
                sparkContext.ensureOpened(true);
                break;
            case OPEN:
                if (!m_settings.hideExistsWarning()) {
                    warnings.add("Spark context exists already. Doing nothing and ignoring settings.");
                }
                break;
        }

        exec.setMessage("Spark context opened");

        setWarningMessages(warnings);

        return new PortObject[]{new SparkContextPortObject(contextID)};
    }

    private void setWarningMessages(final List<String> warnings) {
        if (warnings.size() == 1) {
            setWarningMessage(warnings.get(0));
        } else if (warnings.size() > 1){
            StringBuilder buf = new StringBuilder("Warnings:\n");
            for (String warning : warnings) {
                buf.append("- ");
                buf.append(warning);
                buf.append('\n');
            }
            buf.deleteCharAt(buf.length() - 1);
            setWarningMessage(buf.toString());
        }
    }

    @Override
    protected void onDisposeInternal() {
        if (m_settings.deleteContextOnDispose()) {
            final SparkContextID id = m_settings.getSparkContextID();

            try {
                LOGGER.info("In onDispose() of SparkContextCreateNodeModel. Removing context: " + id);
                SparkContextManager.ensureDestroyedCustomContext(id);
            } catch (KNIMESparkException e) {
                LOGGER.error("Failed to destroy context " + id + " on dispose.", e);
            }
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
