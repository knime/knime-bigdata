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
 *   Created on 28.08.2015 by koetter
 */
package org.knime.bigdata.spark.node.util.context.destroy;

import javax.swing.JOptionPane;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkDestroyContextNodeModel extends SparkNodeModel {

    private final SettingsModelBoolean m_noDialog = createNoDialogModel();

    SparkDestroyContextNodeModel() {
        super(new PortType[]{SparkContextPortObject.TYPE}, new PortType[0]);
    }

    /**
     * @return the no dialog option
     */
    static SettingsModelBoolean createNoDialogModel() {
        return new SettingsModelBoolean("hideConfirmationDialog", false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkContextProvider provider = (SparkContextProvider)inData[0];
        exec.setMessage("Destroying ingoing Spark context");

        if (!m_noDialog.getBooleanValue() && !Boolean.getBoolean("java.awt.headless")) {
            exec.setMessage("Confirmation dialog opened. Waiting for user response...");
            final int n = JOptionPane.showConfirmDialog(null, "Do you really want to destroy the Spark context?",
                "Destroy Spark Context", JOptionPane.OK_CANCEL_OPTION);
            if (n == JOptionPane.CANCEL_OPTION) {
                throw new CanceledExecutionException("Execution aborted. Context has not been destroyed.");
            }
        }
        exec.checkCanceled();

        final SparkContext context = SparkContextManager.getOrCreateSparkContext(provider.getContextID());
        context.ensureDestroyed();

        return new PortObject[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_noDialog.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noDialog.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noDialog.loadSettingsFrom(settings);
    }
}
