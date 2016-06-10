/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark.node.util.context.destroy;

import javax.swing.JOptionPane;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.SparkContextProvider;
import com.knime.bigdata.spark.core.port.context.SparkContextPortObject;

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

        if (!m_noDialog.getBooleanValue()) {
            exec.setMessage("Confirmation dialog opened. Waiting for user response...");
            final int n = JOptionPane.showConfirmDialog(null, "Do you really want to destroy the Spark context?",
                "Destroy Spark Context", JOptionPane.OK_CANCEL_OPTION);
            if (n == JOptionPane.CANCEL_OPTION) {
                throw new CanceledExecutionException("Execution aborted. Context has not been destroyed.");
            }
        }
        exec.checkCanceled();

        switch(SparkContextManager.getOrCreateSparkContext(provider.getContextID()).getStatus()) {
            case NEW:
            case CONFIGURED:
                setWarningMessage("Spark context is not open. Doing nothing.");
                break;
            case OPEN:
                SparkContextManager.destroyCustomContext(provider.getContextID());
        }

        return new PortObject[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_noDialog.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noDialog.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noDialog.loadSettingsFrom(settings);
    }
}