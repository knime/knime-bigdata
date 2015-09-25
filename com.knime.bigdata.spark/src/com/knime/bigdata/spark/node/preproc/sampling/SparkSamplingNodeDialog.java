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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.sampling;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author koetter
 */
public class SparkSamplingNodeDialog extends NodeDialogPane {
    private final SparkSamplingNodeDialogPanel m_panel;
    /**
     * Creates the dialog.
     */
    public SparkSamplingNodeDialog() {
        super();
        m_panel = new SparkSamplingNodeDialogPanel();
        super.addTab("Sampling Method", m_panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] inSpecs)
            throws NotConfigurableException {
        if (inSpecs == null || inSpecs.length != 1 || !(inSpecs[0] instanceof SparkDataPortObjectSpec)) {
            throw new NotConfigurableException("Not connected");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        m_panel.loadSettingsFrom(settings, sparkSpec.getTableSpec());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_panel.saveSettingsTo(settings);
    }
}
