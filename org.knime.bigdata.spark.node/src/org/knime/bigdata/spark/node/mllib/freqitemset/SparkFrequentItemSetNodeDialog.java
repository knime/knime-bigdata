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
 *   Created on Jan 30, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.freqitemset;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Frequent item sets settings dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkFrequentItemSetNodeDialog extends NodeDialogPane {
    private final SparkFrequentItemSetSettings m_settings;
    private final SparkFrquentItemSetNodeDialogPanel m_panel;

    /** Default constructor. */
    public SparkFrequentItemSetNodeDialog() {
        m_settings = new SparkFrequentItemSetSettings();
        m_panel = new SparkFrquentItemSetNodeDialogPanel(m_settings);
        addTab("Settings", m_panel.getComponentPanel());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_panel.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        m_panel.loadSettingsFrom(settings, specs);
    }
}
