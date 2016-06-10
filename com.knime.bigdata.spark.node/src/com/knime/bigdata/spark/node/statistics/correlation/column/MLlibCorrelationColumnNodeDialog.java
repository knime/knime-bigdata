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
package com.knime.bigdata.spark.node.statistics.correlation.column;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.node.MLlibNodeSettings;
import com.knime.bigdata.spark.node.statistics.correlation.MLlibCorrelationMethod;

/**
 *
 * @author koetter
 */
public class MLlibCorrelationColumnNodeDialog extends NodeDialogPane {
    @SuppressWarnings("unchecked")
    private final DialogComponent m_col1 = new DialogComponentColumnNameSelection(
        MLlibCorrelationColumnNodeModel.createCol1Model(), "Column 1: ", 0, DoubleValue.class);
    @SuppressWarnings("unchecked")
    private final DialogComponent m_col2 = new DialogComponentColumnNameSelection(
        MLlibCorrelationColumnNodeModel.createCol2Model(), "Column 2: ", 0, DoubleValue.class);
    private final DialogComponent m_method = new DialogComponentButtonGroup(
        MLlibCorrelationColumnNodeModel.createMethodModel(), null, false,
        MLlibCorrelationMethod.values());
    /**
     *
     */
    public MLlibCorrelationColumnNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        gbc.insets = new Insets(0, 10, 0, 0);
        panel.add(new JLabel("Correlation method:"), gbc);
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.gridx++;
        panel.add(m_method.getComponentPanel(), gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.gridwidth = 2;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        panel.add(m_col1.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_col2.getComponentPanel(), gbc);
        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_method.saveSettingsTo(settings);
        m_col1.saveSettingsTo(settings);
        m_col2.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, specs);
        m_method.loadSettingsFrom(settings, tableSpecs);
        m_col1.loadSettingsFrom(settings, tableSpecs);
        m_col2.loadSettingsFrom(settings, tableSpecs);
    }
}
