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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.node.MLlibNodeSettings;

/**
 *
 * @author koetter
 */
public class MLlibGradientBoostedTreeNodeDialog extends NodeDialogPane {

    private final GradientBoostedTreesSettings m_settings = new GradientBoostedTreesSettings();
    private final GradientBoostedTreesComponents m_components = new GradientBoostedTreesComponents(m_settings);

    /**
     *
     */
    public MLlibGradientBoostedTreeNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.EAST;
        gbc.gridy = 0;
        gbc.gridx = 0;
        panel.add(m_components.getNoOfIterationsComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getMaxNoOfBinsComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getMaxDepthComponent().getComponentPanel(), gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        panel.add(m_components.getLossFunctionComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getIsClassificationComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getLearningRateComponent().getComponentPanel(), gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.weightx = 1;
        gbc.weighty = 0;
        gbc.gridwidth = 3;
        // class column selection
        panel.add(m_components.getClassColComponent().getComponentPanel(), gbc);

        gbc.gridwidth=3;
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        final JPanel colsPanel = m_components.getFeatureColsComponent().getComponentPanel();
        colsPanel.setBorder(BorderFactory.createTitledBorder(" Feature Columns "));
        panel.add(colsPanel, gbc);

        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] ports) throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, ports);
        m_components.loadSettingsFrom(settings, tableSpecs[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_components.saveSettingsTo(settings);
    }
}
