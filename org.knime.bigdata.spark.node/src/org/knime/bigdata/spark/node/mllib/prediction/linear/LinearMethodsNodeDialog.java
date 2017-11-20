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
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JComponent;
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
 * @author Tobias Koetter, KNIME.com
 */
public class LinearMethodsNodeDialog extends NodeDialogPane {

    private final LinearMethodsSettings m_settings = new LinearMethodsSettings();
    private final LinearMethodsComponents m_components = new LinearMethodsComponents(m_settings);

    /**
     * @param supportsLBFGS
     *
     */
    public LinearMethodsNodeDialog(final boolean supportsLBFGS) {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.BOTH;
        gbc.anchor = GridBagConstraints.EAST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(getGDAPanel(supportsLBFGS), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(getGLAPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 1;
        panel.add(m_components.getClassColComponent().getComponentPanel(), gbc);

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

    protected JComponent getGDAPanel(final boolean supportsLBFGS) {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory
            .createEtchedBorder(), " Gradient Descent Optimizer Settings "));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.EAST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(m_components.getUpdaterTypeComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getRegularizationComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getNoOfIterationsComponent().getComponentPanel(), gbc);

        if (supportsLBFGS) {
            gbc.gridy++;
            gbc.gridx = 0;
            panel.add(m_components.getOptimizationMethodComponent().getComponentPanel(), gbc);
            gbc.gridx++;
            panel.add(m_components.getNoOfCorrectionsComponent().getComponentPanel(), gbc);
            gbc.gridx++;
            panel.add(m_components.getToleranceComponent().getComponentPanel(), gbc);
        }

        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(m_components.getGradientTypeComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getStepSizeComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getFractionComponent().getComponentPanel(), gbc);
        return panel;
    }

    protected JComponent getGLAPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory
            .createEtchedBorder(), " Algorithm Settings "));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.EAST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridy++;
        panel.add(m_components.getUseFeatureScalingComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getAddInterceptComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_components.getValidateDataComponent().getComponentPanel(), gbc);
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] ports) throws NotConfigurableException {
        final DataTableSpec[] specs = MLlibNodeSettings.getTableSpecInDialog(0, ports);
        m_components.loadSettingsFrom(settings, specs[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        m_components.saveSettingsTo(settings);
    }
}
