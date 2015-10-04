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
package com.knime.bigdata.spark.node.mllib.prediction.linear;

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

import com.knime.bigdata.spark.node.mllib.MLlibNodeSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LinearMethodsNodeDialog extends NodeDialogPane {

    private final LinearMethodsSettings m_settings = new LinearMethodsSettings();

    /**
     * @param supportsLBFGS
     *
     */
    public LinearMethodsNodeDialog(final boolean supportsLBFGS) {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(m_settings.getNoOfIterationsComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_settings.getRegularizationComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_settings.getUpdaterTypeComponent().getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(m_settings.getUseFeatureScalingComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_settings.getAddInterceptComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_settings.getValidateDataComponent().getComponentPanel(), gbc);


        int startX = 0;
        gbc.gridwidth = 1;
        if (supportsLBFGS) {
            gbc.gridx = 0;
            gbc.gridy++;
            gbc.gridwidth = 3;
            panel.add(m_settings.getOptimizationMethodComponent().getComponentPanel(), gbc);
            gbc.gridwidth = 1;
            startX = 1;
            gbc.gridy++;
            gbc.gridx = startX;
            panel.add(m_settings.getToleranceComponent().getComponentPanel(), gbc);
            gbc.gridx++;
            panel.add(m_settings.getNoOfCorrectionsComponent().getComponentPanel(), gbc);
        }

        gbc.gridx = startX;
        gbc.gridy++;
        panel.add(m_settings.getFractionComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_settings.getStepSizeComponent().getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(m_settings.getGradientTypeComponent().getComponentPanel(), gbc);
        gbc.gridx++;
        gbc.gridwidth = 2;
        panel.add(m_settings.getClassColComponent().getComponentPanel(), gbc);

//        gbc.gridx = 0;
//        gbc.gridy++;
//        gbc.gridwidth = 3;
//        panel.add(m_settings.getClassColComponent().getComponentPanel(), gbc);


        gbc.gridwidth=3;
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        final JPanel colsPanel = m_settings.getFeatureColsComponent().getComponentPanel();
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
        final DataTableSpec[] specs = MLlibNodeSettings.getTableSpecInDialog(0, ports);
        m_settings.loadSettingsFrom(settings, specs[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        m_settings.saveSettingsTo(settings);
    }
}
