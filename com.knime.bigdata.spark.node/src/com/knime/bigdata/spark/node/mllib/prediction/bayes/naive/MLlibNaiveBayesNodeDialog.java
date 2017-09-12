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
package com.knime.bigdata.spark.node.mllib.prediction.bayes.naive;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.node.MLlibNodeComponents;
import com.knime.bigdata.spark.core.node.MLlibNodeSettings;
import com.knime.bigdata.spark.core.node.AbstractSparkNodeDialogPane;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibNaiveBayesNodeDialog extends AbstractSparkNodeDialogPane {
    private final DialogComponentNumber m_lambda =
            new DialogComponentNumber(MLlibNaiveBayesNodeModel.createLambdaModel(), "Lambda: ", 0.05);

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(true);
    private final MLlibNodeComponents<MLlibNodeSettings> m_components = new MLlibNodeComponents<>(m_settings);

    /**
     *
     */
    public MLlibNaiveBayesNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(m_lambda.getComponentPanel(), gbc);
        gbc.gridx++;
        // class column selection
        panel.add(m_components.getClassColComponent().getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 2;
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
    protected void saveAdditionalSparkSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        m_lambda.saveSettingsTo(settings);
        m_components.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalSparkSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] ports) throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, ports);
        m_lambda.loadSettingsFrom(settings, tableSpecs);
        m_components.loadSettingsFrom(settings, tableSpecs[0]);
    }

}
