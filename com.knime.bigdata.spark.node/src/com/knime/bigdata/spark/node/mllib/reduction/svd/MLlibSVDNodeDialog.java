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
package com.knime.bigdata.spark.node.mllib.reduction.svd;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.node.MLlibNodeSettings;

/**
 *
 * @author koetter
 */
public class MLlibSVDNodeDialog extends NodeDialogPane {
    private final DialogComponentNumber m_noOfSingularValues =
            new DialogComponentNumber(MLlibSVDNodeModel.createNoSingularValuesModel(), "Number of leading singular values: ", 10);
    private final DialogComponentNumber m_reciprocalCondition =
            new DialogComponentNumber(MLlibSVDNodeModel.createReciprocalConditionModel(), "Reciprocal condition number: ", 1e-9, 15);
    private final DialogComponentBoolean m_computeU =
            new DialogComponentBoolean(MLlibSVDNodeModel.createComputeUModel(), "Compute U matrix");
    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);

    /**
     *
     */
    public MLlibSVDNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        panel.add(m_noOfSingularValues.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_reciprocalCondition.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_computeU.getComponentPanel(), gbc);
        gbc.gridwidth=3;
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        panel.add(m_settings.getFeatureColsComponent().getComponentPanel(), gbc);
        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_noOfSingularValues.saveSettingsTo(settings);
        m_reciprocalCondition.saveSettingsTo(settings);
        m_computeU.saveSettingsTo(settings);
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, specs);
        m_noOfSingularValues.loadSettingsFrom(settings, tableSpecs);
        m_reciprocalCondition.loadSettingsFrom(settings, tableSpecs);
        m_computeU.loadSettingsFrom(settings, tableSpecs);
        m_settings.loadSettingsFrom(settings, tableSpecs[0]);
    }
}
