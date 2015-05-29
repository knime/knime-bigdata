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
package com.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author koetter
 */
public class MLlibKMeansNodeDialog extends NodeDialogPane {
    private final SettingsModelIntegerBounded m_noOfClusterModel = MLlibKMeansNodeModel.createNoOfClusterModel();
    private final DialogComponentNumber m_noOfCluster = new DialogComponentNumber(m_noOfClusterModel,
        "Number of clusters: ", 1, createFlowVariableModel(m_noOfClusterModel));
    private final DialogComponentNumber m_noOfIterations =
            new DialogComponentNumber(MLlibKMeansNodeModel.createNoOfIterationModel(), "Number of iterations: ", 10);
    private final DialogComponentColumnFilter2 m_cols =
            new DialogComponentColumnFilter2(MLlibKMeansNodeModel.createColumnsModel(), 0);
    private final DialogComponentString m_resultCol =
            new DialogComponentString(MLlibKMeansNodeModel.createColumnNameModel(), "Column name: ", true, 30);
    /**
     *
     */
    public MLlibKMeansNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        panel.add(m_noOfCluster.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_noOfIterations.getComponentPanel(), gbc);
        gbc.gridwidth=2;
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        panel.add(m_cols.getComponentPanel(), gbc);
        gbc.gridy++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        gbc.weighty = 0;
        panel.add(m_resultCol.getComponentPanel(), gbc);
        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_noOfCluster.saveSettingsTo(settings);
        m_noOfIterations.saveSettingsTo(settings);
        m_cols.saveSettingsTo(settings);
        m_resultCol.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs == null || specs.length != 1 || specs[0] == null) {
            throw new NotConfigurableException("No input connection available");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)specs[0];
        final DataTableSpec[] tableSpecs = new DataTableSpec[] {spec.getTableSpec()};
        m_noOfCluster.loadSettingsFrom(settings, tableSpecs);
        m_noOfIterations.loadSettingsFrom(settings, tableSpecs);
        m_cols.loadSettingsFrom(settings, tableSpecs);
        m_resultCol.loadSettingsFrom(settings, tableSpecs);
    }
}
