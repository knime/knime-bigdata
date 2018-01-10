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
package org.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Random;

import javax.swing.JButton;
import javax.swing.JPanel;

import org.knime.bigdata.spark.core.node.MLlibNodeComponents;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.port.PortObjectSpec;

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
    private final DialogComponentNumberEdit m_seed =
            new DialogComponentNumberEdit(MLlibKMeansNodeModel.createSeedModel(), "Initialization seed: ", 10);
    private final static Random RND = new Random();
    private final JButton m_nextSeedButton = new JButton("New");

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);
    private final MLlibNodeComponents<MLlibNodeSettings> m_components = new MLlibNodeComponents<>(m_settings);
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

        gbc.gridx = 0;
        gbc.gridy++;
        final JPanel seedPanel = new JPanel(new BorderLayout());
        seedPanel.add(m_seed.getComponentPanel(), BorderLayout.WEST);
        seedPanel.add(m_nextSeedButton, BorderLayout.EAST);
        panel.add(seedPanel, gbc);
        m_nextSeedButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                // generate a integer here to avoid huge numbers in the dialog
                ((SettingsModelLong) m_seed.getModel()).setLongValue(RND.nextInt());
            }
        });

        gbc.gridwidth = 2;
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        panel.add(m_components.getFeatureColsComponent().getComponentPanel(), gbc);
        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_noOfCluster.saveSettingsTo(settings);
        m_noOfIterations.saveSettingsTo(settings);
        m_components.saveSettingsTo(settings);
        m_seed.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, specs);
        m_noOfCluster.loadSettingsFrom(settings, tableSpecs);
        m_noOfIterations.loadSettingsFrom(settings, tableSpecs);
        m_components.loadSettingsFrom(settings, tableSpecs[0]);
        m_seed.loadSettingsFrom(settings, tableSpecs);
    }
}
