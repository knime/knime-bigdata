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
package com.knime.bigdata.spark.node.mllib.collaborativefiltering;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.node.MLlibNodeSettings;
import com.knime.bigdata.spark.core.node.AbstractSparkNodeDialogPane;

/**
 *
 * @author koetter
 */
public class MLlibCollaborativeFilteringNodeDialog extends AbstractSparkNodeDialogPane {
    @SuppressWarnings("unchecked")
    private final DialogComponentColumnNameSelection m_userCol = new DialogComponentColumnNameSelection(
        MLlibCollaborativeFilteringNodeModel.createUserColModel(), "User column: ", 0, IntValue.class);
    @SuppressWarnings("unchecked")
    private final DialogComponentColumnNameSelection m_productCol = new DialogComponentColumnNameSelection(
        MLlibCollaborativeFilteringNodeModel.createProductColModel(), "Product column: ", 0, IntValue.class);
    @SuppressWarnings("unchecked")
    private final DialogComponentColumnNameSelection m_ratingCol = new DialogComponentColumnNameSelection(
        MLlibCollaborativeFilteringNodeModel.createRatingColModel(), "Rating column: ", 0, DoubleValue.class);
    private final DialogComponentNumber m_lambda =
            new DialogComponentNumber(MLlibCollaborativeFilteringNodeModel.createLambdaModel(), "Lambda: ", 0.01, 5);
    private final DialogComponentNumber m_alpha =
            new DialogComponentNumber(MLlibCollaborativeFilteringNodeModel.createAlphaModel(), "Alpha: ", 0.0001, 5);
    private final DialogComponentNumber m_iterations = new DialogComponentNumber(
        MLlibCollaborativeFilteringNodeModel.createIterationsModel(), "Iterations: ", 1, 5);
    private final DialogComponentNumber m_rank =
            new DialogComponentNumber(MLlibCollaborativeFilteringNodeModel.createRankModel(), "Rank: ", 1, 5);
    private final DialogComponentNumber m_blocks =
            new DialogComponentNumber(MLlibCollaborativeFilteringNodeModel.createNoOfBlocksModel(), "Number of blocks: ", 1, 5);
    private final DialogComponentBoolean m_implicitPrefs =
            new DialogComponentBoolean(MLlibCollaborativeFilteringNodeModel.createImplicitPrefsModel(), "Implicit feedback");


    /**
     *
     */
    public MLlibCollaborativeFilteringNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 2;
        gbc.weightx = 1;
        panel.add(m_userCol.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_productCol.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_ratingCol.getComponentPanel(), gbc);
        gbc.anchor = GridBagConstraints.EAST;
        gbc.gridwidth=1;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(m_lambda.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_alpha.getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(m_rank.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_iterations.getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(m_blocks.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_implicitPrefs.getComponentPanel(), gbc);
        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSparkSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_userCol.saveSettingsTo(settings);
        m_productCol.saveSettingsTo(settings);
        m_ratingCol.saveSettingsTo(settings);
        m_lambda.saveSettingsTo(settings);
        m_alpha.saveSettingsTo(settings);
        m_iterations.saveSettingsTo(settings);
        m_rank.saveSettingsTo(settings);
        m_blocks.saveSettingsTo(settings);
        m_implicitPrefs.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalSparkSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, specs);
        m_userCol.loadSettingsFrom(settings, tableSpecs);
        m_productCol.loadSettingsFrom(settings, tableSpecs);
        m_ratingCol.loadSettingsFrom(settings, tableSpecs);
        m_lambda.loadSettingsFrom(settings, tableSpecs);
        m_alpha.loadSettingsFrom(settings, tableSpecs);
        m_iterations.loadSettingsFrom(settings, tableSpecs);
        m_rank.loadSettingsFrom(settings, tableSpecs);
        m_blocks.loadSettingsFrom(settings, tableSpecs);
        m_implicitPrefs.loadSettingsFrom(settings, tableSpecs);
    }
}
