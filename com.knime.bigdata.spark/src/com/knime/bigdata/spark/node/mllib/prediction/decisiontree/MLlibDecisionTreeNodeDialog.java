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
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.NominalValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringListSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author koetter
 */
public class MLlibDecisionTreeNodeDialog extends NodeDialogPane {
    private final SettingsModelIntegerBounded m_maxNumberBinsModel = MLlibDecisionTreeNodeModel
        .createMaxNumberBinsModel();
    private final DialogComponentNumber m_maxNoBins = new DialogComponentNumber(m_maxNumberBinsModel,
        "Max number of bins: ", 10, createFlowVariableModel(m_maxNumberBinsModel));

    private final DialogComponentNumber m_maxDepth =
            new DialogComponentNumber(MLlibDecisionTreeNodeModel.createMaxDepthModel(), "Max depth: ", 25);

    private final DialogComponentColumnFilter2 m_cols =
            new DialogComponentColumnFilter2(MLlibDecisionTreeNodeModel.createColumnsModel(), 0);

    // quality measure
    private final String[] qualityMethods =
            {ParameterConstants.VALUE_GINI,
        ParameterConstants.VALUE_ENTROPY};
    private final DialogComponentStringListSelection m_quality = new DialogComponentStringListSelection(
        new SettingsModelStringArray(ParameterConstants.PARAM_INFORMATION_GAIN, new String[] {ParameterConstants.VALUE_GINI}),
        "Quality measure", qualityMethods);

    private final DialogComponentColumnNameSelection m_column =
            new DialogComponentColumnNameSelection(MLlibDecisionTreeNodeModel.createClassColModel(),
            "Class column ", 0, NominalValue.class);

    private final DialogComponent[] m_components = new DialogComponent[] {m_maxNoBins, m_maxDepth, m_quality, m_cols, m_column};

    /**
     *
     */
    public MLlibDecisionTreeNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        panel.add(m_maxNoBins.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_maxDepth.getComponentPanel(), gbc);
        gbc.gridx++;
        panel.add(m_quality.getComponentPanel(), gbc);

        gbc.gridwidth=2;
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1;
        gbc.weighty = 1;
        panel.add(m_cols.getComponentPanel(), gbc);

        gbc.gridy++;

        // class column selection
        panel.add(m_column.getComponentPanel(), gbc);

        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] ports) throws NotConfigurableException {
        SparkDataPortObjectSpec dbSpec = (SparkDataPortObjectSpec) ports[0];
        final DataTableSpec[] specs;
        if (dbSpec == null) {
            specs = new DataTableSpec[]{null};
        } else {
            specs = new DataTableSpec[]{dbSpec.getTableSpec()};
        }
        for (DialogComponent c : m_components) {
            c.loadSettingsFrom(settings, specs);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        for (DialogComponent c : m_components) {
            c.saveSettingsTo(settings);
        }
    }
}
