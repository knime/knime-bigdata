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
 *   Created on 06.07.2015 by koetter
 */
package org.knime.bigdata.spark.node.preproc.convert.category2number;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkCategory2NumberNodeDialog extends NodeDialogPane {

    private final DialogComponentColumnFilter2 m_cols =
    new DialogComponentColumnFilter2(SparkCategory2NumberNodeModel.createColumnsModel(), 0);

    private final SettingsModelString m_mappingTypeModel = SparkCategory2NumberNodeModel.createMappingTypeModel();
    private final DialogComponentStringSelection m_mappingType = new DialogComponentStringSelection(
        m_mappingTypeModel, "Mapping type: ", EnumContainer.getNames(MappingType.values()));

    private final DialogComponentBoolean m_keepOrigCols = new DialogComponentBoolean(
        SparkCategory2NumberNodeModel.createKeepOriginalColsModel(), "Keep original columns");
//    private final SettingsModelString m_suffixModel = SparkCategory2NumberNodeModel.createSuffixModel();
//    private final DialogComponentString m_suffix = new DialogComponentString(m_suffixModel, "Column suffix");

    SparkCategory2NumberNodeDialog() {
//        m_mappingTypeModel.addChangeListener(new ChangeListener() {
//            @Override
//            public void stateChanged(final ChangeEvent e) {
//                m_suffixModel.setEnabled(!MappingType.BINARY.name().equals(m_mappingTypeModel.getStringValue()));
//            }
//        });
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gc = new GridBagConstraints();
        gc.gridx = 0;
        gc.gridy = 0;
        gc.weightx = 1;
        gc.weighty = 0;
        panel.add(m_mappingType.getComponentPanel(), gc);
        gc.gridx++;
        panel.add(m_keepOrigCols.getComponentPanel(), gc);
//        gc.gridx++;
//        panel.add(m_suffix.getComponentPanel(), gc);

        gc.gridx = 0;
        gc.gridy++;
        gc.weightx = 1;
        gc.weighty = 1;
        gc.gridwidth = 2;
        gc.fill = GridBagConstraints.BOTH;
        final JPanel colsPanel = m_cols.getComponentPanel();
        colsPanel.setBorder(BorderFactory.createTitledBorder(" Columns to convert "));
        panel.add(colsPanel, gc);
        addTab("Settings", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs == null || specs.length < 1 || specs[0] == null) {
            throw new NotConfigurableException("No input spec available");
        }
        final DataTableSpec[] fakeSpec = MLlibNodeSettings.getTableSpecInDialog(0, specs);
        m_cols.loadSettingsFrom(settings, fakeSpec);
        m_mappingType.loadSettingsFrom(settings, fakeSpec);
        m_keepOrigCols.loadSettingsFrom(settings, fakeSpec);
//        m_suffix.loadSettingsFrom(settings, specs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_cols.saveSettingsTo(settings);
        m_mappingType.saveSettingsTo(settings);
        m_keepOrigCols.saveSettingsTo(settings);
//        m_suffix.saveSettingsTo(settings);
    }

}
