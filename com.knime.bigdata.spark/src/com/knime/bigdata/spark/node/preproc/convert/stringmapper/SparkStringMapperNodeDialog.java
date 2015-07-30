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
 *   Created on 06.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.convert.stringmapper;

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
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkStringMapperNodeDialog extends NodeDialogPane {

    private final DialogComponentColumnFilter2 m_cols =
    new DialogComponentColumnFilter2(SparkStringMapperNodeModel.createColumnsModel(), 0);

    private final String[] mappings = {MappingType.GLOBAL.toString(), MappingType.COLUMN.toString(), MappingType.BINARY.toString()};

    private final DialogComponentStringSelection m_mappingType = new DialogComponentStringSelection(
        SparkStringMapperNodeModel.createMappingTypeModel(), "Mapping type: ", mappings);

    SparkStringMapperNodeDialog() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gc = new GridBagConstraints();
        gc.gridx = 0;
        gc.gridy = 0;
        gc.weightx = 1;
        gc.weighty = 1;
        gc.fill = GridBagConstraints.BOTH;
        final JPanel colsPanel = m_cols.getComponentPanel();
        colsPanel.setBorder(BorderFactory.createTitledBorder(" Columns to convert "));
        panel.add(colsPanel, gc);
        gc.gridy++;
        gc.weightx = 1;
        gc.weighty = 0;
        panel.add(m_mappingType.getComponentPanel(), gc);
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
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)specs[0];
        final DataTableSpec spec = sparkSpec.getTableSpec();
        final DataTableSpec[] fakeSpec = new DataTableSpec[] {spec};
        m_cols.loadSettingsFrom(settings, fakeSpec);
        m_mappingType.loadSettingsFrom(settings, fakeSpec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_cols.saveSettingsTo(settings);
        m_mappingType.saveSettingsTo(settings);
    }

}
