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
package com.knime.bigdata.spark.node.convert.stringmapper;

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
import org.knime.core.node.defaultnodesettings.DialogComponentStringListSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkStringMapperNodeDialog extends NodeDialogPane {

    @SuppressWarnings("unchecked")

    private final DialogComponentColumnFilter2 m_cols =
    new DialogComponentColumnFilter2(SparkStringMapperNodeModel.createColumnsModel(), 0);

//    private final DialogComponentColumnNameSelection m_col = new DialogComponentColumnNameSelection(
//        SparkStringMapperNodeModel.createColModel(), "Column name", 0, StringValue.class);
//    private final DialogComponentString m_colName = new DialogComponentString(
//        SparkStringMapperNodeModel.createColNameModel(), "Mapping column name: ", true, 20);

    private final String[] mappings = {MappingType.GLOBAL.toString(), MappingType.COLUMN.toString(), MappingType.BINARY.toString()};

    private final DialogComponentStringListSelection m_mappingType = new DialogComponentStringListSelection(
    new SettingsModelStringArray("Mapping type", new String[] {MappingType.COLUMN.toString()}),
    "Mapping type", mappings);

    SparkStringMapperNodeDialog() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gc = new GridBagConstraints();
        gc.gridx = 0;
        gc.gridy = 0;
        panel.add(m_cols.getComponentPanel(), gc);
        gc.gridy++;
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
