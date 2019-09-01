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
 *   Created on Sep 05, 2016 by Sascha
 */
package org.knime.bigdata.spark.node.io.database.db.writer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.bigdata.spark.node.SparkSaveMode;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.database.node.component.dbrowser.DBTableSelectorDialogComponent;
import org.knime.database.node.component.dbrowser.FlowVariableModelCreator;

/**
 * Dialog for the Spark to JDBC node.
 *
 * @author Sascha Wolke, KNIME.com
 */
class Spark2DBNodeDialog extends NodeDialogPane {
    private final Spark2DBSettings m_settings = new Spark2DBSettings();

    private final DBTableSelectorDialogComponent m_table;
    private final JCheckBox m_uploadDriver;
    private final JComboBox<SparkSaveMode> m_saveMode;

    Spark2DBNodeDialog() {
        m_table = new DBTableSelectorDialogComponent(m_settings.getSchemaAndTableModel(), 0, false, null,
            "Select a table", "Database Metadata Browser", true, new FlowVariableModelCreator() {
            @Override
            public FlowVariableModel create(final String[] keys, final Type type) {
                return createFlowVariableModel(keys, type);
            }
        });
        m_uploadDriver = new JCheckBox("Upload local JDBC driver.");
        m_saveMode = new JComboBox<>(SparkSaveMode.ALL);
        m_saveMode.setEditable(false);

        addTab("Destination settings", initLayout());
    }

    private JPanel initLayout() {
        final JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 2;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(0, 5, 0, 5);
        panel.add(m_table.getComponentPanel(), gbc);
        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.insets = new Insets(0, 10, 10, 0);
        panel.add(new JLabel("Driver:"), gbc);
        gbc.gridx++;
        panel.add(m_uploadDriver, gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new JLabel("Mode:"), gbc);
        gbc.gridx++;
        panel.add(m_saveMode, gbc);

        return panel;
    }

    private SparkSaveMode getSaveModeSelection() {
        return (SparkSaveMode) m_saveMode.getSelectedItem();
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {

        try {
            m_settings.loadSettings(settings);
            m_table.loadSettingsFrom(settings, specs);
            m_uploadDriver.setSelected(m_settings.uploadDriver());
            m_saveMode.setSelectedItem(m_settings.getSparkSaveMode());
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage(), e);
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_table.saveSettingsTo(settings);
        m_settings.setUploadDriver(m_uploadDriver.isSelected());
        m_settings.setSparkSaveMode(getSaveModeSelection());

        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }
}
