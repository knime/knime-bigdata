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
package org.knime.bigdata.spark.node.io.database.writer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.knime.base.filehandling.NodeUtils;
import org.knime.bigdata.spark.node.SparkSaveMode;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for the Spark to JDBC node.
 *
 * @author Sascha Wolke, KNIME.com
 */
class Spark2DatabaseNodeDialog extends NodeDialogPane {
    private final Spark2DatabaseSettings m_settings = new Spark2DatabaseSettings();

    private final JCheckBox m_uploadDriver;
    private final JTextField m_table;
    private final JComboBox<SparkSaveMode> m_saveMode;

    Spark2DatabaseNodeDialog() {
        m_uploadDriver = new JCheckBox("Upload local JDBC driver.");
        m_table = new JTextField();
        m_saveMode = new JComboBox<>(SparkSaveMode.ALL);
        m_saveMode.setEditable(false);

        addTab("Destination settings", initLayout());
    }

    private JPanel initLayout() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        gbc.weightx = 0;
        panel.add(new JLabel("Driver: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_uploadDriver, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Table name: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_table, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Save mode: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_saveMode, gbc);

        return panel;
    }

    private SparkSaveMode getSaveModeSelection() {
        return (SparkSaveMode) m_saveMode.getSelectedItem();
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {

        m_settings.loadSettings(settings);
        m_uploadDriver.setSelected(m_settings.uploadDriver());
        m_table.setText(m_settings.getTable());
        m_saveMode.setSelectedItem(m_settings.getSparkSaveMode());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setUploadDriver(m_uploadDriver.isSelected());
        m_settings.setTable(m_table.getText());
        m_settings.setSaveMode(getSaveModeSelection());

        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }
}
