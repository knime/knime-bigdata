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
 *   Created on Sep 05, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.database.reader;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.NodeUtils;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabasePortObjectSpec;

/**
 * Dialog for the JDBC to Spark node.
 *
 * @author Sascha Wolke, KNIME.com
 */
class Database2SparkNodeDialog extends NodeDialogPane implements ActionListener {
    private final Database2SparkSettings m_settings = new Database2SparkSettings();

    private final JCheckBox m_uploadDriver;
    private final JCheckBox m_useDefaultFetchSize;
    private final JSpinner m_fetchSize;
    private final JComboBox<String> m_partitionColumn;
    private final JCheckBox m_autoBounds;
    private final JSpinner m_lowerBound;
    private final JSpinner m_upperBound;
    private final JSpinner m_numPartitions;

    Database2SparkNodeDialog() {
        m_uploadDriver = new JCheckBox("Upload local JDBC driver.");
        m_useDefaultFetchSize = new JCheckBox("Use driver default fetch size.");
        m_useDefaultFetchSize.addActionListener(this);
        m_fetchSize = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 10));
        m_partitionColumn = new JComboBox<String>();
        m_partitionColumn.addActionListener(this);
        m_partitionColumn.setEditable(false);
        m_autoBounds = new JCheckBox("Query DB for lower and upper bound.");
        m_autoBounds.addActionListener(this);
        m_lowerBound = new JSpinner(new SpinnerNumberModel(0, 0, Long.MAX_VALUE, 10));
        m_upperBound = new JSpinner(new SpinnerNumberModel(100, 0, Long.MAX_VALUE, 10));
        m_numPartitions = new JSpinner(new SpinnerNumberModel(2, 0, Integer.MAX_VALUE, 1));

        addTab("Source settings", createSourcePanel());
    }

    private JPanel createSourcePanel() {
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
        panel.add(new JLabel("Fetch size: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_useDefaultFetchSize, gbc);
        gbc.gridy++;
        gbc.weightx = 1;
        panel.add(m_fetchSize, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Partition column: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_partitionColumn, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Bound: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_autoBounds, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Lower bound: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_lowerBound, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Upper bound: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_upperBound, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Num partitions: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_numPartitions, gbc);

        return panel;
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        if (e.getSource().equals(m_useDefaultFetchSize)) {
            m_fetchSize.setEnabled(!m_useDefaultFetchSize.isSelected());

        } else if (e.getSource().equals(m_partitionColumn) || e.getSource().equals(m_autoBounds)) {
            updatePartitionFieldsEnabled();
        }
    }

    /** Enable/disable partitioning fields */
    private void updatePartitionFieldsEnabled() {
        final boolean usePartitioning = !StringUtils.isBlank((String) m_partitionColumn.getSelectedItem());
        final boolean autoBounds = m_autoBounds.isSelected();

        if (usePartitioning && !autoBounds) {
            m_autoBounds.setEnabled(true);
            m_lowerBound.setEnabled(true);
            m_upperBound.setEnabled(true);
            m_numPartitions.setEnabled(true);

        } else if (usePartitioning) {
            m_autoBounds.setEnabled(true);
            m_lowerBound.setEnabled(false);
            m_upperBound.setEnabled(false);
            m_numPartitions.setEnabled(true);

        } else {
            m_autoBounds.setEnabled(false);
            m_lowerBound.setEnabled(false);
            m_upperBound.setEnabled(false);
            m_numPartitions.setEnabled(false);
        }
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        m_settings.loadSettingsFrom(settings);
        m_uploadDriver.setSelected(m_settings.uploadDriver());

        m_useDefaultFetchSize.setSelected(m_settings.useDefaultFetchSize());
        m_fetchSize.setValue(m_settings.getFetchSize());
        m_fetchSize.setEnabled(!m_settings.useDefaultFetchSize());

        DatabasePortObjectSpec spec = (DatabasePortObjectSpec)specs[0];
        if (specs[0] == null) {
            throw new NotConfigurableException("No database connection details available. Please execute predecessors first.");
        }

        try {
            final DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) m_partitionColumn.getModel();
            model.removeAllElements();
            model.addElement(""); // default

            for (DataColumnSpec colSpec : spec.getDataTableSpec()) {
                if (colSpec.getType().isCompatible(DoubleValue.class)){
                    model.addElement(colSpec.getName());
                }
            }

            String partCol = m_settings.getPartitionColumn();
            if (!StringUtils.isBlank(partCol) && spec.getDataTableSpec().getColumnSpec(partCol) != null) {
                model.setSelectedItem(m_settings.getPartitionColumn());
            } else {
                model.setSelectedItem("");
            }
            m_autoBounds.setSelected(m_settings.useAutoBounds());
            m_lowerBound.setValue(m_settings.getLowerBound());
            m_upperBound.setValue(m_settings.getUpperBound());
            m_numPartitions.setValue(m_settings.getNumPartitions());
            updatePartitionFieldsEnabled();

        } catch(Exception e) {
            throw new NotConfigurableException("Unable to load database details: " + e.getMessage());
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setUploadDriver(m_uploadDriver.isSelected());

        m_settings.setUseDefaultFetchSize(m_useDefaultFetchSize.isSelected());
        m_settings.setFetchSize(((SpinnerNumberModel) m_fetchSize.getModel()).getNumber().intValue());

        m_settings.setPartitionColumn((String) m_partitionColumn.getSelectedItem());
        m_settings.setAutoBounds(m_autoBounds.isSelected());
        m_settings.setLowerBound(((SpinnerNumberModel) m_lowerBound.getModel()).getNumber().longValue());
        m_settings.setUpperBound(((SpinnerNumberModel) m_upperBound.getModel()).getNumber().longValue());
        m_settings.setNumPartitions(((SpinnerNumberModel) m_numPartitions.getModel()).getNumber().intValue());

        m_settings.validateSettings();

        m_settings.saveSettingsTo(settings);
    }
}
