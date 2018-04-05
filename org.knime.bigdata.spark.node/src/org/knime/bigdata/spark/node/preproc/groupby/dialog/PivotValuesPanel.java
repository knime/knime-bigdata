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
 *   Created on Feb 19, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.groupby.dialog;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.table.DefaultTableModel;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.NodeUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

/**
 * Pivot values configuration panel.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class PivotValuesPanel implements ActionListener, ChangeListener {

    private final SettingsModelStringArray m_settingsModel;
    private final JPanel m_panel;
    private final DefaultTableModel m_tableModel;
    private final JTable m_table;

    private final JTextField m_newValueField;
    private final JButton m_buttonAdd;

    private final JButton m_buttonRemove;
    private final JButton m_buttonRemoveAll;
    private final JButton m_buttonUp;
    private final JButton m_buttonDown;


    PivotValuesPanel(final SettingsModelStringArray settingsModel) {
        m_settingsModel = settingsModel;
        m_panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.insets = new Insets(0, 5, 5, 5);

        m_buttonAdd = new JButton("Add");
        m_buttonAdd.addActionListener(this);
        m_buttonUp = new JButton("Up");
        m_buttonUp.addActionListener(this);
        m_buttonDown = new JButton("Down");
        m_buttonDown.addActionListener(this);
        m_buttonRemove = new JButton("Remove");
        m_buttonRemove.addActionListener(this);
        m_buttonRemoveAll = new JButton("Remove All");
        m_buttonRemoveAll.addActionListener(this);

        m_newValueField = new JTextField();
        m_newValueField.addActionListener(this);
        m_panel.add(m_newValueField, gbc);
        gbc.gridx++;
        m_panel.add(m_buttonAdd, gbc);
        gbc.gridx = 0;
        gbc.gridy++;

        m_tableModel = new DefaultTableModel(0, 1);
        m_table = new JTable(m_tableModel);
        m_table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_table.setCellSelectionEnabled(true);
        m_table.setColumnSelectionAllowed(false);
        m_table.setTableHeader(null); // disable header

        final JScrollPane scrollPane = new JScrollPane(m_table);
        scrollPane.setPreferredSize(new Dimension(400, 240));
        gbc.insets = new Insets(0, 5, 0, 5);
        gbc.gridheight = 6;
        m_panel.add(scrollPane, gbc);
        gbc.gridx++;

        gbc.gridheight = 1;
        m_panel.add(m_buttonUp, gbc);
        gbc.gridy++;
        m_panel.add(m_buttonDown, gbc);
        gbc.gridy++;
        m_panel.add(m_buttonRemove, gbc);
        gbc.gridy++;
        m_panel.add(m_buttonRemoveAll, gbc);
    }

    @Override
    public void actionPerformed(final ActionEvent event) {
        int rowCount = m_table.getRowCount();
        int row = m_table.getSelectedRow();
        if (row == -1) {
            row = m_table.getEditingRow();
        }
        if (m_table.isEditing()) {
            m_table.getCellEditor().stopCellEditing();
        }

        if ((event.getSource().equals(m_buttonAdd) || event.getSource().equals(m_newValueField))
                && !StringUtils.isEmpty(m_newValueField.getText())) {
            m_tableModel.addRow(new Object[] { m_newValueField.getText() });
            m_newValueField.setText("");
            focusRow(m_tableModel.getRowCount() - 1);
            m_newValueField.requestFocus();

        } else if (event.getSource().equals(m_buttonUp) && row > 0) {
            m_tableModel.moveRow(row, row, row - 1);
            focusRow(row - 1);

        } else if (event.getSource().equals(m_buttonDown) && row != -1 && row + 1 < rowCount) {
            m_tableModel.moveRow(row, row, row + 1);
            focusRow(row + 1);

        } else if (event.getSource().equals(m_buttonRemove) && row != -1) {
            m_tableModel.removeRow(row);
            if (rowCount - 1 == 0) {
                m_table.clearSelection();
            } else if (row < rowCount - 1) {
                focusRow(row);
            } else {
                focusRow(Math.max(0, row - 1));
            }

        } else if (event.getSource().equals(m_buttonRemoveAll)) {
            m_table.clearSelection();
            m_tableModel.setRowCount(0);
        }
    }

    private final void focusRow(final int row) {
        m_table.requestFocus();
        m_table.changeSelection(row, 0, false, false);
        m_table.editCellAt(row, 0);
    }

    /**
     * @return the panel in which all sub-components of this component are
     *         arranged.
     */
    public JComponent getComponentPanel() {
        return m_panel;
    }

    /**
     * @param settings {@link NodeSettingsRO}
     * @throws NotConfigurableException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            m_settingsModel.loadSettingsFrom(settings);
            m_tableModel.setRowCount(0);
            for (String value : m_settingsModel.getStringArrayValue()) {
                m_tableModel.addRow(new String[] { value });
            }
            updateInputEnabledState();
            m_settingsModel.addChangeListener(this);

        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException("Unable to load manual pivoting values from configuration.", e);
        }
    }

    /** @return list with non blank distinct values (might be empty) */
    private List<String> getValues() {
        final ArrayList<String> values = new ArrayList<>(m_tableModel.getRowCount());
        for (int i = 0; i < m_tableModel.getRowCount(); i++) {
            final String value = (String) m_tableModel.getValueAt(i, 0);
            if (!StringUtils.isBlank(value) && !values.contains(value)) {
                values.add(value);
            }
        }
        return values;
    }

    /**
     * @param settings the settings object to write to
     * @throws InvalidSettingsException if model is enabled and empty
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        if (m_table.isEditing()) {
            m_table.getCellEditor().stopCellEditing();
        }
        m_table.clearSelection();

        m_settingsModel.removeChangeListener(this);
        m_settingsModel.setStringArrayValue(getValues().toArray(new String[0]));
        m_settingsModel.saveSettingsTo(settings);
    }

    /**
     * Validates that the list contains entries if enabled.
     * @throws InvalidSettingsException if enabled and list is empty
     */
    public void validate() throws InvalidSettingsException {
        if (m_table.isEditing()) {
            m_table.getCellEditor().stopCellEditing();
        }
        m_table.clearSelection();

        if (m_settingsModel.isEnabled() && getValues().isEmpty()) {
            throw new InvalidSettingsException("Unable to use pivoting with empty values list, provide at least one value or select auto mode.");
        }
    }

    /** Enable or disable fields on changes */
    private void updateInputEnabledState() {
        final boolean enabled = m_settingsModel.isEnabled();
        m_table.setEnabled(enabled);
        m_buttonAdd.setEnabled(enabled);
        m_buttonUp.setEnabled(enabled);
        m_buttonDown.setEnabled(enabled);
        m_buttonRemove.setEnabled(enabled);
        m_buttonRemoveAll.setEnabled(enabled);
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        updateInputEnabledState();
    }
}
