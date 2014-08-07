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
 *   Created on 09.05.2014 by thor
 */
package com.knime.database.impala.node.loader;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.base.node.io.database.DBSQLTypesPanel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IntValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ColumnFilterPanel;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Dialog for the Impala Loader node.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
class ImpalaLoaderNodeDialog extends NodeDialogPane {
    private final ImpalaLoaderSettings m_settings = new ImpalaLoaderSettings();

    private final DBSQLTypesPanel m_typeMappingPanel = new DBSQLTypesPanel();

    private final JLabel m_info = new JLabel();

    private final RemoteFileChooserPanel m_target;

    private final JTextField m_tableName = new JTextField();

    private final JCheckBox m_dropTableIfExists = new JCheckBox("Drop existing table");

    @SuppressWarnings("unchecked")
    private final ColumnFilterPanel m_partitionColumns = new ColumnFilterPanel(false, StringValue.class,
        IntValue.class, DateAndTimeValue.class);

    ImpalaLoaderNodeDialog() {
        JPanel p = new JPanel(new GridBagLayout());
        m_target =
            new RemoteFileChooserPanel(p, "", false, "targetHistory", RemoteFileChooser.SELECT_DIR,
                createFlowVariableModel("target", FlowVariable.Type.STRING), null);

        GridBagConstraints c = new GridBagConstraints();

        c.gridx = 0;
        c.gridy++;
        c.insets = new Insets(2, 2, 2, 2);
        c.anchor = GridBagConstraints.WEST;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        c.gridwidth = 2;
        p.add(m_info, c);

        c.gridy++;
        c.gridwidth = 1;
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        p.add(new JLabel("Target folder   "), c);
        c.gridx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        p.add(m_target.getPanel(), c);

        c.gridx = 0;
        c.gridy++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        p.add(new JLabel("Table name   "), c);
        c.gridx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        p.add(m_tableName, c);

        c.gridx = 0;
        c.gridy++;
        c.gridwidth = 2;
        p.add(m_dropTableIfExists, c);

        c.gridy++;
        m_partitionColumns.setBorder(BorderFactory.createTitledBorder("Partition columns"));
        p.add(m_partitionColumns, c);

        addTab("Load settings", p);
        addTab("SQL Types", m_typeMappingPanel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_settings.loadSettingsForDialog(settings);
        try {
            m_settings.guessTypeMapping((DataTableSpec)specs[1], true);
        } catch (InvalidSettingsException ex) {
            // does not happen
        }

        ConnectionInformation connInfo = ((ConnectionInformationPortObjectSpec)specs[0]).getConnectionInformation();

        m_target.setConnectionInformation(connInfo);
        m_target.setSelection(m_settings.targetFolder());
        m_info.setText("Upload to " + connInfo.toURI());
        m_tableName.setText(m_settings.tableName());
        m_dropTableIfExists.setSelected(m_settings.dropTableIfExists());
        m_partitionColumns.update((DataTableSpec)specs[1], false, m_settings.partitionColumns());
        m_typeMappingPanel.loadSettingsFrom(m_settings.getTypeMapping(), (DataTableSpec)specs[1]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_typeMappingPanel.saveSettingsTo(m_settings.getTypeMapping());
        m_settings.targetFolder(m_target.getSelection());
        m_settings.tableName(m_tableName.getText());
        m_settings.dropTableIfExists(m_dropTableIfExists.isSelected());
        m_settings.partitionColumn(m_partitionColumns.getIncludedColumnSet());
        m_settings.saveSettings(settings);
    }
}
