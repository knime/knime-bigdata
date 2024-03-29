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
 *   Created on 09.05.2014 by thor
 */
package org.knime.bigdata.hive.node.loader;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.base.node.io.database.DBSQLTypesPanel;
import org.knime.bigdata.filehandling.local.HDFSLocalRemoteFileHandler;
import org.knime.bigdata.hive.utility.AbstractLoaderNodeModel;
import org.knime.bigdata.hive.utility.LoaderSettings;
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
 * Dialog for the Hive Loader node.
 *
 * @author Thorsten Meinl, KNIME AG, Zurich, Switzerland
 */
@Deprecated
class HiveLoaderNodeDialog extends NodeDialogPane {
    private final LoaderSettings m_settings = new LoaderSettings();

    private final DBSQLTypesPanel m_typeMappingPanel = new DBSQLTypesPanel();

    private final JLabel m_info = new JLabel();

    private final RemoteFileChooserPanel m_target;

    private final JTextField m_tableName = new JTextField();

    private final JCheckBox m_dropTableIfExists = new JCheckBox("Drop existing table");

    /** Connected to local big data environment with broken Spark Thriftserver (BD-729) */
    private boolean m_isLocalHDFS = false;

    @SuppressWarnings("unchecked")
    private final ColumnFilterPanel m_partitionColumns = new ColumnFilterPanel(false, StringValue.class,
        IntValue.class, DateAndTimeValue.class);

    HiveLoaderNodeDialog() {
        JPanel p = new JPanel(new GridBagLayout());
        m_target =
            new RemoteFileChooserPanel(p, "", false, "targetHistory", RemoteFileChooser.SELECT_DIR,
                createFlowVariableModel(LoaderSettings.CFG_TARGET_FOLDER, FlowVariable.Type.STRING), null);

        GridBagConstraints c = new GridBagConstraints();
        NodeUtils.resetGBC(c);

        c.gridx = 0;
        c.gridy++;
        c.anchor = GridBagConstraints.WEST;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        c.gridwidth = 2;
        p.add(m_info, c);

        c.gridy++;
        c.gridwidth = 1;
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        p.add(new JLabel("Target folder: "), c);
        c.gridx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        p.add(m_target.getPanel(), c);

        c.gridx = 0;
        c.gridy++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        p.add(new JLabel("Table name: "), c);
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

        if (specs.length > 0 && specs[0] != null) {
            ConnectionInformation connInfo = ((ConnectionInformationPortObjectSpec) specs[0]).getConnectionInformation();
            m_target.setConnectionInformation(connInfo);
            m_isLocalHDFS = HDFSLocalRemoteFileHandler.isSupportedConnection(connInfo);
        } else {
            m_target.setConnectionInformation(null);
            m_isLocalHDFS = false;
        }

        m_target.setSelection(m_settings.targetFolder());
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
        if (StringUtils.isBlank(m_target.getSelection())) {
            throw new InvalidSettingsException("Target folder required.");
        }

        if (m_isLocalHDFS) {
            AbstractLoaderNodeModel.validateLocalTargetFolder(m_target.getSelection());
        }

        if (StringUtils.isBlank(m_tableName.getText())) {
            throw new InvalidSettingsException("Table name required.");
        }

        m_typeMappingPanel.saveSettingsTo(m_settings.getTypeMapping());
        m_settings.targetFolder(m_target.getSelection());
        m_settings.tableName(m_tableName.getText());
        m_settings.dropTableIfExists(m_dropTableIfExists.isSelected());
        m_settings.partitionColumn(m_partitionColumns.getIncludedColumnSet());
        m_settings.saveSettings(settings);
    }
}
