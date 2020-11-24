/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   2020-10-14 (Alexander Bondaletov): created
 */
package org.knime.bigdata.dbfs.filehandling.node;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.bigdata.databricks.node.DbfsAuthenticationDialog;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnection;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.base.ui.WorkingDirectoryChooser;

/**
 * Node dialog for the {@link DbfsConnectorNodeModel} node.
 *
 * @author Alexander Bondaletov
 */
public class DbfsConnectorNodeDialog extends NodeDialogPane {

    private final DbfsConnectorNodeSettings m_settings;
    private final WorkingDirectoryChooser m_workingDirChooser;
    private DbfsAuthenticationDialog m_authPanel;

    /**
     * Creates new instance
     */
    public DbfsConnectorNodeDialog() {
        m_settings = new DbfsConnectorNodeSettings();

        m_authPanel = new DbfsAuthenticationDialog(m_settings.getAuthenticationSettings(), this);
        m_workingDirChooser = new WorkingDirectoryChooser("dbfs.workingDir", this::createFSConnection);

        addTab("Settings", createSettingsTab());
        addTab("Advanced", createAdvancedTab());
    }

    private JComponent createSettingsTab() {
        final JPanel panel = new JPanel();
        panel.setLayout(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(createDeploymentPanel(), gbc);
        gbc.gridy++;
        panel.add(createAuthPanel(), gbc);
        gbc.gridy++;
        panel.add(createFilesystemPanel(), gbc);
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        panel.add(new JLabel(), gbc);
        return panel;
    }

    private JComponent createDeploymentPanel() {
        DialogComponentString url = new DialogComponentString(m_settings.getUrlModel(), "Databricks URL:", true, 40);

        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        panel.add(url.getComponentPanel());
        panel.setBorder(BorderFactory.createTitledBorder("Connection settings"));
        return panel;
    }

    private JComponent createAuthPanel() {
        final JPanel panel = new JPanel();
        panel.setBorder(BorderFactory.createTitledBorder("Authentication"));
        panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
        panel.add(m_authPanel);
        return panel;
    }

    private JComponent createFilesystemPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.LINE_AXIS));
        panel.add(Box.createHorizontalStrut(5));
        panel.setBorder(BorderFactory.createTitledBorder("File System settings"));
        panel.add(m_workingDirChooser);
        return panel;
    }

    private JComponent createTimeoutPanel() {
        DialogComponentNumber connectionTimeout =
            new DialogComponentNumber(m_settings.getConnectionTimeoutModel(), null, 1, 6);
        DialogComponentNumber readTimeout =
            new DialogComponentNumber(m_settings.getReadTimeoutModel(), null, 1, 6);

        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.WEST;
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        c.weighty = 0;
        c.gridx = 0;
        c.gridy = 0;
        panel.add(new JLabel("Connection timeout (seconds): "), c);

        c.gridy = 1;
        panel.add(new JLabel("Read timeout (seconds): "), c);

        c.weightx = 1;
        c.gridx = 1;
        c.gridy = 0;
        panel.add(connectionTimeout.getComponentPanel(), c);

        c.gridy = 1;
        panel.add(readTimeout.getComponentPanel(), c);

        c.fill = GridBagConstraints.BOTH;
        c.gridx = 0;
        c.gridy++;
        c.gridwidth = 2;
        c.weightx = 1;
        c.weighty = 1;
        panel.add(Box.createVerticalGlue(), c);

        panel.setBorder(BorderFactory.createTitledBorder("Connection timeouts"));
        return panel;
    }

    private JComponent createAdvancedTab() {
        final JPanel advancedTabPanel = new JPanel();
        advancedTabPanel.setLayout(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        advancedTabPanel.add(createTimeoutPanel(), gbc);
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        advancedTabPanel.add(new JLabel(), gbc);
        return advancedTabPanel;
    }

    private FSConnection createFSConnection() throws IOException {
        return new DbfsFSConnection(m_settings, getCredentialsProvider());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_authPanel.saveSettingsTo(settings);
        m_settings.getWorkingDirectoryModel().setStringValue(m_workingDirChooser.getSelectedWorkingDirectory());

        m_settings.validate();

        m_settings.saveSettingsForDialog(settings);
        m_workingDirChooser.addCurrentSelectionToHistory();
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        try {
            m_authPanel.loadSettingsFrom(settings, specs);
            m_settings.loadSettingsForDialog(settings);
            m_workingDirChooser.setSelectedWorkingDirectory(m_settings.getWorkingDirectory());
        } catch (InvalidSettingsException ex) { // NOSONAR can be ignored
        }
    }

    @Override
    public void onOpen() {
        m_authPanel.onOpen();
    }

    @Override
    public void onClose() {
        m_workingDirChooser.onClose();
    }
}
