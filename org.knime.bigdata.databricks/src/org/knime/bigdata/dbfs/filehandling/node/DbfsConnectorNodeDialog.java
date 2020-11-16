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
        Box box = new Box(BoxLayout.Y_AXIS);
        box.add(createDeploymentPanel());
        box.add(createAuthPanel());
        box.add(createFilesystemPanel());
        return box;
    }

    private JComponent createDeploymentPanel() {
        DialogComponentString host = new DialogComponentString(m_settings.getHostModel(), "Host", true, 40);
        DialogComponentNumber port = new DialogComponentNumber(m_settings.getPortModel(), "Port", 1);

        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        panel.add(host.getComponentPanel());
        panel.add(port.getComponentPanel());
        panel.setBorder(BorderFactory.createTitledBorder("Deployment URL"));
        return panel;
    }

    private JComponent createAuthPanel() {
        final JPanel panel = new JPanel();
        panel.setBorder(BorderFactory.createTitledBorder("Authentication"));
        panel.add(m_authPanel);
        return panel;
    }

    private JComponent createFilesystemPanel() {
        m_workingDirChooser.setBorder(BorderFactory.createTitledBorder("File system settings"));
        return m_workingDirChooser;
    }

    private JComponent createAdvancedTab() {
        DialogComponentNumber connectionTimeout =
            new DialogComponentNumber(m_settings.getConnectionTimeoutModel(), "", 1);
        DialogComponentNumber readTimeout =
            new DialogComponentNumber(m_settings.getReadTimeoutModel(), "", 1);

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

        panel.setBorder(BorderFactory.createTitledBorder("Connection settings"));
        return panel;
    }

    private FSConnection createFSConnection() throws IOException {
        return new DbfsFSConnection(m_settings, getCredentialsProvider());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        preSettingsSave();
        validateBeforeSaving();
        m_authPanel.saveSettingsTo(settings.addNodeSettings(DbfsConnectorNodeSettings.KEY_AUTH));
        m_settings.saveSettingsForDialog(settings);
    }

    private void preSettingsSave() {
        m_settings.getWorkingDirectoryModel().setStringValue(m_workingDirChooser.getSelectedWorkingDirectory());
    }

    private void validateBeforeSaving() throws InvalidSettingsException {
        m_settings.validate();
        m_workingDirChooser.addCurrentSelectionToHistory();
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        try {
            m_authPanel.loadSettingsFrom(settings.getNodeSettings(DbfsConnectorNodeSettings.KEY_AUTH), specs);
            m_settings.loadSettingsForDialog(settings);
        } catch (InvalidSettingsException ex) { // NOSONAR can be ignored
        }
    }

    @Override
    public void onOpen() {
        m_workingDirChooser.setSelectedWorkingDirectory(m_settings.getWorkingDirectory());
        m_authPanel.onOpen();
    }

    @Override
    public void onClose() {
        m_workingDirChooser.onClose();
    }
}
