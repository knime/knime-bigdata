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
 */
package org.knime.bigdata.hadoop.filehandling.knox.node;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;

import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.Border;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnection;
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
 * WebHDFS via KNOX Connection node dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHdfsConnectorNodeDialog extends NodeDialogPane {

    private static final String WORKING_DIR_HISTORY_ID = "hdfs.workingDir";

    private final KnoxHdfsConnectorNodeSettings m_settings = new KnoxHdfsConnectorNodeSettings();

    private final DialogComponentString m_url =
        new DialogComponentString(m_settings.getURLSettingsModel(), "", true, 60);

    private final KnoxHdfsAuthenticationDialog m_auth =
        new KnoxHdfsAuthenticationDialog(m_settings.getAuthSettingsModel(), this);

    private final ChangeListener m_workingDirListener;

    private final WorkingDirectoryChooser m_workingDirChooser =
        new WorkingDirectoryChooser(WORKING_DIR_HISTORY_ID, this::createFSConnection);

    private final DialogComponentNumber m_connectionTimeout =
        new DialogComponentNumber(m_settings.getConnectionTimeoutSettingsModel(), null, 10);

    private final DialogComponentNumber m_receiveTimeout =
        new DialogComponentNumber(m_settings.getReceiveTimeoutSettingsModel(), null, 10);

    /**
     * Default constructor.
     */
    public KnoxHdfsConnectorNodeDialog() {
        m_workingDirListener = e -> updateWorkingDirSetting();

        addTab("Settings", createSettingsTab());
        addTab("Advanced", createAdvancedTab());
    }

    private JComponent createSettingsTab() {
        final JPanel panel = new JPanel();
        final BoxLayout parentLayout = new BoxLayout(panel, BoxLayout.Y_AXIS);
        panel.setLayout(parentLayout);

        panel.add(createConnectionSettingsPanel());
        panel.add(createPanel("Authentication settings", m_auth.getComponentPanel()));
        panel.add(createPanel("File System settings", m_workingDirChooser));

        return panel;
    }

    private JPanel createConnectionSettingsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = gbc.gridy = 0;
        gbc.weightx = gbc.weighty = 0;

        panel.setBorder(createTitledBorder("Connection settings"));
        addLine(panel, gbc, "URL: ", m_url.getComponentPanel());

        return panel;
    }

    private static JPanel createPanel(final String panelTitle, final JPanel comp) {
        final JPanel panel = new JPanel(new GridBagLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;

        panel.setBorder(createTitledBorder(panelTitle));
        panel.add(comp, gbc);
        return panel;
    }

    /**
     * @param title border title.
     * @return titled border.
     */
    private static Border createTitledBorder(final String title) {
        return new TitledBorder(new EtchedBorder(EtchedBorder.RAISED), title);
    }

    private JPanel createAdvancedTab() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = gbc.gridy = 0;
        gbc.weightx = gbc.weighty = 0;

        // space top side
        gbc.insets = new Insets(10, 0, 0, 0);
        panel.add(new JLabel(), gbc);

        addLine(panel, gbc, "Connection timeout (seconds):", m_connectionTimeout.getComponentPanel());
        addLine(panel, gbc, "Receive timeout (seconds):", m_receiveTimeout.getComponentPanel());

        // fill right side
        gbc.gridx++;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(new JLabel(), gbc);

        // fill bottom side
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.VERTICAL;
        panel.add(new JLabel(), gbc);

        return panel;
    }

    private static void addLine(final JPanel panel, final GridBagConstraints gbc, final String label,
        final JPanel inputPanel) {

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.insets = new Insets(5, 5, 5, 5);
        panel.add(new JLabel(label), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.insets = new Insets(0, 0, 0, 10);
        panel.add(inputPanel, gbc);
    }

    private void updateWorkingDirSetting() {
        m_settings.getWorkingDirectorySettingsModel().setStringValue(m_workingDirChooser.getSelectedWorkingDirectory());
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        try {
            m_url.loadSettingsFrom(settings, specs);
            m_settings.getWorkingDirectorySettingsModel().loadSettingsFrom(settings);
            m_workingDirChooser.setSelectedWorkingDirectory(m_settings.getWorkingDirectory());
            m_workingDirChooser.addListener(m_workingDirListener);
            m_auth.loadSettingsFrom(settings, specs);
            m_connectionTimeout.loadSettingsFrom(settings, specs);
            m_receiveTimeout.loadSettingsFrom(settings, specs);
        } catch (InvalidSettingsException e) { // NOSONAR ignore and use defaults
            // ignore and use defaults
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateValues();

        m_url.saveSettingsTo(settings);
        m_settings.getWorkingDirectorySettingsModel().saveSettingsTo(settings);
        m_auth.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
        m_receiveTimeout.saveSettingsTo(settings);

        m_workingDirChooser.addCurrentSelectionToHistory();
    }

    private FSConnection createFSConnection() throws IOException {
        KnoxHdfsConnectorNodeSettings clonedSettings = new KnoxHdfsConnectorNodeSettings(m_settings);
        return new KnoxHdfsFSConnection(clonedSettings, getCredentialsProvider());
    }

    @Override
    public void onOpen() {
        m_auth.onOpen();
    }

    @Override
    public void onClose() {
        m_auth.onClose();
        m_workingDirChooser.removeListener(m_workingDirListener);
        m_workingDirChooser.onClose();
    }
}
