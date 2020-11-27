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
package org.knime.bigdata.hadoop.filehandling.node;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.ListCellRenderer;
import javax.swing.border.Border;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.hadoop.filehandling.fs.HdfsFSConnection;
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
 * HDFS Connection node dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsConnectorNodeDialog extends NodeDialogPane implements ActionListener {

    private static final String WORKING_DIR_HISTORY_ID = "hdfs.workingDir";

    private final HdfsConnectorNodeSettings m_settings = new HdfsConnectorNodeSettings();

    private final JComboBox<HdfsProtocol> m_protocol;

    private final DialogComponentString m_host =
        new DialogComponentString(m_settings.getHostSettingsModel(), null, true, 40);

    private final JRadioButton m_useDefaultPortButton = new JRadioButton("default:");

    private final JLabel m_defaultPortLabel = new JLabel("8020");

    private final JRadioButton m_useCustomPortButton = new JRadioButton("custom:");

    private final DialogComponentNumber m_customPort =
        new DialogComponentNumber(m_settings.getCustomPortSettingsModel(), null, 1);

    private final HdfsAuthenticationDialog m_auth = new HdfsAuthenticationDialog(m_settings.getAuthSettingsModel());

    private final ChangeListener m_workingDirListener;

    private final WorkingDirectoryChooser m_workingDirChooser =
        new WorkingDirectoryChooser(WORKING_DIR_HISTORY_ID, this::createFSConnection);

    /**
     * Default constructor.
     */
    public HdfsConnectorNodeDialog() {
        m_protocol = new JComboBox<>(HdfsProtocol.values());
        m_protocol.setEditable(false);
        m_protocol.setRenderer(new ProtocolListCellRenderer());

        final ButtonGroup portButtonGroup = new ButtonGroup();
        portButtonGroup.add(m_useDefaultPortButton);
        portButtonGroup.add(m_useCustomPortButton);
        m_useDefaultPortButton.addActionListener(e -> updatePortSettings());
        m_useCustomPortButton.addActionListener(e -> updatePortSettings());

        m_workingDirListener = e -> updateWorkingDirSetting();

        addTab("Settings", createSettingsTab());
    }

    /**
     * {@link HdfsProtocol} combo box cell renderer.
     */
    private static final class ProtocolListCellRenderer extends JLabel implements ListCellRenderer<HdfsProtocol> {

        private static final long serialVersionUID = 1L;

        private ProtocolListCellRenderer() {
            super();
            setOpaque(true);
            setVerticalAlignment(CENTER);
        }

        @Override
        public Component getListCellRendererComponent(final JList<? extends HdfsProtocol> list, final HdfsProtocol value,
            final int index, final boolean isSelected, final boolean cellHasFocus) {

            if (isSelected) {
                setBackground(list.getSelectionBackground());
                setForeground(list.getSelectionForeground());
            } else {
                setBackground(list.getBackground());
                setForeground(list.getForeground());
            }

            setText(value.getText());

            return this;
        }
    }

    private JComponent createSettingsTab() {
        final JPanel panel = new JPanel();
        final BoxLayout parentLayout = new BoxLayout(panel, BoxLayout.Y_AXIS);
        panel.setLayout(parentLayout);

        panel.add(createConnectionSettingsPanel());
        panel.add(createAuthenticationSettingsPanel());
        panel.add(createFileSystemSettingsPanel());

        return panel;
    }

    private JPanel createConnectionSettingsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Connection settings"));

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = gbc.gridy = 0;
        gbc.weightx = gbc.weighty = 0;

        addLine(panel, gbc, "Protocol:", m_protocol, GridBagConstraints.WEST, 12);
        addLine(panel, gbc, "Host:", m_host.getComponentPanel(), GridBagConstraints.WEST, 0);
        addLine(panel, gbc, "Port:", createPortPanel(), GridBagConstraints.NORTHWEST, 5);

        return panel;
    }

    private Component createAuthenticationSettingsPanel() {
        final JPanel panel = new JPanel();
        final BoxLayout parentLayout = new BoxLayout(panel, BoxLayout.Y_AXIS);
        panel.setLayout(parentLayout);
        panel.setBorder(createTitledBorder("Authentication settings"));
        panel.add(m_auth.getComponentPanel());
        return panel;
    }

    private Component createFileSystemSettingsPanel() {
        final JPanel panel = new JPanel();
        final BoxLayout parentLayout = new BoxLayout(panel, BoxLayout.Y_AXIS);
        panel.setLayout(parentLayout);
        panel.setBorder(createTitledBorder("File System settings"));

        panel.add(m_workingDirChooser);
        return panel;
    }

    /**
     * @param title
     *            border title.
     * @return titled border.
     */
    private static Border createTitledBorder(final String title) {
        return new TitledBorder(new EtchedBorder(EtchedBorder.RAISED), title);
    }

    private JPanel createPortPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.weightx = gbc.weighty = 0;

        gbc.gridx = gbc.gridy = 0;
        gbc.insets = new Insets(5, 5, 0, 0);
        panel.add(m_useDefaultPortButton, gbc);
        gbc.gridx++;
        gbc.insets = new Insets(5, 15, 0, 5);
        panel.add(m_defaultPortLabel, gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 5, 0, 0);
        panel.add(m_useCustomPortButton, gbc);
        gbc.gridx++;
        gbc.insets = new Insets(5, 0, 0, 0);
        panel.add(m_customPort.getComponentPanel(), gbc);

        return panel;
    }

    private static void addLine(final JPanel panel, final GridBagConstraints gbc, final String label,
        final JComponent inputPanel, final int labelAnchor, final int inputInsetLeft) {

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = labelAnchor;
        gbc.insets = new Insets(5, 5, 5, 0);
        panel.add(new JLabel(label), gbc);
        gbc.gridx++;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.insets = new Insets(0, inputInsetLeft, 0, 0);
        panel.add(inputPanel, gbc);
        gbc.gridx++;
        gbc.insets = new Insets(0, 0, 0, 10);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(Box.createHorizontalGlue(), gbc);
    }

    private void updatePortSettings() {
        m_settings.setUseCustomPort(m_useCustomPortButton.isSelected());
    }

    private void updateWorkingDirSetting() {
        m_settings.getWorkingDirectorySettingsModel().setStringValue(m_workingDirChooser.getSelectedWorkingDirectory());
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        try {
            m_settings.getHadoopProtocolSettingsModel().loadSettingsFrom(settings);
            m_protocol.setSelectedItem(m_settings.getProtocol());
            m_host.loadSettingsFrom(settings, specs);
            m_settings.getUseCustomPortSettingsModel().loadSettingsFrom(settings);
            m_useDefaultPortButton.setSelected(!m_settings.useCustomPort());
            m_defaultPortLabel.setText(Integer.toString(m_settings.getDefaultPort()));
            m_useCustomPortButton.setSelected(m_settings.useCustomPort());
            m_customPort.loadSettingsFrom(settings, specs);
            m_settings.getCustomPortSettingsModel().setEnabled(m_settings.useCustomPort());
            m_settings.getWorkingDirectorySettingsModel().loadSettingsFrom(settings);
            m_workingDirChooser.setSelectedWorkingDirectory(m_settings.getWorkingDirectory());
            m_workingDirChooser.addListener(m_workingDirListener);
            m_auth.loadSettingsFrom(settings, specs);
        } catch (InvalidSettingsException e) { // NOSONAR ignore and use defaults
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateValues();

        m_settings.getHadoopProtocolSettingsModel().saveSettingsTo(settings);
        m_host.saveSettingsTo(settings);
        m_settings.getUseCustomPortSettingsModel().saveSettingsTo(settings);
        m_customPort.saveSettingsTo(settings);
        m_settings.getWorkingDirectorySettingsModel().saveSettingsTo(settings);
        m_auth.saveSettingsTo(settings);

        m_workingDirChooser.addCurrentSelectionToHistory();
    }

    private FSConnection createFSConnection() throws IOException {
        HdfsConnectorNodeSettings clonedSettings = new HdfsConnectorNodeSettings(m_settings);
        return new HdfsFSConnection(clonedSettings);
    }

    @Override
    public void onOpen() {
        m_protocol.addActionListener(this);
        m_auth.onOpen();
    }

    @Override
    public void onClose() {
        m_protocol.removeActionListener(this);
        m_auth.onClose();
        m_workingDirChooser.removeListener(m_workingDirListener);
        m_workingDirChooser.onClose();
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        final HdfsProtocol protocol = (HdfsProtocol)m_protocol.getSelectedItem();
        m_settings.getHadoopProtocolSettingsModel().setStringValue(protocol.toString());
        m_defaultPortLabel.setText(Integer.toString(protocol.getDefaultPort()));
    }
}
