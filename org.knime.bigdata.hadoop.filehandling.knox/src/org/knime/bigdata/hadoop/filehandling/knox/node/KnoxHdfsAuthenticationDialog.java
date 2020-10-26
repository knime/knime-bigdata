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

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.hadoop.filehandling.knox.node.KnoxHdfsAuthenticationSettings.AuthType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentFlowVariableNameSelection2;
import org.knime.core.node.defaultnodesettings.DialogComponentPasswordField;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.VariableType.CredentialsType;

/**
 * HDFS Connection node dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHdfsAuthenticationDialog implements ChangeListener {

    private final JPanel m_componentPanel;

    private final KnoxHdfsAuthenticationSettings m_settings;

    private final DialogComponentButtonGroup m_authType;

    private final DialogComponentFlowVariableNameSelection2 m_credential;

    private final DialogComponentString m_user;

    private final DialogComponentPasswordField m_pass;

    /**
     * Default constructor.
     *
     * @param settings authentication settings model to use
     * @param parentDialog parent dialog with flow variables
     */
    public KnoxHdfsAuthenticationDialog(final KnoxHdfsAuthenticationSettings settings,
        final NodeDialogPane parentDialog) {

        m_settings = settings;
        m_authType = new DialogComponentButtonGroup(settings.getAuthTypeSettingsModel(), "Auth", true, AuthType.values());
        m_credential = new DialogComponentFlowVariableNameSelection2(settings.getCredentialsSettingsModel(), "",
            () -> parentDialog.getAvailableFlowVariables(CredentialsType.INSTANCE));
        m_user = new DialogComponentString(settings.getUserSettingsModel(), null, false, 35);
        m_pass = new DialogComponentPasswordField(settings.getPassSettingsModel(), null, 35);

        m_componentPanel = new JPanel();
        final BoxLayout boxLayout = new BoxLayout(m_componentPanel, BoxLayout.PAGE_AXIS);
        m_componentPanel.setLayout(boxLayout);
        m_componentPanel.add(createCredentialsAuthPanel());
        m_componentPanel.add(createUserPassAuthPanel());
    }

    JPanel getComponentPanel() {
        return m_componentPanel;
    }

    private JPanel createCredentialsAuthPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(0, 5, 0, 0);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(m_authType.getButton(AuthType.CREDENTIALS.toString()), gbc);

        gbc.gridx++;
        gbc.insets = new Insets(0, 0, 0, 0);
        panel.add(m_credential.getComponentPanel(), gbc);

        gbc.gridx++;
        gbc.insets = new Insets(0, 0, 0, 10);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(Box.createHorizontalGlue(), gbc);

        return panel;
    }

    private JPanel createUserPassAuthPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(0, 5, 0, 0);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 2;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(m_authType.getButton(AuthType.USER_PASS.toString()), gbc);

        gbc.gridx++;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 0, 0, 10);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(Box.createHorizontalGlue(), gbc);


        gbc.insets = new Insets(0, 25, 0, 0);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.anchor = GridBagConstraints.EAST;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(new JLabel("Username:"), gbc);

        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.gridx++;
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(m_user.getComponentPanel(), gbc);

        gbc.insets = new Insets(0, 0, 0, 10);
        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(Box.createHorizontalGlue(), gbc);

        gbc.insets = new Insets(0, 25, 0, 0);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.anchor = GridBagConstraints.EAST;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(new JLabel("Password:"), gbc);

        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.gridx++;
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(m_pass.getComponentPanel(), gbc);

        gbc.insets = new Insets(0, 0, 0, 10);
        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(Box.createHorizontalGlue(), gbc);

        return panel;
    }

    private void updateEnabledness() {
        m_settings.getCredentialsSettingsModel().setEnabled(m_settings.useCredentials());
        m_settings.getUserSettingsModel().setEnabled(m_settings.useUserPass());
        m_settings.getPassSettingsModel().setEnabled(m_settings.useUserPass());
    }

    void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        m_authType.loadSettingsFrom(settings, specs);
        m_credential.loadSettingsFrom(settings, specs);
        m_user.loadSettingsFrom(settings, specs);
        m_pass.loadSettingsFrom(settings, specs);
        updateEnabledness();
    }

    void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_authType.saveSettingsTo(settings);
        m_credential.saveSettingsTo(settings);
        m_user.saveSettingsTo(settings);
        m_pass.saveSettingsTo(settings);
    }

    void onOpen() {
        m_settings.getAuthTypeSettingsModel().addChangeListener(this);
    }

    void onClose() {
        m_settings.getAuthTypeSettingsModel().removeChangeListener(this);
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        updateEnabledness();
    }
}
