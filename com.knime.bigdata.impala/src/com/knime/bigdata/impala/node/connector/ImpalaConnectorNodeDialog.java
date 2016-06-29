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
 *   Created on 06.05.2014 by thor
 */
package com.knime.bigdata.impala.node.connector;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.node.io.database.connection.util.DBAuthenticationPanel;
import org.knime.base.node.io.database.connection.util.DBConnectionPanel;
import org.knime.base.node.io.database.connection.util.DBMiscPanel;
import org.knime.base.node.io.database.connection.util.DBTimezonePanel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.util.StringHistoryPanel;

/**
 * Dialog for the Impala Connector node.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
class ImpalaConnectorNodeDialog extends NodeDialogPane {
    private class ImpalaConnectionPanel extends DBConnectionPanel<ImpalaConnectorSettings> {
        private static final long serialVersionUID = 1L;

        ImpalaConnectionPanel(final ImpalaConnectorSettings settings) {
            super(settings, ImpalaConnectorNodeDialog.class.getName());
            m_c.gridx = 0;
            m_c.gridy++;
            add(new JLabel("Parameter "), m_c);

            m_c.gridy++;
            m_c.fill = GridBagConstraints.HORIZONTAL;
            m_c.weightx = 1;
            add(m_parameter, m_c);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected String getJDBCURL(final String host, final int port, final String dbName) {
            return ImpalaConnectorNodeModel.getJDBCURL(m_settings);
        }
    }

    private final ImpalaConnectorSettings m_settings = new ImpalaConnectorSettings();

    private final StringHistoryPanel m_parameter = new StringHistoryPanel(getClass().getName() + "_parameter    ");

    private final ImpalaConnectionPanel m_connectionPanel = new ImpalaConnectionPanel(m_settings);

    private final DBAuthenticationPanel<DatabaseConnectionSettings> m_authPanel =
        new DBAuthenticationPanel<>(m_settings);

    private final DBTimezonePanel<DatabaseConnectionSettings> m_tzPanel =
        new DBTimezonePanel<>(m_settings);

    private final DBMiscPanel<DatabaseConnectionSettings> m_miscPanel = new DBMiscPanel<>(
        m_settings, false);

    ImpalaConnectorNodeDialog() {
        JPanel p = new JPanel(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.gridy = 0;
        c.weightx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(0, 0, 4, 0);

        p.add(m_connectionPanel, c);
        c.gridy++;
        p.add(m_authPanel, c);
        c.gridy++;
        p.add(m_tzPanel, c);
        c.gridy++;
        c.insets = new Insets(0, 0, 0, 0);
        p.add(m_miscPanel, c);

        addTab("Connection settings", p);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        try {
            m_settings.loadValidatedConnection(settings, getCredentialsProvider());
        } catch (InvalidSettingsException ex) {
            // too bad, use default values
        }

        m_connectionPanel.loadSettings(specs);
        m_authPanel.loadSettings(specs, getCredentialsProvider());
        m_tzPanel.loadSettings(specs);
        m_miscPanel.loadSettings(specs);
        m_parameter.setSelectedString(m_settings.getParameter());
        m_parameter.commitSelectedToHistory();
        m_parameter.updateHistory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_connectionPanel.saveSettings();
        m_authPanel.saveSettings();
        m_tzPanel.saveSettings();
        m_miscPanel.saveSettings(getCredentialsProvider());
        m_settings.setParameter(m_parameter.getSelectedString());
        m_parameter.commitSelectedToHistory();
        m_settings.saveConnection(settings);
    }
}
