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

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * Authentication settings for {@link HdfsConnectorNodeModel}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsAuthenticationSettings {

    private static final String DEFAULT_AUTH_TYPE = AuthType.SIMPLE.toString();

    private static final String DEFAULT_USER = System.getProperty("user.name");

    private final SettingsModelString m_authType = new SettingsModelString("auth", DEFAULT_AUTH_TYPE);

    private final SettingsModelString m_user = new SettingsModelString("user", DEFAULT_USER);

    /**
     * Authentication type enumeration.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public enum AuthType implements ButtonGroupEnumInterface {
        /**
         * Username authentication.
         */
        SIMPLE("Username:", "Simple authentication with a provided username"),

        /**
         * Kerberos based authentication.
         */
        KERBEROS("Kerberos", "Kerberos based authentication");

        private final String m_toolTip;
        private final String m_text;

        private AuthType(final String text, final String toolTip) {
            m_text = text;
            m_toolTip = toolTip;
        }

        @Override
        public String getText() {
            return m_text;
        }

        @Override
        public String getActionCommand() {
            return name();
        }

        @Override
        public String getToolTip() {
            return m_toolTip;
        }

        @Override
        public boolean isDefault() {
            return equals(SIMPLE);
        }
    }

    /**
     * Default constructor.
     */
    public HdfsAuthenticationSettings() {
    }

    /**
     * Integration tests constructor.
     *
     * @param authType authentication type
     * @param user username to use
     */
    public HdfsAuthenticationSettings(final AuthType authType, final String user) {
        m_authType.setStringValue(authType.toString());
        m_user.setStringValue(user);
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_authType.saveSettingsTo(settings);
        m_user.saveSettingsTo(settings);
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_authType.loadSettingsFrom(settings);
        m_user.loadSettingsFrom(settings);
    }

    /**
     * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_authType.validateSettings(settings);
        m_user.validateSettings(settings);
    }

    /**
     * Validate the values for all the {@link SettingsModel}s
     *
     * @throws InvalidSettingsException When a setting is set inappropriately.
     */
    public void validateValues() throws InvalidSettingsException {
        if (useSimpleAuthentication() && StringUtils.isAllBlank(getUser())) {
            throw new InvalidSettingsException(
                "Username required. Select another authentication method or enter a username.");
        }
    }

    /**
     * @return authentication type settings model
     */
    SettingsModelString getAuthTypeSettingsModel() {
        return m_authType;
    }

    /**
     * @return authentication type
     */
    public AuthType getAuthType() {
        return AuthType.valueOf(m_authType.getStringValue());
    }

    /**
     * @return {@code true} if simple authentication should be used
     */
    public boolean useSimpleAuthentication() {
        return getAuthType() == AuthType.SIMPLE;
    }

    /**
     * @return {@code true} if kerberos authentication should be used
     */
    public boolean useKerberosAuthentication() {
        return getAuthType() == AuthType.KERBEROS;
    }

    /**
     * @return user name, might be empty depending on the authentication method
     */
    public String getUser() {
        return m_user.getStringValue();
    }

    /**
     * @return authentication settings model
     */
    SettingsModelString getUserSettingsModel() {
        return m_user;
    }
}
