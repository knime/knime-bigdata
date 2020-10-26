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

import java.security.InvalidParameterException;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelPassword;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * Authentication settings for {@link KnoxHdfsConnectorNodeModel}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHdfsAuthenticationSettings {

    private static final String DEFAULT_AUTH_TYPE = AuthType.USER_PASS.toString();

    private static final String DEFAULT_USER = System.getProperty("user.name");

    private static final String SECRET_KEY = "A8dTm7j?uNLagmmoOn7d%tJ6MSo_vSj9";

    private final SettingsModelString m_credentials = new SettingsModelString("credentials", "");

    private final SettingsModelString m_authType = new SettingsModelString("auth", DEFAULT_AUTH_TYPE);

    private final SettingsModelString m_user = new SettingsModelString("user", DEFAULT_USER);

    private final SettingsModelPassword m_pass = new SettingsModelPassword("pass", SECRET_KEY, "");

    /**
     * Authentication type enumeration.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public enum AuthType implements ButtonGroupEnumInterface {
        /**
         * Username and password authentication.
         */
        USER_PASS("Username/Password", "Authentication using a username and password."),

        /**
         * Authentication using a workflow credentials.
         */
        CREDENTIALS("Credentials", "Authentication using a workflow credentials.");

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
            return equals(USER_PASS);
        }
    }

    /**
     * Default constructor.
     */
    public KnoxHdfsAuthenticationSettings() {
    }

    /**
     * Integration tests constructor using username and password.
     *
     * @param user username to use
     * @param pass password to use
     */
    public KnoxHdfsAuthenticationSettings(final String user, final String pass) {
        m_authType.setStringValue(AuthType.USER_PASS.toString());
        m_user.setStringValue(user);
        m_pass.setStringValue(pass);
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_authType.saveSettingsTo(settings);
        m_credentials.saveSettingsTo(settings);
        m_user.saveSettingsTo(settings);
        m_pass.saveSettingsTo(settings);
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_authType.loadSettingsFrom(settings);
        m_credentials.loadSettingsFrom(settings);
        m_user.loadSettingsFrom(settings);
        m_pass.loadSettingsFrom(settings);
    }

    /**
     * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_authType.validateSettings(settings);
        m_credentials.validateSettings(settings);
        m_user.validateSettings(settings);
        m_pass.validateSettings(settings);
    }

    /**
     * Validate the values for all the {@link SettingsModel}s
     *
     * @throws InvalidSettingsException When a setting is set inappropriately.
     */
    public void validateValues() throws InvalidSettingsException {
        if (useCredentials() && StringUtils.isBlank(getCredentialsName())) {
            throw new InvalidSettingsException(
                "Credentials name required. Select another authentication method or select a name.");
        } else if (useUserPass() && StringUtils.isBlank(m_user.getStringValue())) { // NOSONAR
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
     * @return {@code true} if username and password authentication should be used
     */
    public boolean useCredentials() {
        return getAuthType() == AuthType.CREDENTIALS;
    }

    /**
     * @return {@code true} if username and password authentication should be used
     */
    public boolean useUserPass() {
        return getAuthType() == AuthType.USER_PASS;
    }

    /**
     * @return credentials name, might be empty depending on the authentication method
     */
    String getCredentialsName() {
        return m_credentials.getStringValue();
    }

    /**
     * @return credentials settings model
     */
    SettingsModelString getCredentialsSettingsModel() {
        return m_credentials;
    }

    /**
     * @param cp credentials provider to use, might be {@code null} if not required
     * @return user, might be empty depending on the authentication method
     */
    public String getUser(final CredentialsProvider cp) {
        if (useCredentials() && cp == null) {
            throw new InvalidParameterException("Credentials provider required.");
        } else if (useCredentials()) {
            return cp.get(getCredentialsName()).getLogin();
        } else {
            return m_user.getStringValue();
        }
    }

    /**
     * @return user settings model
     */
    SettingsModelString getUserSettingsModel() {
        return m_user;
    }

    /**
     * @param cp credentials provider to use, might be {@code null} if not required
     * @return password, might be empty depending on the authentication method
     */
    public String getPass(final CredentialsProvider cp) {
        if (useCredentials() && cp == null) {
            throw new InvalidParameterException("Credentials provider required.");
        } else if (useCredentials()) {
            return cp.get(getCredentialsName()).getPassword();
        } else {
            return m_pass.getStringValue();
        }
    }

    /**
     * @return password settings model
     */
    SettingsModelPassword getPassSettingsModel() {
        return m_pass;
    }
}
