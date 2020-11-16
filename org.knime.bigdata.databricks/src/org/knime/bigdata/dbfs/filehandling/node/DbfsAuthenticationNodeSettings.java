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
 *   2020-10-15 (Vyacheslav Soldatov): created
 */
package org.knime.bigdata.dbfs.filehandling.node;

import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelPassword;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;

/**
 * Authentication settings for {@link DbfsConnectorNodeModel}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DbfsAuthenticationNodeSettings {

    private static final String KEY_AUTH_TYPE = "authType";

    private static final String KEY_USE_CREDENTIALS = "useCredentials";

    private static final String KEY_CREDENTIALS = "credentialsName";

    private static final String KEY_USER_PASSWORD = "user_pwd";

    private static final String KEY_USER = "user";

    private static final String KEY_PASSWORD = "password";

    private static final String KEY_TOKEN = "token";

    private static final String SECRET_KEY = "ekerjvjhmzle,ptktysq";

    private AuthType m_authType;

    private final SettingsModelBoolean m_userPassUseCredentials;

    private final SettingsModelString m_userPassCredentialsName;

    private final SettingsModelString m_user;

    private final SettingsModelPassword m_password;

    private final SettingsModelBoolean m_tokenUseCredentials;

    private final SettingsModelString m_tokenCredentialsName;

    private final SettingsModelPassword m_token;

    /**
     * Authentication type enumeration.
     *
     * @author Vyacheslav Soldatov <vyacheslav@redfield.se>
     */
    public enum AuthType implements ButtonGroupEnumInterface {
            /**
             * User name and password authentication.
             */
            USER_PWD("USER_PWD", "Username/password", "Authenticate with username and password"),
            /**
             * Token authentication.
             */
            TOKEN("TOKEN", "Token", "Authenticate with a token");

        private final String m_settingsValue;

        private final String m_toolTip;

        private final String m_text;

        private AuthType(final String settingsValue, final String text, final String toolTip) {
            m_settingsValue = settingsValue;
            m_text = text;
            m_toolTip = toolTip;
        }

        /**
         * @return the node settings value for this authentication type
         */
        public String getSettingsValue() {
            return m_settingsValue;
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
            return equals(USER_PWD);
        }

        /**
         * @param actionCommand the action command
         * @return the {@link AuthenticationType} for the action command
         */
        public static AuthType get(final String actionCommand) {
            return valueOf(actionCommand);
        }

        /**
         * Maps a settings value string to an {@link AuthType} enum value.
         *
         * @param settingsValue The node settings value of a {@link AuthType} enum value.
         * @return the {@link AuthType} enum value corresponding to the given node settings value.
         * @throws IllegalArgumentException if there is no {@link AuthType} enum value corresponding to the given node
         *             settings value.
         */
        public static AuthType fromSettingsValue(final String settingsValue) {
            for (AuthType type : values()) {
                if (type.getSettingsValue().equals(settingsValue)) {
                    return type;
                }
            }

            throw new IllegalArgumentException("Invalid authentication type " + settingsValue);
        }
    }

    /**
     * Default constructor.
     */
    public DbfsAuthenticationNodeSettings() {
        //authentication
        m_authType = AuthType.USER_PWD;

        m_userPassUseCredentials = new SettingsModelBoolean(KEY_USE_CREDENTIALS, false);
        m_userPassCredentialsName = new SettingsModelString(KEY_CREDENTIALS, "");
        m_user = new SettingsModelString(KEY_USER, System.getProperty("user.name"));
        m_password = new SettingsModelPassword(KEY_PASSWORD, SECRET_KEY, "");

        m_tokenUseCredentials = new SettingsModelBoolean(KEY_USE_CREDENTIALS, false);
        m_tokenCredentialsName = new SettingsModelString(KEY_CREDENTIALS, "");
        m_token = new SettingsModelPassword(KEY_TOKEN, SECRET_KEY, "");

        m_userPassUseCredentials.addChangeListener(e -> updateEnabledness());
        m_tokenUseCredentials.addChangeListener(e -> updateEnabledness());

        updateEnabledness();
    }

    /**
     * @return authentication type.
     */
    public AuthType getAuthType() {
        return m_authType;
    }

    /**
     * @param authType the authType to set
     */
    public void setAuthType(final AuthType authType) {
        m_authType = authType;
        updateEnabledness();
    }

    private void updateEnabledness() {
        m_userPassUseCredentials.setEnabled(m_authType == AuthType.USER_PWD);
        m_userPassCredentialsName
            .setEnabled(m_authType == AuthType.USER_PWD && useUserPassCredentials());
        m_user.setEnabled(m_authType == AuthType.USER_PWD && !useUserPassCredentials());
        m_password.setEnabled(m_authType == AuthType.USER_PWD && !useUserPassCredentials());

        m_tokenUseCredentials.setEnabled(m_authType == AuthType.TOKEN);
        m_tokenCredentialsName
            .setEnabled(m_authType == AuthType.TOKEN && useTokenCredentials());
        m_token.setEnabled(m_authType == AuthType.TOKEN && !useTokenCredentials());
    }

    /**
     * @return password settings model.
     */
    public SettingsModelPassword getPasswordModel() {
        return m_password;
    }

    /**
     * @return user name model.
     */
    public SettingsModelString getUserModel() {
        return m_user;
    }

    /**
     * @return settings model for whether or not to use a credentials flow variable for username/password
     *         authentication.
     */
    public SettingsModelBoolean getUserPassUseCredentialsModel() {
        return m_userPassUseCredentials;
    }

    /**
     * @return whether or not to use a credentials flow variable for username/password authentication.
     */
    public boolean useUserPassCredentials() {
        return m_userPassUseCredentials.getBooleanValue();
    }

    /**
     * @return settings model for the name of the credentials flow variable for username/password authentication.
     */
    public SettingsModelString getUserPassCredentialsNameModel() {
        return m_userPassCredentialsName;
    }

    /**
     * @return the name of the credentials flow variable for username/password authentication (or null, if not set).
     */
    public String getUserPassCredentialsName() {
        String creds = m_userPassCredentialsName.getStringValue();
        return StringUtils.isBlank(creds) ? null : creds;
    }

    /**
     * @return settings model for whether or not to use a credentials flow variable for token authentication.
     */
    public SettingsModelBoolean getTokenUseCredentialsModel() {
        return m_tokenUseCredentials;
    }

    /**
     * @return whether or not to use a credentials flow variable for token authentication.
     */
    public boolean useTokenCredentials() {
        return m_tokenUseCredentials.getBooleanValue();
    }

    /**
     * @return settings model for the name of the credentials flow variable for token authentication.
     */
    public SettingsModelString getTokenCredentialsNameModel() {
        return m_tokenCredentialsName;
    }

    /**
     * @return the name of the credentials flow variable for token authentication (or null, if not set).
     */
    public String getTokenCredentialsName() {
        String creds = m_tokenCredentialsName.getStringValue();
        return StringUtils.isBlank(creds) ? null : creds;
    }

    /**
     * @return token model.
     */
    public SettingsModelPassword getTokenModel() {
        return m_token;
    }

    /**
     * @return a (deep) clone of this node settings object.
     */
    public DbfsAuthenticationNodeSettings createClone() {
        final NodeSettings tempSettings = new NodeSettings("ignored");
        saveSettingsForModel(tempSettings);

        final DbfsAuthenticationNodeSettings toReturn = new DbfsAuthenticationNodeSettings();
        try {
            toReturn.loadSettingsForModel(tempSettings);
        } catch (InvalidSettingsException ex) { // NOSONAR can never happen
            // won't happen
        }
        return toReturn;
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the dialog).
     *
     * @param settings
     * @throws InvalidSettingsException
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) throws InvalidSettingsException {
        load(settings);
        // m_keyFile must be loaded by dialog component
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the node model).
     *
     * @param settings
     * @throws InvalidSettingsException
     */
    public void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        load(settings);
    }

    private void load(final NodeSettingsRO settings) throws InvalidSettingsException {
        try {
            m_authType = AuthType.fromSettingsValue(settings.getString(KEY_AUTH_TYPE));
        } catch (IllegalArgumentException e) {
            throw new InvalidSettingsException(
                settings.getString(KEY_AUTH_TYPE) + " is not a valid authentication type", e);
        }

        final NodeSettingsRO userPassSettings = settings.getNodeSettings(KEY_USER_PASSWORD);
        m_userPassUseCredentials.loadSettingsFrom(userPassSettings);
        m_userPassCredentialsName.loadSettingsFrom(userPassSettings);
        m_user.loadSettingsFrom(userPassSettings);
        m_password.loadSettingsFrom(userPassSettings);

        final NodeSettingsRO tokenSettings = settings.getNodeSettings(KEY_TOKEN);
        m_tokenUseCredentials.loadSettingsFrom(tokenSettings);
        m_tokenCredentialsName.loadSettingsFrom(tokenSettings);
        m_token.loadSettingsFrom(tokenSettings);

        updateEnabledness();
    }

    /**
     * Validates the settings in the given {@link NodeSettingsRO}.
     *
     * @param settings
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final NodeSettingsRO userPassSettings = settings.getNodeSettings(KEY_USER_PASSWORD);
        m_userPassUseCredentials.validateSettings(userPassSettings);
        m_userPassCredentialsName.validateSettings(userPassSettings);
        m_user.validateSettings(userPassSettings);
        m_password.validateSettings(userPassSettings);

        final NodeSettingsRO tokenSettings = settings.getNodeSettings(KEY_TOKEN);
        m_tokenUseCredentials.validateSettings(tokenSettings);
        m_tokenCredentialsName.validateSettings(tokenSettings);
        m_token.validateSettings(tokenSettings);

        DbfsAuthenticationNodeSettings temp = new DbfsAuthenticationNodeSettings();
        temp.loadSettingsForModel(settings);
        temp.validate();
    }

    /**
     * Validates the current settings.
     *
     * @throws InvalidSettingsException
     */
    public void validate() throws InvalidSettingsException {
        switch (getAuthType()) {
            case USER_PWD:
                validateUserPasswordSettings();
                break;
            case TOKEN:
                validateTokenSettings();
                break;
            default:
                break;
        }
    }

    private void validateUserPasswordSettings() throws InvalidSettingsException {
        if (useUserPassCredentials()) {
            if (StringUtils.isBlank(getUserPassCredentialsName())) {
                throw new InvalidSettingsException(
                    "Please choose a credentials flow variable for username/password authentication.");
            }
        } else if (StringUtils.isBlank(m_user.getStringValue()) || StringUtils.isBlank(m_password.getStringValue())) {
            throw new InvalidSettingsException("Please provide a valid username and password.");
        }
    }

    private void validateTokenSettings() throws InvalidSettingsException {
        if (useTokenCredentials()) {
            if (StringUtils.isBlank(getTokenCredentialsName())) {
                throw new InvalidSettingsException(
                    "Please choose a credentials flow variable for token authentication.");
            }
        } else if (StringUtils.isBlank(m_token.getStringValue())) {
            throw new InvalidSettingsException("Please provide a valid token.");
        }
    }

    /**
     * Saves the settings (to be called by node dialog).
     *
     * @param settings
     */
    public void saveSettingsForDialog(final NodeSettingsWO settings) {
        save(settings);
    }

    /**
     * Saves the settings (to be called by node model).
     *
     * @param settings
     */
    public void saveSettingsForModel(final NodeSettingsWO settings) {
        save(settings);
    }

    /**
     * Saves settings to the given {@link NodeSettingsWO}.
     *
     * @param settings
     */
    private void save(final NodeSettingsWO settings) {
        settings.addString(KEY_AUTH_TYPE, m_authType.getSettingsValue());

        final NodeSettingsWO userPassSettings = settings.addNodeSettings(KEY_USER_PASSWORD);
        m_userPassUseCredentials.saveSettingsTo(userPassSettings);
        m_userPassCredentialsName.saveSettingsTo(userPassSettings);
        m_user.saveSettingsTo(userPassSettings);
        m_password.saveSettingsTo(userPassSettings);

        final NodeSettingsWO tokenSettings = settings.addNodeSettings(KEY_TOKEN);
        m_tokenUseCredentials.saveSettingsTo(tokenSettings);
        m_tokenCredentialsName.saveSettingsTo(tokenSettings);
        m_token.saveSettingsTo(tokenSettings);
    }

    @Override
    public String toString() {
        return getAuthType().name();
    }

    /**
     * Forwards the given {@link PortObjectSpec} and status message consumer to the file chooser settings models to they
     * can configure themselves properly.
     *
     * @param inSpecs input specifications.
     * @param statusConsumer status consumer.
     * @throws InvalidSettingsException
     */
    public void configureInModel(final PortObjectSpec[] inSpecs, final Consumer<StatusMessage> statusConsumer)
        throws InvalidSettingsException {
        // nothing for now
    }
}
