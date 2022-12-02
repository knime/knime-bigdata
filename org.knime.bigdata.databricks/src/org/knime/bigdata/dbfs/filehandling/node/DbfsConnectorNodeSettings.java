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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings.AuthType;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnectionConfig;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig.BrowserRelativizationBehavior;

/**
 * Settings for the DBFS Connector node.
 *
 * @author Alexander Bondaletov
 */
public class DbfsConnectorNodeSettings {

    private static final String KEY_AUTH = "auth";

    private static final String KEY_URL = "url";

    private static final String KEY_WORKING_DIRECTORY = "workingDirectory";
    private static final String KEY_CONNECTION_TIMEOUT = "connectionTimeout";
    private static final String KEY_READ_TIMEOUT = "readTimeout";

    private static final String KEY_BROWSER_PATH_RELATIVE = "browserPathRelativize";

    private static final int DEFAULT_TIMEOUT = 30;

    private final SettingsModelString m_url;

    private final SettingsModelString m_workingDirectory;
    private final SettingsModelIntegerBounded m_connectionTimeout;
    private final SettingsModelIntegerBounded m_readTimeout;

    private final SettingsModelBoolean m_browserPathRelative;

    private final DbfsAuthenticationNodeSettings m_authSettings;

    /**
     * Creates new instance.
     */
    public DbfsConnectorNodeSettings() {
        m_url = new SettingsModelString(KEY_URL, "https://");
        m_workingDirectory = new SettingsModelString(KEY_WORKING_DIRECTORY, DbfsFileSystem.PATH_SEPARATOR);
        m_connectionTimeout =
            new SettingsModelIntegerBounded(KEY_CONNECTION_TIMEOUT, DEFAULT_TIMEOUT, 0, Integer.MAX_VALUE);
        m_readTimeout = new SettingsModelIntegerBounded(KEY_READ_TIMEOUT, DEFAULT_TIMEOUT, 0, Integer.MAX_VALUE);
        m_browserPathRelative = new SettingsModelBoolean(KEY_BROWSER_PATH_RELATIVE, false);
        m_authSettings = new DbfsAuthenticationNodeSettings(KEY_AUTH, AuthType.TOKEN);
    }

    /**
     * @return authentication settings.
     */
    public DbfsAuthenticationNodeSettings getAuthenticationSettings() {
        return m_authSettings;
    }

    /**
     * @return the URL model
     */
    public SettingsModelString getUrlModel() {
        return m_url;
    }

    /**
     * @return the host part of the deployment URL
     */
    public String getHost() {
        try {
            return new URI(getDeploymentUrl()).getHost();
        } catch (final URISyntaxException e) {
            return null;
        }
    }

    /**
     * @return The deployment URL consists of host and port from the settings.
     */
    public String getDeploymentUrl() {
        return m_url.getStringValue();
    }

    /**
     * @return the workingDirectory model
     */
    public SettingsModelString getWorkingDirectoryModel() {
        return m_workingDirectory;
    }

    /**
     * @return the working directory
     */
    public String getWorkingDirectory() {
        return m_workingDirectory.getStringValue();
    }

    /**
     * @return the connectionTimeout model
     */
    public SettingsModelIntegerBounded getConnectionTimeoutModel() {
        return m_connectionTimeout;
    }

    /**
     * @return the connection timeout
     */
    public Duration getConnectionTimeout() {
        return Duration.ofSeconds(m_connectionTimeout.getIntValue());
    }

    /**
     * @return the readTimeout model
     */
    public SettingsModelIntegerBounded getReadTimeoutModel() {
        return m_readTimeout;
    }

    /**
     * @return the read timeout
     */
    public Duration getReadTimeout() {
        return Duration.ofSeconds(m_readTimeout.getIntValue());
    }

    /**
     * @return the browserPathRelative model
     */
    public SettingsModelBoolean getBrowserPathRelativeModel() {
        return m_browserPathRelative;
    }

    /**
     * @return the browser relativization behavior
     */
    public BrowserRelativizationBehavior getBrowserRelativizationBehavior() {
        if (m_browserPathRelative.getBooleanValue()) {
            return BrowserRelativizationBehavior.RELATIVE;
        } else {
            return BrowserRelativizationBehavior.ABSOLUTE;
        }
    }

    private void save(final NodeSettingsWO settings) {
        m_url.saveSettingsTo(settings);
        m_workingDirectory.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
        m_readTimeout.saveSettingsTo(settings);
        m_browserPathRelative.saveSettingsTo(settings);
    }

    /**
     * Saves settings to the given {@link NodeSettingsWO} (to be called by the node dialog).
     *
     * @param settings
     */
    public void saveSettingsForDialog(final NodeSettingsWO settings) {
        save(settings);
        // auth settings are saved by dialog
    }

    /**
     * Saves settings to the given {@link NodeSettingsWO} (to be called by the node model).
     *
     * @param settings
     */
    public void saveSettingsForModel(final NodeSettingsWO settings) {
        save(settings);
        m_authSettings.saveSettingsForModel(settings);
    }

    /**
     * Validates settings in the given {@link NodeSettingsRO}.
     *
     * @param settings
     *            The settings.
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.validateSettings(settings);
        m_workingDirectory.validateSettings(settings);
        m_connectionTimeout.validateSettings(settings);
        m_readTimeout.validateSettings(settings);

        if (settings.containsKey(KEY_BROWSER_PATH_RELATIVE)) {
            m_browserPathRelative.validateSettings(settings);
        }

        m_authSettings.validateSettings(settings);

        DbfsConnectorNodeSettings temp = new DbfsConnectorNodeSettings();
        temp.loadSettingsForModel(settings);
        temp.validate();
    }

    /**
     * Validates settings consistency for this instance.
     *
     * @throws InvalidSettingsException
     */
    public void validate() throws InvalidSettingsException {
        if (StringUtils.isBlank(getDeploymentUrl())) {
            throw new InvalidSettingsException("The Databricks deployment URL must not be empty.");
        } else {
            try {
                final URL uri = new URL(getDeploymentUrl());

                if (StringUtils.isBlank(uri.getProtocol()) || !uri.getProtocol().equalsIgnoreCase("https")) {
                    throw new InvalidSettingsException(
                        "HTTPS Protocol in Databricks deployment URL required (only https supported)");
                } else if (StringUtils.isBlank(uri.getHost())) {
                    throw new InvalidSettingsException("Hostname in Databricks deployment URL required.");
                }

            } catch (MalformedURLException e) {
                throw new InvalidSettingsException(
                    String.format("Invalid Databricks deployment URL: %s", e.getMessage()));
            }
        }

        if (!m_workingDirectory.getStringValue().startsWith(DbfsFileSystem.PATH_SEPARATOR)) {
            throw new InvalidSettingsException("Working directory must be an absolute path.");
        }

        m_authSettings.validate();
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO}.
     *
     * @param settings
     *            The settings.
     * @throws InvalidSettingsException
     */
    public void load(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.loadSettingsFrom(settings);
        m_workingDirectory.loadSettingsFrom(settings);
        m_connectionTimeout.loadSettingsFrom(settings);
        m_readTimeout.loadSettingsFrom(settings);

        if (settings.containsKey(KEY_BROWSER_PATH_RELATIVE)) {
            m_browserPathRelative.loadSettingsFrom(settings);
        } else {
            m_browserPathRelative.setBooleanValue(false);
        }
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the node dialog).
     *
     * @param settings
     * @throws InvalidSettingsException
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) throws InvalidSettingsException {
        load(settings);
        // auth settings are loaded by dialog
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the node model).
     *
     * @param settings
     * @throws InvalidSettingsException
     */
    public void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        load(settings);
        m_authSettings.loadSettingsForModel(settings);
    }

    /**
     * @return a (deep) clone of this node settings object.
     */
    public DbfsConnectorNodeSettings createClone() {
        final NodeSettings tempSettings = new NodeSettings("ignored");
        saveSettingsForModel(tempSettings);

        final DbfsConnectorNodeSettings toReturn = new DbfsConnectorNodeSettings();
        try {
            toReturn.loadSettingsForModel(tempSettings);
        } catch (InvalidSettingsException ex) { // NOSONAR can never happen
            // won't happen
        }
        return toReturn;
    }

    /**
     * Convert this settings to a {@link DbfsFSConnectionConfig} instance.
     *
     * @param credentialsProvider provider of credential variables
     * @return a {@link DbfsFSConnectionConfig} using this settings
     */
    public DbfsFSConnectionConfig toFSConnectionConfig(final CredentialsProvider credentialsProvider) {
        final DbfsFSConnectionConfig.Builder builder = DbfsFSConnectionConfig.builder() //
            .withDeploymentUrl(getDeploymentUrl()) //
            .withWorkingDirectory(getWorkingDirectory()) //
            .withConnectionTimeout(getConnectionTimeout()) //
            .withReadTimeout(getReadTimeout()) //
            .withRelativizationBehavior(getBrowserRelativizationBehavior());

        if (m_authSettings.getAuthType() == AuthType.TOKEN) {
            final String token = m_authSettings.useTokenCredentials() //
                ? getCredentials(m_authSettings.getTokenCredentialsName(), credentialsProvider).getPassword()
                : m_authSettings.getTokenModel().getStringValue();
            builder.withToken(token);
        } else {
            if (m_authSettings.useUserPassCredentials()) {
                final ICredentials creds =
                    getCredentials(m_authSettings.getUserPassCredentialsName(), credentialsProvider);
                builder.withUserAndPassword(creds.getLogin(), creds.getPassword());
            } else {
                builder.withUserAndPassword(m_authSettings.getUserModel().getStringValue(),
                    m_authSettings.getPasswordModel().getStringValue());
            }
        }

        return builder.build();
    }

    private static ICredentials getCredentials(final String name, final CredentialsProvider credProvider) {
        if (credProvider == null) {
            throw new IllegalStateException("Credential provider is not available");
        }

        return credProvider.get(name);
    }

}
