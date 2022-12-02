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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnectionConfig;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.filehandling.core.connections.base.auth.AuthSettings;
import org.knime.filehandling.core.connections.base.auth.StandardAuthTypes;
import org.knime.filehandling.core.connections.base.auth.UserPasswordAuthProviderSettings;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig.BrowserRelativizationBehavior;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;

/**
 * Settings for KNOX Connector node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class KnoxHdfsConnectorNodeSettings {

    private static final String KEY_BROWSER_PATH_RELATIVE = "browserPathRelativize";

    private static final String DEFAULT_URL = "https://server:8443/gateway/default";

    private static final String DEFAULT_WORKING_DIR = KnoxHdfsFileSystem.PATH_SEPARATOR;

    private static final int DEFAULT_TIMEOUT = 30;

    private final SettingsModelString m_url = new SettingsModelString("url", DEFAULT_URL);

    private final AuthSettings m_authSettings;

    private final SettingsModelString m_workingDirectory =
        new SettingsModelString("workingDirectory", DEFAULT_WORKING_DIR);

    /** Connection timeout in seconds */
    private final SettingsModelIntegerBounded m_connectionTimeout =
        new SettingsModelIntegerBounded("connectionTimeout", DEFAULT_TIMEOUT, 0, Integer.MAX_VALUE);

    /** Receive timeout in seconds */
    private final SettingsModelIntegerBounded m_receiveTimeout =
        new SettingsModelIntegerBounded("receiveTimeout", DEFAULT_TIMEOUT, 0, Integer.MAX_VALUE);

    private final SettingsModelBoolean m_browserPathRelative;

    /**
     * Default constructor.
     */
    public KnoxHdfsConnectorNodeSettings() {
        m_authSettings = new AuthSettings.Builder() //
            .add(new UserPasswordAuthProviderSettings(StandardAuthTypes.USER_PASSWORD, true)) //
            .build();

        m_browserPathRelative = new SettingsModelBoolean(KEY_BROWSER_PATH_RELATIVE, false);
    }

    /**
     * Clone a given settings model
     */
    KnoxHdfsConnectorNodeSettings(final KnoxHdfsConnectorNodeSettings other) {
        this();

        try {
            final NodeSettings transferSettings = new NodeSettings("ignored");
            other.saveSettingsForModel(transferSettings);
            loadSettingsForModel(transferSettings); // NOSONAR
        } catch (InvalidSettingsException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Integration tests constructor.
     *
     * @param url KNOX endpoint to use
     * @param user username to use
     * @param pass password to use
     * @param workingDir current working directory
     * @param connectionTimeout connection timeout in seconds
     * @param receiveTimeout receive timeout in seconds
     */
    public KnoxHdfsConnectorNodeSettings(final String url, final String user, final String pass,
        final String workingDir, final int connectionTimeout, final int receiveTimeout) {

        this();

        m_url.setStringValue(url);

        final UserPasswordAuthProviderSettings userPwdSettings = m_authSettings.getSettingsForAuthType(StandardAuthTypes.USER_PASSWORD);
        userPwdSettings.getUserModel().setStringValue(user);
        userPwdSettings.getPasswordModel().setStringValue(pass);

        m_workingDirectory.setStringValue(workingDir);
        m_connectionTimeout.setIntValue(connectionTimeout);
        m_receiveTimeout.setIntValue(receiveTimeout);
    }

    private void save(final NodeSettingsWO settings) {
        m_url.saveSettingsTo(settings);
        m_workingDirectory.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
        m_receiveTimeout.saveSettingsTo(settings);
        m_browserPathRelative.saveSettingsTo(settings);
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     */
    void saveSettingsForModel(final NodeSettingsWO settings) {
        save(settings);
        m_authSettings.saveSettingsForModel(settings.addNodeSettings(AuthSettings.KEY_AUTH));
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     */
    void saveSettingsForDialog(final NodeSettingsWO settings) {
        save(settings);
        // auth settings are saved by dialog
    }

    private void load(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.loadSettingsFrom(settings);
        m_workingDirectory.loadSettingsFrom(settings);
        m_connectionTimeout.loadSettingsFrom(settings);
        m_receiveTimeout.loadSettingsFrom(settings);

        if (settings.containsKey(KEY_BROWSER_PATH_RELATIVE)) {
            m_browserPathRelative.loadSettingsFrom(settings);
        } else {
            m_browserPathRelative.setBooleanValue(false);
        }
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        load(settings);
        m_authSettings.loadSettingsForModel(settings.getNodeSettings(AuthSettings.KEY_AUTH));
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void loadSettingsForDialog(final NodeSettingsRO settings) throws InvalidSettingsException {
        load(settings);
        // auth settings are loaded by dialog
    }

    /**
     * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.validateSettings(settings);
        m_workingDirectory.validateSettings(settings);
        m_authSettings.validateSettings(settings.getNodeSettings(AuthSettings.KEY_AUTH));
        m_connectionTimeout.validateSettings(settings);
        m_receiveTimeout.validateSettings(settings);

        if (settings.containsKey(KEY_BROWSER_PATH_RELATIVE)) {
            m_browserPathRelative.validateSettings(settings);
        }
    }

    /**
     * Validate the values for all the {@link SettingsModel}s
     *
     * @throws InvalidSettingsException When a setting is set inappropriately.
     */
    public void validateValues() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_url.getStringValue())) {
            throw new InvalidSettingsException("URL required.");
        }

        try {
            final URI uri = new URI(m_url.getStringValue());

           if (StringUtils.isBlank(uri.getScheme())) {
                throw new InvalidSettingsException("Empty scheme in URL " +  uri.toString());
            } else if (StringUtils.isBlank(uri.getHost())) {
                throw new InvalidSettingsException("Empty host in URL " +  uri.toString());
            } else if (StringUtils.isBlank(uri.getPath())) { // NOSONAR no else required
                throw new InvalidSettingsException("Empty path in URL " +  uri.toString());
            }

        } catch (final URISyntaxException e) {
            throw new InvalidSettingsException("Failed to parse URL: " + e.getMessage(), e);
        }

        m_authSettings.validate();

        if (StringUtils.isBlank(m_workingDirectory.getStringValue())) {
            throw new InvalidSettingsException("Working directory required.");
        }
    }

    void configureInModel(final PortObjectSpec[] inSpecs, final Consumer<StatusMessage> statusConsumer,
        final CredentialsProvider credentialsProvider) throws InvalidSettingsException {
        m_authSettings.configureInModel(inSpecs, statusConsumer, credentialsProvider);
    }

    /**
     * @return KNOX endpoint URL
     */
    public String getURL() {
        return m_url.getStringValue();
    }

    /**
     *
     * @return Host part of the KNOX endpoint URL
     */
    public String getHost() {
        return URI.create(getURL()).getHost();
    }

    /**
     * @return URL settings model
     */
    SettingsModelString getURLSettingsModel() {
        return m_url;
    }

    /**
     * @param cp credentials provider to use, might be {@code null} if not required
     * @return user name
     */
    public String getUser(final CredentialsProvider cp) {
        return m_authSettings.<UserPasswordAuthProviderSettings>getSettingsForAuthType(StandardAuthTypes.USER_PASSWORD) //
                .getUser(cp::get);
    }

    /**
     * @param cp credentials provider to use, might be {@code null} if not required
     * @return password
     */
    public String getPassword(final CredentialsProvider cp) {
        return m_authSettings.<UserPasswordAuthProviderSettings>getSettingsForAuthType(StandardAuthTypes.USER_PASSWORD) //
                .getPassword(cp::get);
    }

    /**
     * @return authentication settings model
     */
    AuthSettings getAuthSettings() {
        return m_authSettings;
    }

    /**
     * @return working directory
     */
    public String getWorkingDirectory() {
        return m_workingDirectory.getStringValue();
    }

    /**
     * @return working directory settings model
     */
    SettingsModelString getWorkingDirectorySettingsModel() {
        return m_workingDirectory;
    }

    /**
     * @return connection timeout in seconds
     */
    public Duration getConnectionTimeout() {
        return Duration.ofSeconds(m_connectionTimeout.getIntValue());
    }

    /**
     * @return connection timeout settings model
     */
    SettingsModelIntegerBounded getConnectionTimeoutSettingsModel() {
        return m_connectionTimeout;
    }

    /**
     * @return receive timeout
     */
    public Duration getReceiveTimeout() {
        return Duration.ofSeconds(m_receiveTimeout.getIntValue());
    }

    /**
     * @return receive timeout settings model
     */
    SettingsModelIntegerBounded getReceiveTimeoutSettingsModel() {
        return m_receiveTimeout;
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

    /**
     * Convert this settings to a {@link KnoxHdfsFSConnectionConfig} instance.
     *
     * @param credentialsProvider provider of credential variables
     * @return a {@link KnoxHdfsFSConnectionConfig} using this settings
     */
    public KnoxHdfsFSConnectionConfig toFSConnectionConfig(final CredentialsProvider credentialsProvider) {
        return KnoxHdfsFSConnectionConfig.builder() //
            .withEndpointUrl(getURL()) //
            .withWorkingDirectory(getWorkingDirectory()) //
            .withUserAndPassword(getUser(credentialsProvider), getPassword(credentialsProvider)) //
            .withConnectionTimeout(getConnectionTimeout()) //
            .withReceiveTimeout(getReceiveTimeout()) //
            .withRelativizationBehavior(getBrowserRelativizationBehavior()) //
            .build();
    }
}
