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

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * Settings for {@link KnoxHdfsConnectorNodeModel}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHdfsConnectorNodeSettings {

    private static final String DEFAULT_URL = "https://server:8443/gateway/default";

    private static final String DEFAULT_WORKING_DIR = KnoxHdfsFileSystem.PATH_SEPARATOR;

    private static final int DEFAULT_TIMEOUT = 30;

    private final SettingsModelString m_url = new SettingsModelString("url", DEFAULT_URL);

    private final KnoxHdfsAuthenticationSettings m_auth;

    private final SettingsModelString m_workingDirectory =
        new SettingsModelString("workingDirectory", DEFAULT_WORKING_DIR);

    /** Connection timeout in seconds */
    private final SettingsModelIntegerBounded m_connectionTimeout =
        new SettingsModelIntegerBounded("connectionTimeout", DEFAULT_TIMEOUT, 1, Integer.MAX_VALUE);

    /** Receive timeout in seconds */
    private final SettingsModelIntegerBounded m_receiveTimeout =
        new SettingsModelIntegerBounded("receiveTimeout", DEFAULT_TIMEOUT, 1, Integer.MAX_VALUE);

    /**
     * Default constructor.
     */
    public KnoxHdfsConnectorNodeSettings() {
        m_auth = new KnoxHdfsAuthenticationSettings();
    }

    /**
     * Clone a given settings model
     */
    KnoxHdfsConnectorNodeSettings(final KnoxHdfsConnectorNodeSettings other) {
        m_auth = new KnoxHdfsAuthenticationSettings();

        try {
            final NodeSettings transferSettings = new NodeSettings("ignored");
            other.saveSettingsTo(transferSettings);
            loadSettingsFrom(transferSettings); // NOSONAR
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

        m_url.setStringValue(url);
        m_auth = new KnoxHdfsAuthenticationSettings(user, pass);
        m_workingDirectory.setStringValue(workingDir);
        m_connectionTimeout.setIntValue(connectionTimeout);
        m_receiveTimeout.setIntValue(receiveTimeout);
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_url.saveSettingsTo(settings);
        m_workingDirectory.saveSettingsTo(settings);
        m_auth.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
        m_receiveTimeout.saveSettingsTo(settings);
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.loadSettingsFrom(settings);
        m_workingDirectory.loadSettingsFrom(settings);
        m_auth.loadSettingsFrom(settings);
        m_connectionTimeout.loadSettingsFrom(settings);
        m_receiveTimeout.loadSettingsFrom(settings);
    }

    /**
     * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.validateSettings(settings);
        m_workingDirectory.validateSettings(settings);
        m_auth.validateSettings(settings);
        m_connectionTimeout.validateSettings(settings);
        m_receiveTimeout.validateSettings(settings);

        final KnoxHdfsAuthenticationSettings temp = new KnoxHdfsAuthenticationSettings();
        temp.loadSettingsFrom(settings);
        temp.validateValues();
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

            if (uri.getScheme() == null) {
                throw new InvalidSettingsException("Empty or invalid scheme in URL");
            } else if (StringUtils.isBlank(uri.getScheme())) {
                throw new InvalidSettingsException("Empty or invalid scheme in URL: '" + uri.getScheme() + "'");
            } else if (uri.getHost() == null) {
                throw new InvalidSettingsException("Empty or invalid host in URL");
            } else if (StringUtils.isBlank(uri.getHost())) {
                throw new InvalidSettingsException("Empty or invalid host in URL: '" + uri.getHost() + "'");
            } else if (uri.getPath() == null) {
                throw new InvalidSettingsException("Empty or invalid path in URL");
            } else if (StringUtils.isBlank(uri.getPath())) { // NOSONAR no else required
                throw new InvalidSettingsException("Empty or invalid path in URL: '" + uri.getPath() + "'");
            }

        } catch (final URISyntaxException e) {
            throw new InvalidSettingsException("Failed to parse URL: " + e.getMessage(), e);
        }

        m_auth.validateValues();

        if (StringUtils.isBlank(m_workingDirectory.getStringValue())) {
            throw new InvalidSettingsException("Working directory required.");
        }
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
        return m_auth.getUser(cp);
    }

    /**
     * @param cp credentials provider to use, might be {@code null} if not required
     * @return password
     */
    public String getPassword(final CredentialsProvider cp) {
        return m_auth.getPass(cp);
    }

    /**
     * @return authentication settings model
     */
    KnoxHdfsAuthenticationSettings getAuthSettingsModel() {
        return m_auth;
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
    public int getConnectionTimeout() {
        return m_connectionTimeout.getIntValue();
    }

    /**
     * @return connection timeout settings model
     */
    SettingsModelIntegerBounded getConnectionTimeoutSettingsModel() {
        return m_connectionTimeout;
    }

    /**
     * @return receive timeout in seconds
     */
    public int getReceiveTimeout() {
        return m_receiveTimeout.getIntValue();
    }

    /**
     * @return receive timeout settings model
     */
    SettingsModelIntegerBounded getReceiveTimeoutSettingsModel() {
        return m_receiveTimeout;
    }
}
