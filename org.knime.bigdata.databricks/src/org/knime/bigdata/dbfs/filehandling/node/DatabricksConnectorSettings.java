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

import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.dbfs.filehandling.fs.DatabricksFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the {@link DatabricksConnectorNodeModel} node.
 *
 * @author Alexander Bondaletov
 */
public class DatabricksConnectorSettings {

    /**
     * Settings key for the authentication sub-settings. Must be public for dialog.
     */
    public static final String KEY_AUTH = "auth";

    private static final String KEY_HOST = "host";
    private static final String KEY_PORT = "port";

    private static final String KEY_WORKING_DIRECTORY = "workingDirectory";
    private static final String KEY_CONNECTION_TIMEOUT = "connectionTimeout";
    private static final String KEY_READ_TIMEOUT = "readTimeout";

    private static final int DEFAULT_TIMEOUT = 30;

    private final SettingsModelString m_host;
    private final SettingsModelIntegerBounded m_port;

    private final SettingsModelString m_workingDirectory;
    private final SettingsModelIntegerBounded m_connectionTimeout;
    private final SettingsModelIntegerBounded m_readTimeout;

    private final DbfsAuthenticationNodeSettings m_authSettings;

    /**
     * Creates new instance.
     */
    public DatabricksConnectorSettings() {
        m_host = new SettingsModelString(KEY_HOST, "");
        m_port = new SettingsModelIntegerBounded(KEY_PORT, 443, 0, 65535);
        m_workingDirectory = new SettingsModelString(KEY_WORKING_DIRECTORY, DatabricksFileSystem.PATH_SEPARATOR);
        m_connectionTimeout =
            new SettingsModelIntegerBounded(KEY_CONNECTION_TIMEOUT, DEFAULT_TIMEOUT, 0, Integer.MAX_VALUE);
        m_readTimeout = new SettingsModelIntegerBounded(KEY_READ_TIMEOUT, DEFAULT_TIMEOUT, 0, Integer.MAX_VALUE);
        m_authSettings = new DbfsAuthenticationNodeSettings();
    }

    /**
     * @return authentication settings.
     */
    public DbfsAuthenticationNodeSettings getAuthenticationSettings() {
        return m_authSettings;
    }

    /**
     * @return the host model
     */
    public SettingsModelString getHostModel() {
        return m_host;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return m_host.getStringValue();
    }

    /**
     * @return the port model
     */
    public SettingsModelIntegerBounded getPortModel() {
        return m_port;
    }

    /**
     * @return The deployment URL consists of host and port from the settings.
     */
    public String getDeploymentUrl() {
        return String.format("https://%s:%d", m_host.getStringValue(), m_port.getIntValue());
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

    private void save(final NodeSettingsWO settings) {
        m_host.saveSettingsTo(settings);
        m_port.saveSettingsTo(settings);
        m_workingDirectory.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
        m_readTimeout.saveSettingsTo(settings);
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
        m_authSettings.saveSettingsForModel(settings.addNodeSettings(KEY_AUTH));
    }

    /**
     * Validates settings in the given {@link NodeSettingsRO}.
     *
     * @param settings
     *            The settings.
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_host.validateSettings(settings);
        m_port.validateSettings(settings);
        m_workingDirectory.validateSettings(settings);
        m_connectionTimeout.validateSettings(settings);
        m_readTimeout.validateSettings(settings);

        m_authSettings.validateSettings(settings.getNodeSettings(KEY_AUTH));

        DatabricksConnectorSettings temp = new DatabricksConnectorSettings();
        temp.loadSettingsForModel(settings);
        temp.validate();
    }

    /**
     * Validates settings consistency for this instance.
     *
     * @throws InvalidSettingsException
     */
    public void validate() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_host.getStringValue())) {
            throw new InvalidSettingsException("Please provide the hostname of your Databricks deployment.");
        }

        int port = m_port.getIntValue();
        if (port <= 0 || port > 65535) {
            throw new InvalidSettingsException(
                "Please provide a valid port to connect to the REST interface of your Databricks deployment.");
        }

        if (!m_workingDirectory.getStringValue().startsWith(DatabricksFileSystem.PATH_SEPARATOR)) {
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
        m_host.loadSettingsFrom(settings);
        m_port.loadSettingsFrom(settings);
        m_workingDirectory.loadSettingsFrom(settings);
        m_connectionTimeout.loadSettingsFrom(settings);
        m_readTimeout.loadSettingsFrom(settings);
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
        m_authSettings.loadSettingsForModel(settings.getNodeSettings(KEY_AUTH));
    }

    /**
     * @return a (deep) clone of this node settings object.
     */
    public DatabricksConnectorSettings createClone() {
        final NodeSettings tempSettings = new NodeSettings("ignored");
        saveSettingsForModel(tempSettings);

        final DatabricksConnectorSettings toReturn = new DatabricksConnectorSettings();
        try {
            toReturn.loadSettingsForModel(tempSettings);
        } catch (InvalidSettingsException ex) { // NOSONAR can never happen
            // won't happen
        }
        return toReturn;
    }
}
