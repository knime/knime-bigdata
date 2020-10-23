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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFileSystem;
import org.knime.bigdata.hadoop.filehandling.node.HdfsAuthenticationSettings.AuthType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for {@link HdfsConnectorNodeModel}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsConnectorNodeSettings {

    private static final HdfsProtocol DEFAULT_PROTOCOL = HdfsProtocol.HDFS;

    private static final String DEFAULT_HOST = "";

    private static final String DEFAULT_WORKING_DIR = HdfsFileSystem.PATH_SEPARATOR;

    private final SettingsModelString m_protocol = new SettingsModelString("protocol", DEFAULT_PROTOCOL.name());

    private final SettingsModelString m_host = new SettingsModelString("host", DEFAULT_HOST);

    private final SettingsModelBoolean m_useCustomPort = new SettingsModelBoolean("useCustomPort", false);

    private final SettingsModelIntegerBounded m_customPort = new SettingsModelIntegerBounded("customPort", DEFAULT_PROTOCOL.getDefaultPort(), 1, 65535);

    private final HdfsAuthenticationSettings m_auth;

    private final SettingsModelString m_workingDirectory =
        new SettingsModelString("workingDirectory", DEFAULT_WORKING_DIR);

    /**
     * Default constructor.
     */
    public HdfsConnectorNodeSettings() {
        m_customPort.setEnabled(m_useCustomPort.getBooleanValue());
        m_auth = new HdfsAuthenticationSettings();
    }

    /**
     * Clone a given settings model
     */
    HdfsConnectorNodeSettings(final HdfsConnectorNodeSettings other) {
        m_auth = new HdfsAuthenticationSettings();

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
     * @param protocol hadoop protocol to use (hdfs, webhdfs or swebhdfs)
     * @param host hostname of name node or REST endpoint
     * @param useCustomPort {@code false} if the default port should be used, {@code true} if the given port should be used
     * @param customPort port of name node or REST endpoint
     * @param authType authentication type
     * @param user username to use
     * @param workingDir current working directory
     */
    public HdfsConnectorNodeSettings(final String protocol, final String host, final boolean useCustomPort,
        final int customPort, final AuthType authType, final String user, final String workingDir) {

        m_protocol.setStringValue(protocol);
        m_host.setStringValue(host);
        m_useCustomPort.setBooleanValue(useCustomPort);
        m_customPort.setIntValue(customPort);
        m_customPort.setEnabled(useCustomPort);
        m_auth = new HdfsAuthenticationSettings(authType, user);
        m_workingDirectory.setStringValue(workingDir);
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_protocol.saveSettingsTo(settings);
        m_host.saveSettingsTo(settings);
        m_useCustomPort.saveSettingsTo(settings);
        m_customPort.saveSettingsTo(settings);
        m_auth.saveSettingsTo(settings);
        m_workingDirectory.saveSettingsTo(settings);
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_protocol.loadSettingsFrom(settings);
        m_host.loadSettingsFrom(settings);
        m_useCustomPort.loadSettingsFrom(settings);
        m_customPort.loadSettingsFrom(settings);
        m_customPort.setEnabled(m_useCustomPort.getBooleanValue());
        m_auth.loadSettingsFrom(settings);
        m_workingDirectory.loadSettingsFrom(settings);
    }

    /**
     * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_protocol.validateSettings(settings);
        m_host.validateSettings(settings);
        m_useCustomPort.validateSettings(settings);
        m_customPort.validateSettings(settings);
        m_auth.validateSettings(settings);
        m_workingDirectory.validateSettings(settings);
    }

    /**
     * Validate the values for all the {@link SettingsModel}s
     *
     * @throws InvalidSettingsException When a setting is set inappropriately.
     */
    public void validateValues() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_protocol.getStringValue())) {
            throw new InvalidSettingsException("Protocol required.");
        }

        if (StringUtils.isBlank(m_host.getStringValue())) {
            throw new InvalidSettingsException("Host required.");
        }

        m_auth.validateValues();

        if (StringUtils.isBlank(m_workingDirectory.getStringValue())) {
            throw new InvalidSettingsException("Working directory required.");
        }
    }

    /**
     * @return selected protocol
     */
    public HdfsProtocol getProtocol() {
        return HdfsProtocol.valueOf(m_protocol.getStringValue());
    }

    /**
     * @return Hadoop compatible URI using scheme, host and port
     * @throws IOException
     */
    public URI getHadopURI() throws IOException {
        try {
            return new URI(getProtocol().getHadoopScheme(), null, getHost(), getPort(), null, null, null);
        } catch (final URISyntaxException e) {
            throw new IOException("Failed to create Hadoop URI", e);
        }
    }

    /**
     * @return protocol settings model
     */
    SettingsModelString getHadoopProtocolSettingsModel() {
        return m_protocol;
    }

    /**
     * @return remote host.
     */
    public String getHost() {
        return m_host.getStringValue();
    }

    /**
     * @return host settings model
     */
    SettingsModelString getHostSettingsModel() {
        return m_host;
    }

    /**
     * @return default or custom port, depending on {@link #useCustomPort()}
     */
    public int getPort() {
        return useCustomPort() ? getCustomPort() : getDefaultPort();
    }

    /**
     * @return {@code false} if the default port should be used, {@code true} if port of {@link #getCustomPort()} should be used
     */
    public boolean useCustomPort() {
        return m_useCustomPort.getBooleanValue();
    }

    /**
     * @return use custom port settings model
     */
    SettingsModelBoolean getUseCustomPortSettingsModel() {
        return m_useCustomPort;
    }

    /**
     * Set use custom port setting
     */
    void setUseCustomPort(final boolean useCustomPort) {
        m_useCustomPort.setBooleanValue(useCustomPort);
        m_customPort.setEnabled(useCustomPort);
    }

    /**
     * @return default protocol port
     */
    public int getDefaultPort() {
        return getProtocol().getDefaultPort();
    }

    /**
     * @return custom remote port
     */
    public int getCustomPort() {
        return m_customPort.getIntValue();
    }

    /**
     * @return custom port settings model
     */
    SettingsModelIntegerBounded getCustomPortSettingsModel() {
        return m_customPort;
    }

    /**
     * @return {@code true} if kerberos authentication should be used
     */
    public boolean useKerberos() {
        return m_auth.useKerberosAuthentication();
    }

    /**
     * @return user name, might be empty depending on the authentication method
     */
    public String getUser() {
        return m_auth.getUser();
    }

    /**
     * @return authentication settings model
     */
    HdfsAuthenticationSettings getAuthSettingsModel() {
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
}
