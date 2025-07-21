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
 *   Nov 11, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.filehandling.knox.node.connector;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.filehandling.knox.KnoxHDFSConnectionInformation;
import org.knime.bigdata.filehandling.knox.KnoxHDFSRemoteFileHandler;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * Web HDFS via KNOX connection settings model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@Deprecated
public class KnoxHDFSConnectionNodeSettings {

    private final SettingsModelString m_url = new SettingsModelString("url", "https://");

    private final SettingsModelAuthentication m_auth =
        new SettingsModelAuthentication("auth", AuthenticationType.USER_PWD);

    /**
     * Connection timeout in seconds.
     */
    private final SettingsModelInteger m_connectionTimeout = new SettingsModelInteger("connectionTimeout", 30);

    /**
     * Receive timeout in seconds.
     */
    private final SettingsModelInteger m_receiveTimeout = new SettingsModelInteger("receiveTimeout", 60);

    SettingsModelString getUrlModel() {
        return m_url;
    }

    SettingsModelAuthentication getAuthModel() {
        return m_auth;
    }

    SettingsModelInteger getConnectionTimeoutModel() {
        return m_connectionTimeout;
    }

    SettingsModelInteger getReceiveTimeoutModel() {
        return m_receiveTimeout;
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     * @deprecated
     */
    @Deprecated
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_url.saveSettingsTo(settings);
        m_auth.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
        m_receiveTimeout.saveSettingsTo(settings);
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     * @deprecated
     */
    @Deprecated
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.loadSettingsFrom(settings);
        m_auth.loadSettingsFrom(settings);
        m_connectionTimeout.loadSettingsFrom(settings);
        m_receiveTimeout.loadSettingsFrom(settings);
    }

    /**
     * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     * @deprecated
     */
    @Deprecated
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_url.validateSettings(settings);
        m_auth.validateSettings(settings);
        m_connectionTimeout.validateSettings(settings);
        m_receiveTimeout.validateSettings(settings);
    }

    /**
     * Validate the values for all the {@link SettingsModel}s
     *
     * @throws InvalidSettingsException When a setting is set inappropriately.
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("unused")
    public void validateValues() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_url.getStringValue())) {
            throw new InvalidSettingsException("URL required.");
        }

        try {
            new URI(m_url.getStringValue());
        } catch (final URISyntaxException e) {
            throw new InvalidSettingsException(
                "Unable to parse URL '" + m_url.getStringValue() + "': " + e.getMessage(), e);
        }

        if ((m_auth.getAuthenticationType() == AuthenticationType.USER
                || m_auth.getAuthenticationType() == AuthenticationType.USER_PWD)
                && StringUtils.isAllBlank(m_auth.getUsername())) {
            throw new InvalidSettingsException(
                "Username required. Select another authentication method or enter a username.");
        }
    }

    /**
     * Create a {@link KnoxHDFSConnectionInformation} from this settings model.
     * @deprecated
     */
    @Deprecated
    KnoxHDFSConnectionInformation createConnectionInformation(final CredentialsProvider credentialsProvider) {
        final KnoxHDFSConnectionInformation info = new KnoxHDFSConnectionInformation();
        info.setProtocol(KnoxHDFSRemoteFileHandler.KNOXHDFS_PROTOCOL.getName());
        info.setURL(URI.create(m_url.getStringValue().replaceAll("/+$", "")));
        if (m_auth.useKerberos()) {
            info.setUseKerberos(true);
        } else {
            info.setUser(m_auth.getUserName(credentialsProvider));
            info.setPassword(m_auth.getPassword(credentialsProvider));
        }
        return info;
    }
}
