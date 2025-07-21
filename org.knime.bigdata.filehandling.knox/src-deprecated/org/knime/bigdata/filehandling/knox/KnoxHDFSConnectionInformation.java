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
package org.knime.bigdata.filehandling.knox;

import java.net.URI;
import java.net.URISyntaxException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;
import org.knime.bigdata.hdfs.filehandler.HDFSCompatibleConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;

/**
 * A {@link ConnectionInformation} for Web HDFS connection to a KNOX instance.
 *
 * This is an extension of the {@link ConnectionInformation} using a URL instead of a host/port setting.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@Deprecated
public class KnoxHDFSConnectionInformation extends ConnectionInformation
    implements HDFSCompatibleConnectionInformation {

    private static final long serialVersionUID = 1L;

    private static final KnoxHDFSRemoteFileHandler REMOTE_FILE_HANDLER = new KnoxHDFSRemoteFileHandler();

    private static final String URL_KEY = "knoxUrl";

    private URI m_url = null;

    /**
     * Parameterless constructor.
     * @deprecated
     */
    @Deprecated
    public KnoxHDFSConnectionInformation() {
        super();
    }

    /**
     * Construct an instance from given model.
     *
     * @param model model to restore
     * @throws InvalidSettingsException if model contains invalid settings
     * @deprecated
     */
    @Deprecated
    protected KnoxHDFSConnectionInformation(final ModelContentRO model) throws InvalidSettingsException {
        super(model);

        try {
            setURL(new URI(model.getString(URL_KEY)));
        } catch (final URISyntaxException e) {
            throw new InvalidSettingsException(
                "Unable to parse URL '" + model.getString(URL_KEY) + "': " + e.getMessage(), e);
        }
    }

    @Override
    public void save(final ModelContentWO model) {
        super.save(model);
        model.addString(URL_KEY, m_url.toString());
    }

    public static KnoxHDFSConnectionInformation load(final ModelContentRO model) throws InvalidSettingsException {
        return new KnoxHDFSConnectionInformation(model);
    }

    @Override
    public RemoteFileHandler<?> getRemoteFileHandler() {
        return REMOTE_FILE_HANDLER;
    }

    /**
     * @return KNOX URL to use
     * @deprecated
     */
    @Deprecated
    URI getURL() {
        return m_url;
    }

    /**
     * @param url KNOX URL to use
     * @deprecated
     */
    @Deprecated
    public void setURL(final URI url) {
        m_url = url;
        setHost(m_url.getHost());
        setPort(m_url.getPort());
    }

    /**
     * @return unique identifier of the connection using the URL and the configured username
     * @deprecated
     */
    @Deprecated
    String getIdentifier() {
        try {
            final Protocol protocol = KnoxHDFSRemoteFileHandler.KNOXHDFS_PROTOCOL;
            int port = m_url.getPort();
            // If no port is available use the default port
            if (port < 0) {
                port = protocol.getPort();
            }

            return new URI(protocol.getName(), getUser(), m_url.getHost(), port, m_url.getPath(), null, null).toString();
        } catch (final URISyntaxException e) {
            throw new RuntimeException("Unable to create uniq KNOX connection identifier: " + e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return getIdentifier();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((m_url == null) ? 0 : m_url.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KnoxHDFSConnectionInformation other = (KnoxHDFSConnectionInformation)obj;
        if (m_url == null) {
            if (other.m_url != null) {
                return false;
            }
        } else if (!m_url.equals(other.m_url)) {
            return false;
        }
        return true;
    }
}
