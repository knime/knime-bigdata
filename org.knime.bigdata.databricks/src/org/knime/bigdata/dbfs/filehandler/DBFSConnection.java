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
package org.knime.bigdata.dbfs.filehandler;

import java.io.IOException;
import java.time.Duration;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.dbfs.AddBlock;
import org.knime.bigdata.databricks.rest.dbfs.Close;
import org.knime.bigdata.databricks.rest.dbfs.Create;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPI;
import org.knime.bigdata.databricks.rest.dbfs.Delete;
import org.knime.bigdata.databricks.rest.dbfs.FileBlock;
import org.knime.bigdata.databricks.rest.dbfs.FileInfo;
import org.knime.bigdata.databricks.rest.dbfs.FileInfoList;
import org.knime.bigdata.databricks.rest.dbfs.Mkdir;
import org.knime.bigdata.databricks.rest.dbfs.Move;
import org.knime.core.util.KnimeEncryption;

/**
 * DBFS connection implementation.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DBFSConnection extends Connection {

    private DBFSAPI m_dbfsApi;

    private final ConnectionInformation m_connectionInformation;

    /**
     * @param connectionInformation the {@link ConnectionInformation} to use
     */
    public DBFSConnection(final ConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;

        if (connectionInformation == null) {
            throw new RuntimeException("DBFS connection informations input required.");
        }
    }

    synchronized boolean delete(final String path, final boolean recursive) throws IOException {
        m_dbfsApi.delete(Delete.create(path, recursive));
        return true;
    }

    synchronized void move(final String source, final String destination) throws IOException {
        m_dbfsApi.move(Move.create(source, destination));
    }

    synchronized boolean mkDir(final String path) throws IOException {
        m_dbfsApi.mkdirs(Mkdir.create(path));
        return true;
    }

    synchronized FileBlock readBlock(final String path, final long offset, final long length) throws IOException {
        return m_dbfsApi.read(path, offset, length);
    }

    synchronized long createHandle(final String path, final boolean overwrite) throws IOException {
        return m_dbfsApi.create(Create.create(path, overwrite)).handle;
    }

    synchronized void addBlock(final long handle, final String data) throws IOException {
        m_dbfsApi.addBlock(AddBlock.create(handle, data));
    }

    synchronized void closeHandle(final long handle) throws IOException {
        m_dbfsApi.close(Close.create(handle));
    }

    synchronized FileInfo getFileInfo(final String path) throws IOException {
        return m_dbfsApi.getStatus(path);
    }

    synchronized FileInfo[] listFiles(final String path) throws IOException {
        final FileInfoList resp = m_dbfsApi.list(path);
        final FileInfo[] files = resp.files;
        return files != null ? files : new FileInfo[0];
    }

    @Override
    public synchronized void open() throws Exception {
        // token based authentication
        Duration timeout = Duration.ofMillis(m_connectionInformation.getTimeout());

        if (m_dbfsApi == null && m_connectionInformation.useToken()) {
            m_dbfsApi = DatabricksRESTClient.create(
                "https://" + m_connectionInformation.getHost() + ":" + m_connectionInformation.getPort(),
                DBFSAPI.class,
                KnimeEncryption.decrypt(m_connectionInformation.getToken()),
                timeout, timeout);

        // user+password based authentication
        } else if (m_dbfsApi == null) {
            m_dbfsApi = DatabricksRESTClient.create(
                "https://" + m_connectionInformation.getHost() + ":" + m_connectionInformation.getPort(),
                DBFSAPI.class,
                m_connectionInformation.getUser(),
                KnimeEncryption.decrypt(m_connectionInformation.getPassword()),
                timeout, timeout);
        }
    }

    @Override
    public synchronized boolean isOpen() {
        return m_dbfsApi != null;
    }

    @Override
    public synchronized void close() throws Exception {
        DatabricksRESTClient.close(m_dbfsApi);
        m_dbfsApi = null;
    }

    /**
     * @return the {@link DBFSAPI} object.
     */
    public DBFSAPI getDbfsApi() {
        return m_dbfsApi;
    }
}
