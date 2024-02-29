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
package org.knime.bigdata.filehandling.knox;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.bigdata.filehandling.knox.rest.FileStatus;
import org.knime.bigdata.filehandling.knox.rest.KnoxHDFSClient;
import org.knime.bigdata.filehandling.knox.rest.WebHDFSAPI;
import org.knime.bigdata.hdfs.filehandler.HDFSCompatibleConnection;
import org.knime.core.util.KnimeEncryption;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;

/**
 * Web HDFS via KNOX connection implementation.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHDFSConnection extends Connection implements HDFSCompatibleConnection {

    private WebHDFSAPI m_client;

    private final KnoxHDFSConnectionInformation m_connectionInformation;

    private final ExecutorService m_uploadExecutor;

    /**
     * @param connectionInformation the {@link ConnectionInformation} to use
     */
    public KnoxHDFSConnection(final KnoxHDFSConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;
        m_uploadExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "knox-webhdfs-uploader"));

        if (connectionInformation == null) {
            throw new RuntimeException("KNOX connection informations input required.");
        }
    }

    synchronized boolean delete(final String path, final boolean recursive) throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            return m_client.delete(path, DeleteOpParam.Op.DELETE, recursive);
        } catch (Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e);
        }
    }

    synchronized void move(final String source, final String destination) throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            m_client.rename(source, PutOpParam.Op.RENAME, destination);
        } catch (Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e);
        }
    }

    synchronized boolean mkdirs(final String path) throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            return m_client.mkdirs(path, PutOpParam.Op.MKDIRS);
        } catch (Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e);
        }
    }

    synchronized InputStream open(final String path) throws IOException {
        return KnoxHDFSClient.openFile(m_client, path);
    }

    synchronized OutputStream create(final String path, final boolean overwrite) throws IOException {
        return KnoxHDFSClient.createFile(m_client, m_uploadExecutor, path, overwrite);
    }

    synchronized FileStatus getFileStatus(final String path) throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            return m_client.getFileStatus(path, GetOpParam.Op.GETFILESTATUS);
        } catch (Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e);
        }
    }

    synchronized FileStatus[] listFiles(final String path) throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            final FileStatus[] files = m_client.listStatus(path, GetOpParam.Op.LISTSTATUS).fileStatus;
            return files != null ? files : new FileStatus[0];
        } catch (Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e);
        }

    }

    @Override
    public synchronized boolean exists(final URI uri) throws IOException {
        try {
            return getFileStatus(uri.getPath()) != null;
        } catch (FileNotFoundException e) { // NOSONAR
            return false;
        }
    }

    @Override
    public synchronized URI getHomeDirectory() throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            final URI baseURI = m_connectionInformation.toURI();
            return baseURI.resolve(m_client.getHomeDirectory(GetOpParam.Op.GETHOMEDIRECTORY).toUri().getPath());
        } catch (Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e);
        }
    }

    @Override
    public synchronized void setPermission(final URI uri, final String unixSymbolicPermission) throws IOException {
        try {
            m_client.setPermission(uri.getPath(), FsPermission.valueOf(unixSymbolicPermission));
        } catch (Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e);
        }

    }

    @Override
    public synchronized void open() throws Exception {
        if (m_client == null && m_connectionInformation.useKerberos()) {
            throw new InvalidParameterException("Unsupported kerberos authentication selected.");
//            m_client = doWithAuth(() ->
//                KnoxHDFSClient.createClient(
//                    m_connectionInformation.getURL().toString(),
//                    m_connectionInformation.getTimeout(),
//                    m_connectionInformation.getTimeout())
//            );
        } else if (m_client == null) { // user+password based authentication
            m_client = KnoxHDFSClient.createClientBasicAuth(
                m_connectionInformation.getURL().toString(),
                m_connectionInformation.getUser(),
                KnimeEncryption.decrypt(m_connectionInformation.getPassword()),
                Duration.ofMillis(m_connectionInformation.getTimeout()),
                Duration.ofMillis(m_connectionInformation.getTimeout()));
        }
    }

    @Override
    public synchronized boolean isOpen() {
        return m_client != null;
    }

    @Override
    public synchronized void close() throws Exception {
        m_uploadExecutor.shutdownNow();
        KnoxHDFSClient.close(m_client);
        m_client = null;
    }
}
