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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.databricks.rest.dbfs.FileInfo;
import org.knime.core.node.ExecutionContext;

/**
 * DBFS remote file implementation with file info cache.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DBFSRemoteFile extends RemoteFile<DBFSConnection> {

    private boolean m_fileInfoLoaded = false;
    private FileInfo m_fileInfo;

    /**
     * Create a remote DBFS file.
     *
     * @param uri The URI pointing to the file
     * @param connectionInformation Connection information to the file
     * @param connectionMonitor Monitor for the connection
     */
    protected DBFSRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<DBFSConnection> connectionMonitor) {
        super(uri, connectionInformation, connectionMonitor);
    }

    /**
     * Create a remote DBFS file with cached {@link FileInfo}.
     *
     * @param fileInfo cached file info
     * @param uri The URI pointing to the file
     * @param connectionInformation Connection information to the file
     * @param connectionMonitor Monitor for the connection
     */
    protected DBFSRemoteFile(final FileInfo fileInfo, final URI uri,
            final ConnectionInformation connectionInformation,
            final ConnectionMonitor<DBFSConnection> connectionMonitor) {
        super(uri, connectionInformation, connectionMonitor);

        m_fileInfoLoaded = true;
        m_fileInfo = fileInfo;
    }

    private DBFSConnection getOpenedConnection() throws IOException {
        try {
            open();
            return getConnection();
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    private FileInfo getFileInfo() throws IOException {
        if (!m_fileInfoLoaded) {
            try {
                m_fileInfo = getOpenedConnection().getFileInfo(getURI().getPath());
                m_fileInfoLoaded = true;
            } catch (AccessDeniedException e) {
                throw new RemoteFile.AccessControlException(e);
            } catch (FileNotFoundException e) {
                m_fileInfoLoaded = true;
                m_fileInfo = null;
                throw e;
            }
        } else if (m_fileInfo == null) {
            throw new FileNotFoundException(getURI().getPath());
        }

        return m_fileInfo;
    }

    @Override
    protected boolean usesConnection() {
        return true;
    }

    @Override
    protected DBFSConnection createConnection() {
        return new DBFSConnection(getConnectionInformation());
    }

    @Override
    public String getType() {
        return DBFSRemoteFileHandler.DBFS_PROTOCOL.getName();
    }

    @Override
    public boolean exists() throws Exception {
        try {
            // Note: The databricks REST API includes special directories like '/databricks-results'
            // in the root directory file listing, but return a 404 not found on get file info of
            // '/databricks-results'. This crashes the file chooser dialog.
            // Use the cached 'exists' info here if available from e.g. the list directory.
            getFileInfo();
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean isDirectory() throws Exception {
        return getFileInfo().is_dir;
    }

    @Override
    public InputStream openInputStream() throws Exception {
        return new DBFSInputStream(getURI().getPath(), getOpenedConnection().getDbfsApi());
    }

    @Override
    public OutputStream openOutputStream() throws Exception {
        invalidateCachedFileInfo();
        return new DBFSOutputStream(getURI().getPath(), getOpenedConnection().getDbfsApi(), true);
    }

    @Override
    public long getSize() throws Exception {
        return getFileInfo().file_size;
    }

    @Override
    public long lastModified() throws Exception {
        throw new IllegalArgumentException("DBFS does not support modified timestamps.");
    }

    @Override
    public void move(final RemoteFile<DBFSConnection> source, final ExecutionContext exec) throws Exception {
        invalidateCachedFileInfo();
        if (getIdentifier().equals(source.getIdentifier())) {
            getOpenedConnection().move(source.getURI().getPath(), getURI().getPath());
        } else {
            super.move(source, exec);
        }
    }

    @Override
    public boolean delete() throws Exception {
        invalidateCachedFileInfo();
        return getOpenedConnection().delete(getURI().getPath(), true);
    }

    @Override
    public DBFSRemoteFile[] listFiles() throws Exception {
        try {
            final FileInfo[] fileInfos = getOpenedConnection().listFiles(getURI().getPath());
            final DBFSRemoteFile[] files = new DBFSRemoteFile[fileInfos.length];
            for (int i = 0, length = fileInfos.length; i < length; i++) {
                final String path = fileInfos[i].path + (fileInfos[i].is_dir ? "/" : "");
                final URI uri = getURI().resolve(NodeUtils.encodePath(path));
                files[i] = new DBFSRemoteFile(fileInfos[i], uri, getConnectionInformation(), getConnectionMonitor());
            }
            return files;
        } catch (AccessDeniedException e) {
            throw new RemoteFile.AccessControlException(e);
        }
    }

    @Override
    public boolean mkDir() throws Exception {
        invalidateCachedFileInfo();
        return getOpenedConnection().mkDir(getURI().getPath());
    }

    private void invalidateCachedFileInfo() {
        m_fileInfoLoaded = false;
        m_fileInfo = null;
    }
}
