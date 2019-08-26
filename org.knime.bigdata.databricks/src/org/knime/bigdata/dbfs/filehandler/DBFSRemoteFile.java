package org.knime.bigdata.dbfs.filehandler;

import java.io.FileNotFoundException;
import java.io.IOException;

/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 */

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.databricks.rest.dbfs.FileInfoReponse;
import org.knime.core.node.ExecutionContext;

/**
 * DBFS remote file implementation with file info cache.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DBFSRemoteFile extends RemoteFile<DBFSConnection> {

    private boolean m_fileInfoLoaded = false;
    private FileInfoReponse m_fileInfo;

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
     * Create a remote DBFS file with cached {@link FileInfoReponse}.
     *
     * @param fileInfo cached file info
     * @param uri The URI pointing to the file
     * @param connectionInformation Connection information to the file
     * @param connectionMonitor Monitor for the connection
     */
    protected DBFSRemoteFile(final FileInfoReponse fileInfo, final URI uri,
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

    private FileInfoReponse getFileInfo() throws IOException {
        if (!m_fileInfoLoaded) {
            try {
                m_fileInfo = getOpenedConnection().getFileInfo(getURI().getPath());
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
        return new DBFSInputStream(getURI().getPath(), getOpenedConnection());
    }

    @Override
    public OutputStream openOutputStream() throws Exception {
        invalidateCachedFileInfo();
        return new DBFSOutputStream(getURI().getPath(), getOpenedConnection(), true);
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
        getOpenedConnection().move(source.getURI().getPath(), getURI().getPath());
    }

    @Override
    public boolean delete() throws Exception {
        invalidateCachedFileInfo();
        return getOpenedConnection().delete(getURI().getPath(), true);
    }

    @Override
    public DBFSRemoteFile[] listFiles() throws Exception {
        final FileInfoReponse[] fileInfos = getOpenedConnection().listFiles(getURI().getPath());
        final DBFSRemoteFile[] files = new DBFSRemoteFile[fileInfos.length];
        for (int i = 0, length = fileInfos.length; i < length; i++) {
            final String path = fileInfos[i].path + (fileInfos[i].is_dir ? "/" : "");
            final URI uri = getURI().resolve(path);
            files[i] = new DBFSRemoteFile(fileInfos[i], uri, getConnectionInformation(), getConnectionMonitor());
        }
        return files;
    }

    @Override
    public boolean mkDir() throws Exception {
        invalidateCachedFileInfo();
        return getConnection().mkDir(getURI().getPath());
    }

    private void invalidateCachedFileInfo() {
        m_fileInfoLoaded = false;
        m_fileInfo = null;
    }
}
