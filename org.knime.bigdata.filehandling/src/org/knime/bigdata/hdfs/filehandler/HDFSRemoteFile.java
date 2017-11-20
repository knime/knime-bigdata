package org.knime.bigdata.hdfs.filehandler;

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
 *
 * History
 *   Created on 31.07.2014 by koetter
 */


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class HDFSRemoteFile extends RemoteFile<HDFSConnection> {

    /**
     *  Create a remote hdfs file.
     * @param uri The uri pointing to the file
     * @param connectionInformation Connection information to the file
     * @param connectionMonitor Monitor for the connection
     */
    protected HDFSRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<HDFSConnection> connectionMonitor) {
        super(uri, connectionInformation, connectionMonitor);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean usesConnection() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected HDFSConnection createConnection() {
        return new HDFSConnection(getConnectionInformation());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return super.getConnectionInformation().getProtocol();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() throws Exception {
        return getOpenedConnection().exists(getURI());
    }

    private HDFSConnection getOpenedConnection() throws IOException {
        try {
            open();
            return getConnection();
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDirectory() throws Exception {
         return getOpenedConnection().isDirectory(getURI());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream openInputStream() throws Exception {
        return getOpenedConnection().open(getURI());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream openOutputStream() throws Exception {
        return getOpenedConnection().create(getURI(), true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getSize() throws Exception {
        final ContentSummary summary = getOpenedConnection().getContentSummary(getURI());
        return summary.getLength();
    }

    /**
     * Helper class to access {@link FileStatus} information.
     * @return the {@link FileStatus} object of this remote file
     * @throws IOException
     */
    private FileStatus getFileStatus() throws IOException {
        final FileStatus fileStatus = getOpenedConnection().getFileStatus(getURI());
        return fileStatus;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long lastModified() throws Exception {
        final FileStatus fileStatus = getFileStatus();
        return fileStatus.getModificationTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete() throws Exception {
        return getOpenedConnection().delete(getURI(), true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HDFSRemoteFile[] listFiles() throws Exception {
        try {
            final FileStatus[] listStatus = getOpenedConnection().listFiles(getURI());
            if (listStatus == null) {
                return new HDFSRemoteFile[0];
            }
            final HDFSRemoteFile[] files = new HDFSRemoteFile[listStatus.length];
            for (int i = 0, length = listStatus.length; i < length; i++) {
                files[i] = new HDFSRemoteFile(listStatus[i].getPath().toUri(), getConnectionInformation(),
                    getConnectionMonitor());
            }
            return files;

        } catch (org.apache.hadoop.security.AccessControlException e) {
            throw new RemoteFile.AccessControlException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean mkDir() throws Exception {
        return getOpenedConnection().mkDir(getURI());
    }

    /**
     * @param unixSymbolicPermission  a Unix symbolic permission string e.g. "-rw-rw-rw-"
     * @throws IOException if an exception occurs
     */
    public void setPermission(final String unixSymbolicPermission) throws IOException {
        getOpenedConnection().setPermission(getURI(), unixSymbolicPermission);
    }
}
