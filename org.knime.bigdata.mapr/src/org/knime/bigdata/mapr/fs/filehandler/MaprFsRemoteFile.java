package org.knime.bigdata.mapr.fs.filehandler;

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
import java.util.Arrays;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.mapr.fs.wrapper.MaprFsRemoteFileWrapper;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class MaprFsRemoteFile extends RemoteFile<MaprFsConnection> {

    private final MaprFsRemoteFileWrapper m_wrapper;

    protected MaprFsRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<MaprFsConnection> connectionMonitor, final MaprFsRemoteFileWrapper wrapper) {
        super(uri, connectionInformation, connectionMonitor);
        m_wrapper = wrapper;
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
    public String getType() {
        return getConnectionInformation().getProtocol();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() throws Exception {
        return m_wrapper.exists();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDirectory() throws Exception {
        return m_wrapper.isDirectory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream openInputStream() throws Exception {
        return m_wrapper.openInputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream openOutputStream() throws Exception {
        return m_wrapper.openOutputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getSize() throws Exception {
        return m_wrapper.getSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long lastModified() throws Exception {
        return m_wrapper.lastModified();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete() throws Exception {
        return m_wrapper.delete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MaprFsRemoteFile[] listFiles() throws Exception {

        return Arrays.stream(m_wrapper.listFiles()).map(fileWrapper -> new MaprFsRemoteFile(fileWrapper.getURI(),
            getConnectionInformation(), getConnectionMonitor(), fileWrapper)).toArray(MaprFsRemoteFile[]::new);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean mkDir() throws Exception {
        return m_wrapper.mkDir();
    }

    /**
     * @param unixSymbolicPermission a Unix symbolic permission string e.g. "-rw-rw-rw-"
     * @throws IOException if an exception occurs
     */
    public void setPermission(final String unixSymbolicPermission) throws IOException {
        m_wrapper.setPermission(unixSymbolicPermission);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected MaprFsConnection createConnection() {
        return new MaprFsConnection(getConnectionInformation());
    }
}
