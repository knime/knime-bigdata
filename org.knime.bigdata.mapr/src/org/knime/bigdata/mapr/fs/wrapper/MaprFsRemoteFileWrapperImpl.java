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
 *   Created on Sep 9, 2018 by bjoern
 */
package org.knime.bigdata.mapr.fs.wrapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MaprFsRemoteFileWrapperImpl implements MaprFsRemoteFileWrapper {

    private final URI m_uri;

    private final MaprFsConnectionWrapperImpl m_connection;

    public MaprFsRemoteFileWrapperImpl(final Object connection, final URI uri) {
        m_uri = uri;
        m_connection = (MaprFsConnectionWrapperImpl) connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() throws Exception {
        return m_connection.exists(m_uri);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDirectory() throws Exception {
        return m_connection.isDirectory(m_uri);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream openInputStream() throws Exception {
        return m_connection.open(m_uri);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream openOutputStream() throws Exception {
        return m_connection.create(m_uri, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getSize() throws Exception {
        return m_connection.getContentSummary(m_uri).getLength();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long lastModified() throws Exception {
        return m_connection.getFileStatus(m_uri).getModificationTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete() throws Exception {
        return m_connection.delete(m_uri, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean mkDir() throws Exception {
        return m_connection.mkDir(m_uri);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setPermission(final String unixSymbolicPermission) throws IOException {
        m_connection.setPermission(m_uri, unixSymbolicPermission);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MaprFsRemoteFileWrapper[] listFiles() throws Exception {
        return Arrays.stream(m_connection.listFiles(m_uri))
            .map(f -> new MaprFsRemoteFileWrapperImpl(m_connection,
                f.getPath().toUri()))
            .toArray(MaprFsRemoteFileWrapper[]::new);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URI getURI() {
        return m_uri;
    }
}
