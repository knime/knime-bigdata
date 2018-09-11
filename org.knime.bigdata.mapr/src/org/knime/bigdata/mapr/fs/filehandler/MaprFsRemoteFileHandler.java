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


import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;
import org.knime.bigdata.mapr.fs.wrapper.MaprFsWrapperFactory;

/**
 * Hadoop file system (hdfs) implementation of the {@link RemoteFileHandler} interface.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public final class MaprFsRemoteFileHandler implements RemoteFileHandler<MaprFsConnection> {

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol MAPRFS_PROTOCOL =
            new Protocol("maprfs", 8020, false, false, false, true, true, true, false, true);

    public static final Protocol SUPPORTED_PROTOCOLS[] =
            new Protocol[] { MAPRFS_PROTOCOL };

    @Override
    public Protocol[] getSupportedProtocols() {
        return SUPPORTED_PROTOCOLS;
    }

    /**
     * @param connectionInformation - Connection to check
     * @return <code>true</code> if this handler supports given connection.
     */
    public static boolean isSupportedConnection(final ConnectionInformation connectionInformation) {
        return containsScheme(SUPPORTED_PROTOCOLS, connectionInformation.getProtocol());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RemoteFile<MaprFsConnection> createRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<MaprFsConnection> connectionMonitor) throws Exception {
        return new MaprFsRemoteFile(uri,
            connectionInformation,
            connectionMonitor,
            MaprFsWrapperFactory.createRemoteFileWrapper(uri, connectionInformation.useKerberos()));
    }

    /**
     * @return <code>true</code> if protocols contains protocol with given name
     */
    private static boolean containsScheme(final Protocol protocols[], final String name) {
        for (Protocol current : protocols) {
            if (current.getName().equals(name)) {
                return true;
            }
        }

        return false;
    }
}
