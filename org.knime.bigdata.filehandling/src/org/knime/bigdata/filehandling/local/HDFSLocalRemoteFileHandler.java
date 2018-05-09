package org.knime.bigdata.filehandling.local;

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
+ *   Created on Jan 22, 2018 by oole
 */


import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.FileRemoteFileHandler;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;

/**
 * Masks the local filesystem as an Hadoop file system (hdfs). Implementation of the {@link RemoteFileHandler} interface.
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
public final class HDFSLocalRemoteFileHandler implements RemoteFileHandler<HDFSLocalConnection> {

    /**The {@link Protocol} of this {@link RemoteFileHandler}, this is used to register the {@link RemoteFileHandler}.*/
    public static final Protocol HDFS_LOCAL_PROTOCOL =
            new Protocol("hdfs-local", -1, false, false, false, true, true, true, false, true);

    /** The true {@link Protocol}, used for accessing the remote file.
     *  The 'file' protocol is masked by the {@link FileRemoteFileHandler}.*/
    static final Protocol TRUE_PROTOCOL =
            new Protocol("file", -1, false, false, false, true, true, true, false, true);

    /** All supported {@link Protocol}s of this {@link RemoteFileHandler}. */
    public static final Protocol SUPPORTED_PROTOCOLS[] =
            new Protocol[] { HDFS_LOCAL_PROTOCOL};


    @Override
    public Protocol[] getSupportedProtocols() {
        return SUPPORTED_PROTOCOLS;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RemoteFile<HDFSLocalConnection> createRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<HDFSLocalConnection> connectionMonitor) throws Exception {
        final HDFSLocalRemoteFile remoteFile =
                new HDFSLocalRemoteFile(uri,
                    connectionInformation, connectionMonitor);
        return remoteFile;
    }


    /**
     * Maps HttpFS into WebHDFS scheme.
     * @param scheme - Input scheme
     * @return Mapped scheme if required, input scheme otherwise.
     */
    public static String mapScheme(final String scheme) {
        if (scheme.equalsIgnoreCase(HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL.getName())) {
            return HDFSLocalRemoteFileHandler.TRUE_PROTOCOL.getName();
        } else if (scheme.equalsIgnoreCase(HDFSLocalRemoteFileHandler.TRUE_PROTOCOL.getName())){
            return HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL.getName();
        } else {
        	return scheme;
        }
    }


    /**
     * @param connectionInformation - Connection to check
     * @return <code>true</code> if this handler supports given connection.
     */
    public static boolean isSupportedConnection(final ConnectionInformation connectionInformation) {
        return containsScheme(SUPPORTED_PROTOCOLS, connectionInformation.getProtocol());
    }


    /**
     * @return <code>true</code> if protocols contains protocol with given name
     */
    private static boolean containsScheme(final Protocol protocols[], final String name) {
        for (final Protocol current : protocols) {
            if (current.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
