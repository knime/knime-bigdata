package org.knime.bigdata.dbfs.filehandler;

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

import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;

/**
 * Databricks file system (DBFS) implementation of the {@link RemoteFileHandler} interface.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class DBFSRemoteFileHandler implements RemoteFileHandler<DBFSConnection> {

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol DBFS_PROTOCOL =
            new Protocol("dbfs", 443, false, false, false, false, true, true, true, false, true);

    @Override
    public Protocol[] getSupportedProtocols() {
        return new Protocol[] { DBFS_PROTOCOL };
    }

    /**
     * @param connectionInformation - Connection to check
     * @return <code>true</code> if this handler supports given connection.
     */
    public static boolean isSupportedConnection(final ConnectionInformation connectionInformation) {
        return DBFS_PROTOCOL.getName().equalsIgnoreCase(connectionInformation.getProtocol());
    }

    @Override
    public RemoteFile<DBFSConnection> createRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<DBFSConnection> connectionMonitor) throws Exception {
        return new DBFSRemoteFile(uri, connectionInformation, connectionMonitor);
    }
}
