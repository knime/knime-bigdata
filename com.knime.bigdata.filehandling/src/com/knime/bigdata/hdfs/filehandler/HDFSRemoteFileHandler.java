package com.knime.bigdata.hdfs.filehandler;

/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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

import com.knime.licenses.LicenseChecker;
import com.knime.licenses.LicenseFeatures;
import com.knime.licenses.LicenseUtil;

/**
 * Hadoop file system (hdfs) implementation of the {@link RemoteFileHandler} interface.
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public final class HDFSRemoteFileHandler implements RemoteFileHandler<HDFSConnection> {

    /**
     * Singleton instance.
     */
    public static final LicenseChecker LICENSE_CHECKER = new LicenseUtil(LicenseFeatures.HDFSFileHandling);

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol HDFS_PROTOCOL =
            new Protocol("hdfs", 8020, false, false, false, true, true, true, false, true);

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol WEBHDFS_PROTOCOL =
            new Protocol("webhdfs", 50070, false, false, false, true, true, true, false, true);

    public static final Protocol SUPPORTED_PROTOCOLS[] = new Protocol[] { HDFS_PROTOCOL, WEBHDFS_PROTOCOL };

    /**
     * {@inheritDoc}
     */
    @Override
    public Protocol[] getSupportedProtocols() {
        return SUPPORTED_PROTOCOLS;
    }

    /** @return true if this handler supports given connection. */
    public static boolean isSupportedConnection(final ConnectionInformation connectionInformation) {
        String protocol = connectionInformation.getProtocol();

        for (Protocol current : SUPPORTED_PROTOCOLS) {
            if (current.getName().equals(protocol)) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RemoteFile<HDFSConnection> createRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<HDFSConnection> connectionMonitor) throws Exception {
        LICENSE_CHECKER.checkLicense();
        final HDFSRemoteFile remoteFile = new HDFSRemoteFile(uri, connectionInformation, connectionMonitor);
        return remoteFile;
    }

}
