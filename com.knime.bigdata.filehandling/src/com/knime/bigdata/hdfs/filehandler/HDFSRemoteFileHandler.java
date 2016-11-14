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

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol SWEBHDFS_PROTOCOL =
            new Protocol("swebhdfs", 50470, false, false, false, true, true, true, false, true);

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol HTTPFS_PROTOCOL =
            new Protocol("httpfs", 14000, false, false, false, true, true, true, false, true);

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol HTTPSFS_PROTOCOL =
            new Protocol("httpsfs", 14000, false, false, false, true, true, true, false, true);

    /** All supported {@link Protocol}s of this {@link RemoteFileHandler}. */
    public static final Protocol SUPPORTED_PROTOCOLS[] =
            new Protocol[] { HDFS_PROTOCOL, WEBHDFS_PROTOCOL, SWEBHDFS_PROTOCOL, HTTPFS_PROTOCOL, HTTPSFS_PROTOCOL };

    /** All encrypted {@link Protocol}s of this {@link RemoteFileHandler}. */
    public static final Protocol ENCRYPTED_PROTOCOLS[] =
            new Protocol[] { SWEBHDFS_PROTOCOL, HTTPSFS_PROTOCOL };

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
     * @param connectionInformation - Connection to check
     * @return <code>true</code> if this protocol use encryption
     */
    public static boolean isEncryptedConnection(final ConnectionInformation connectionInformation) {
        return containsScheme(ENCRYPTED_PROTOCOLS, connectionInformation.getProtocol());
    }

    /**
     * Maps HttpFS into WebHDFS scheme.
     * @param scheme - Input scheme
     * @return Mapped scheme if required, input scheme otherwise.
     */
    public static String mapScheme(final String scheme) {
        if (scheme.equalsIgnoreCase(HTTPFS_PROTOCOL.getName())) {
            return WEBHDFS_PROTOCOL.getName();
        } else if (scheme.equalsIgnoreCase(HTTPSFS_PROTOCOL.getName())) {
            return SWEBHDFS_PROTOCOL.getName();
        } else {
            return scheme;
        }
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
