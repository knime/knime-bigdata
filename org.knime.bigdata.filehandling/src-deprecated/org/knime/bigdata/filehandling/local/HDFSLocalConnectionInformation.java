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
package org.knime.bigdata.filehandling.local;

import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;
import org.knime.base.filehandling.remote.files.RemoteFileHandlerRegistry;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;

/**
 * Subclass of {@link ConnectionInformation} to model local HDFS connections. This class is a singleton, use
 * {@link #getInstance()} to obtain a reference.
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
@Deprecated
public class HDFSLocalConnectionInformation extends ConnectionInformation {

    private static final long serialVersionUID = 1L;

    private static ConnectionInformation INSTANCE;

    /**
     * @return the singleton instance of type {@link HDFSLocalConnectionInformation}.
     * @deprecated
     */
    @Deprecated
    public static synchronized ConnectionInformation getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new HDFSLocalConnectionInformation();
            INSTANCE.setProtocol(HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL.getName());
            INSTANCE.setHost("localhost");
            INSTANCE.setPort(HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL.getPort());
            INSTANCE.setUser(null);
            INSTANCE.setPassword(null);
        }

        return INSTANCE;
    }

    /**
     * Private  constructor.
     */
    private HDFSLocalConnectionInformation() {

    }

    /**
     * Constructor to load model settings from {@link ModelContentRO}
     *
     * @param model The {@link ModelContentRO} to load the settings from
     * @throws InvalidSettingsException
     * @deprecated
     */
    @Deprecated
    protected HDFSLocalConnectionInformation(final ModelContentRO model) throws InvalidSettingsException {
        super(model);
    }

    /**
     * Checks if this connection information object fits to the URI.
     *
     *
     * @param uri The URI to check against
     * @throws Exception If something is incompatible
     */
    @Override
	public void fitsToURI(final URI uri) throws Exception {
        final RemoteFileHandler<?> fileHandler =
                RemoteFileHandlerRegistry.getRemoteFileHandler(getProtocol());
        if (fileHandler == null) {
            throw new Exception("No file handler found for protocol: " + getProtocol());
        }
        final String scheme = uri.getScheme().toLowerCase();
        final Protocol[] protocols = fileHandler.getSupportedProtocols();
        boolean supportedProtocol = false;
        for (final Protocol protocol : protocols) {
            if (protocol.getName().equals(scheme)) {
                supportedProtocol = true;
                break;
            }
        }
        if (!supportedProtocol) {
            throw new Exception("Protocol " + scheme + " incompatible with connection information protcol "
                    + getProtocol());
        }
    }

}
