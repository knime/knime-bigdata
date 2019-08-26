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
package org.knime.bigdata.dbfs.node.connector;

import java.net.URI;
import java.net.URISyntaxException;

import org.knime.base.filehandling.remote.connectioninformation.node.ConnectionInformationNodeModel;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.dbfs.filehandler.DBFSConnection;
import org.knime.bigdata.dbfs.filehandler.DBFSRemoteFileHandler;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;

/**
 * DBFS connection node model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DBFSConnectionInformationNodeModel extends ConnectionInformationNodeModel {

    DBFSConnectionInformationNodeModel() {
        super(DBFSRemoteFileHandler.DBFS_PROTOCOL);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final ConnectionInformationPortObjectSpec spec = createSpec();
        final ConnectionInformation connInfo = spec.getConnectionInformation();

        // validate that we can create a valid URI
        try {
            @SuppressWarnings("unused")
            final URI uri = new URI(connInfo.getProtocol(), connInfo.getUser(), connInfo.getHost(), connInfo.getPort(),
                null, null, null);
        } catch (final URISyntaxException e) {
            throw new InvalidSettingsException("Unable to generate connection URI: " + e.getMessage(), e);
        }

        return new PortObjectSpec[]{spec};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        testConnection();
        return super.execute(inObjects, exec);
    }

    /**
     * Perform a simple exists operation to validate that we can connect with given settings.
     *
     * @throws InvalidSettingsException on invalid connection settings
     */
    private void testConnection() throws Exception {
        final ConnectionMonitor<DBFSConnection> monitor = new ConnectionMonitor<>();
        try {
            final ConnectionInformation connectionInformation = createSpec().getConnectionInformation();
            final URI uri = connectionInformation.toURI().resolve("/");
            RemoteFile<DBFSConnection> remoteFile =
                RemoteFileFactory.createRemoteFile(uri, connectionInformation, monitor);
            remoteFile.exists();
        } finally {
            monitor.closeAll();
        }
    }
}
