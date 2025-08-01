/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
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
@Deprecated
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
