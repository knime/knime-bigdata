/*
 * ------------------------------------------------------------------------
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 *
 */

package org.knime.bigdata.orc.utility;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.FileRemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.NodeLogger;

/**
 * ORC Utility for the ORC-Reader and -Writer nodes.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcUtility {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(OrcUtility.class);

    /**
     * Creates a remote file for the given file name.
     *
     * @param connInfo the connection information
     * @param fileName the name
     * @return a remote file, representing the directory
     * @throws Exception If file can not be created.
     */
    public RemoteFile<Connection> createRemoteFile(String fileName, final ConnectionInformation connInfo)
            throws Exception {
        final ConnectionMonitor<Connection> connectionMonitor = new ConnectionMonitor<>();
        RemoteFile<Connection> remoteFile;
        if (connInfo == null) {
            remoteFile = createLocalFile(fileName);
        } else {

            remoteFile = RemoteFileFactory.createRemoteFile(new URI(connInfo.toURI().toString() + fileName), connInfo,
                    connectionMonitor);
        }
        return remoteFile;
    }

    /**
     * Creates a local file using the {@link FileRemoteFile} with the given
     * Name.
     *
     * @param fileName the name
     * @param overwrite whether the file should be overwritten
     * @return a remote file, representing the directory
     * @throws Exception thrown if file exists and should not be overwritten
     * @throws URISyntaxException if the filenName is not valid
     */
    public RemoteFile<Connection> createLocalFile(String fileName) throws Exception {
        RemoteFile<Connection> remoteFile;
        // No remote connection. Create Local File
        URI fileuri = new URI(fileName);
        if (fileuri.getScheme() == null) {

            // Use File protocol as default
            final File file = new File(fileName);
            fileuri = file.toURI();
        }

        remoteFile = RemoteFileFactory.createRemoteFile(fileuri, null, null);
        LOGGER.debug(String.format("Creating local File %s.", remoteFile.getFullName()));
        return remoteFile;
    }
}
