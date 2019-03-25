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
 * History 28.05.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.utility;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.FileRemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.NodeLogger;

/**
 * Utility that handles file creation.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileHandlingUtility {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileHandlingUtility.class);

    private FileHandlingUtility() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Creates a remote file for the given file name.
     *
     * @param connInfo the connection information
     * @param fileName the name
     * @param connectionMonitor
     * @return a remote file, representing the directory
     * @throws Exception If file can not be created.
     */
    public static RemoteFile<Connection> createRemoteFile(final String fileName, final ConnectionInformation connInfo,
        final ConnectionMonitor<Connection> connectionMonitor) throws Exception {

        RemoteFile<Connection> remoteFile;
        if (connInfo == null || connInfo.getProtocol().equalsIgnoreCase("hdfs-local")) {
            remoteFile = createLocalFile(fileName);
        } else {

            remoteFile = RemoteFileFactory.createRemoteFile(new URI(connInfo.toURI().toString() + fileName), connInfo,
                connectionMonitor);
        }
        return remoteFile;
    }

    /**
     * Creates a local file using the {@link FileRemoteFile} with the given Name.
     *
     * @param fileName the name
     * @return a remote file, representing the directory
     * @throws Exception thrown if file exists and should not be overwritten
     * @throws URISyntaxException if the filenName is not valid
     */
    public static RemoteFile<Connection> createLocalFile(final String fileName) throws Exception {
        RemoteFile<Connection> remoteFile;
        // No remote connection. Create Local File
        URI fileuri = new URI(fileName.replace(" ", "%20"));
        if (fileuri.getScheme() == null) {

            // Use File protocol as default
            final File file = new File(fileName);
            fileuri = file.toURI();
        }

        remoteFile = RemoteFileFactory.createRemoteFile(fileuri, null, null);
        LOGGER.debug(String.format("Creating local File %s.", remoteFile.getFullName()));
        return remoteFile;
    }

    /**
     * Creates a remote directory with the given Name.
     *
     * @param connObj the connection information
     * @param connMonitor
     * @param fileName the name
     * @return a remote file, representing the directory
     * @throws Exception If file can not be created.
     */

    @SuppressWarnings({"unchecked"})
    public static RemoteFile<Connection> createRemoteDir(final ConnectionInformationPortObject connObj,
        final ConnectionMonitor<?> connMonitor, final String fileName) throws Exception {

        final ConnectionInformation connInfo = connObj.getConnectionInformation();
        final URI targetURI = new URI(connInfo.toURI().toString() + NodeUtils.encodePath(fileName) + "/");
        LOGGER.info("Creating remote File " + targetURI + " from name " + fileName);
        return (RemoteFile<Connection>)RemoteFileFactory.createRemoteFile(targetURI, connInfo, connMonitor);
    }

    /**
     * Checks if file exists. If the file exists and it should be overwritten tries to remove it.
     *
     * @param remoteFile the remote file to check.
     * @param overwrite whether the file should be overwritten
     * @throws Exception if the file exists, and should not be overwritten. Or if the file can not be deleted.
     */
    public static void checkOverwrite(final RemoteFile<Connection> remoteFile, final boolean overwrite)
        throws Exception {
        if (remoteFile.exists()) {
            if (overwrite) {
                LOGGER.debug(String.format("File %s already exists. Deleting it.", remoteFile.getFullName()));
                if (!remoteFile.delete()) {
                    throw new IOException(
                        String.format("File %s already exists and could not be deleted.", remoteFile.getPath()));
                }
            } else {
                throw new IOException(String.format("File %s already exists.", remoteFile.getPath()));
            }
        }
    }

    /**
     * Creates a temporary file with a counter suffix in the directory given in path.
     *
     * @param path file that represents the target directory
     * @param filecount the counter for the suffix
     * @return a {@link FileRemoteFile} for the temporary file
     * @throws Exception if file creation fails
     */
    public static RemoteFile<Connection> createTempFile(final File path, final int filecount) throws Exception {
        final String fileNameEnding =
            String.format("%s_%05d", UUID.randomUUID().toString().replace('-', '_'), filecount);
        final File file = new File(path, fileNameEnding);
        return RemoteFileFactory.createRemoteFile(file.toURI(), null, null);
    }

}
