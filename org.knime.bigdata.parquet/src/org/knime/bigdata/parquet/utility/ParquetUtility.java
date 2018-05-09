package org.knime.bigdata.parquet.utility;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.FileRemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.NodeLogger;

/**
 * Parquet Utility that handles file creation
 *
 * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
 */
public class ParquetUtility {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(ParquetUtility.class);

    /**
     * Creates a remote directory with the given Name.
     *
     * @param connObj the connection information
     * @param fileName the name
     * @param overwrite whether the file should be overwritten
     * @return a remote file, representing the directory
     * @throws Exception If file can not be created.
     */

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public RemoteFile<Connection> createRemoteDir(final ConnectionInformationPortObject connObj, String fileName,
            boolean overwrite) throws Exception {

        final ConnectionMonitor<?> connMonitor = new ConnectionMonitor<>();
        final ConnectionInformation connInfo = connObj.getConnectionInformation();
        final URI targetURI = new URI(connInfo.toURI().toString() + fileName + "/");
        LOGGER.info("Creating remote File " + targetURI + " from name " + fileName);
        final RemoteFile remoteDir = RemoteFileFactory.createRemoteFile(targetURI, connInfo, connMonitor);
        checkOverwrite(remoteDir, overwrite);
        return remoteDir;
    }

    /**
     * Creates a remote file with the given Name.
     *
     * @param connObj the connection information
     * @param fileName the name
     * @param overwrite whether the file should be overwritten
     * @return a remote file, representing the directory
     * @throws Exception If file can not be created.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public RemoteFile<Connection> createRemoteFile(final ConnectionInformationPortObject connObj, String fileName,
            boolean overwrite) throws Exception {
        RemoteFile remoteFile;
        if (connObj == null) {
            remoteFile = createLocalFile(fileName, overwrite);
        } else {
            final ConnectionMonitor<?> connMonitor = new ConnectionMonitor<>();
            final ConnectionInformation connInfo = connObj.getConnectionInformation();
            final URI targetURI = new URI(connInfo.toURI().toString() + fileName);
            remoteFile = RemoteFileFactory.createRemoteFile(targetURI, connInfo, connMonitor);
        }

        checkOverwrite(remoteFile, overwrite);
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
    public RemoteFile<Connection> createLocalFile(String fileName, boolean overwrite) throws Exception {
        RemoteFile<Connection> remoteFile;
        // No remote connection. Create Local File
        URI fileuri = new URI(fileName);
        if (fileuri.getScheme() == null) {

            // Use File protocol as default
            fileuri = new URI("file://" + fileName);
        }

        remoteFile = RemoteFileFactory.createRemoteFile(fileuri, null, null);
        LOGGER.debug(String.format("Creating local File %s.", remoteFile.getFullName()));
        checkOverwrite(remoteFile, overwrite);
        return remoteFile;
    }

    /**
     * Checks if file exists. If the file exists and it should be overwritten
     * tries to remove it.
     *
     * @param remoteFile the remote file to check.
     * @param overwrite whether the file should be overwritten
     * @throws Exception if the file exists, and should not be overwritten. Or
     *         if the file can not be deleted.
     */
    public void checkOverwrite(RemoteFile<Connection> remoteFile, boolean overwrite) throws Exception {
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
     * Creates a temporary file with a counter suffix in the directory given in
     * path.
     *
     * @param path file that represents the target directory
     * @param filecount the counter for the suffix
     * @return a {@link FileRemoteFile} for the temporary file
     * @throws Exception if file creation fails
     */
    public RemoteFile<Connection> createTempFile(File path, int filecount) throws Exception {
        final String filepath = "file://" + path + "/" + UUID.randomUUID().toString().replace('-', '_');
        final URI tempfileuri = new URI(String.format("%s_%05d", filepath, filecount));
        return RemoteFileFactory.createRemoteFile(tempfileuri, null, null);
    }

}
