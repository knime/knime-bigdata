package org.knime.bigdata.spark.core.livy.context;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.filehandling.local.HDFSLocalRemoteFileHandler;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFile;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobSerializationUtils.StagingAreaAccess;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;

/**
 * KNIME-side implementation of {@link StagingAreaAccess} that accesses the staging area through the KNIME remote file
 * handling API.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class RemoteFSController implements StagingAreaAccess {

    private static final NodeLogger LOG = NodeLogger.getLogger(RemoteFSController.class);

    private final ConnectionInformation m_connectionInformation;

    private final String m_stagingAreaParent;

    private ConnectionMonitor<?> m_connectionMonitor;

    private boolean m_stagingAreaIsPath;
    
    /**
     * Path of the staging area folder. The path always ends with a "/".
     */
    private String m_stagingAreaPath;

    /**
     * 
     * @param connectionInformation
     * @param stagingAreaParent
     */
    public RemoteFSController(ConnectionInformation connectionInformation, final String stagingAreaParent) {
        m_connectionInformation = connectionInformation;
        m_stagingAreaParent = stagingAreaParent;
    }

    /**
     * Tries to create the staging area, which is typically a folder in the remote file system provided in the
     * constructor. This method may try to create the folder in several locations before it fails.
     * 
     * @throws KNIMESparkException If no staging area could be created, this exception wraps the original exception
     *             thrown during the last attempt to create the staging area folder.
     */
    public void createStagingArea() throws KNIMESparkException {
        m_connectionMonitor = new ConnectionMonitor<>();
        boolean success = false;
        Exception lastException = null;

        try {
            for (String stagingAreaCandidate : generateStagingAreaCandidates()) {
                try {
                    tryCreateStagingArea(stagingAreaCandidate);
                    success = true;
                    m_stagingAreaPath = stagingAreaCandidate;
                    break;
                } catch (Exception e) {
                    lastException = e;
                    LOG.debug("Failed to create staging area with error message: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            lastException = e;
        }

        if (!success) {
            m_connectionMonitor.closeAll();
            m_connectionMonitor = null;
            throw new KNIMESparkException(lastException);
        }

        m_stagingAreaIsPath = stagingAreaIsPath(m_connectionInformation);
    }

    private static boolean stagingAreaIsPath(final ConnectionInformation connInfo) {
        if (HDFSRemoteFileHandler.isSupportedConnection(connInfo)
            || HDFSLocalRemoteFileHandler.isSupportedConnection(connInfo)) {
            return true;
        } else if (connInfo instanceof CloudConnectionInformation) {
            // FIXME: this test does not always work, e.g. if the ingoing port object was persisted
            return false;
        } else {
            throw new IllegalArgumentException("Unsupported remote file system: " + connInfo.getProtocol());
        }
    }

    private List<String> generateStagingAreaCandidates() throws Exception {
        if (HDFSRemoteFileHandler.isSupportedConnection(m_connectionInformation)) {
            return generateStagingAreaCandidatesForHDFS();
        } else if (HDFSLocalRemoteFileHandler.isSupportedConnection(m_connectionInformation)) {
            return generateStagingAreaCandidatesForLocalHDFS();
        } else if (m_connectionInformation instanceof CloudConnectionInformation) {
            // FIXME: this test does not always work, e.g. if the ingoing port object was persisted
            return generateCloudStagingAreaCandidates();
        } else {
            throw new IllegalArgumentException(
                "Unsupported remote file system: " + m_connectionInformation.getProtocol());
        }
    }

    private static List<String> generateStagingAreaCandidatesForLocalHDFS() {
        final String stagingDir = ".knime-spark-staging-" + UUID.randomUUID().toString();
        final String stagingDirParent = Paths.get(System.getProperty("java.io.tmpdir")).toUri().getPath();

        return Collections.singletonList(appendDirs(stagingDirParent, stagingDir));
    }

    private List<String> generateCloudStagingAreaCandidates() throws Exception {
        if (m_stagingAreaParent == null) {
            throw new IllegalArgumentException(
                String.format("When connecting to %s a staging directory must be specified (see Advanced tab).",
                    m_connectionInformation.getProtocol()));
        } else {
            final String stagingDir = "knime-spark-staging-" + UUID.randomUUID().toString();
            return Collections.singletonList(appendDirs(m_stagingAreaParent, stagingDir));
        }
    }

    private static String appendDirs(String parent, String childDir) {
        return String.format("%s%s%s/", parent, (parent.endsWith("/") ? "" : "/"), childDir);
    }

    @SuppressWarnings("resource")
    private List<String> generateStagingAreaCandidatesForHDFS() throws Exception {
        final FileSystem hadoopFs = getHadoopFS();
        final List<String> toReturn = new LinkedList<>();

        final String stagingDir = ".knime-spark-staging-" + UUID.randomUUID().toString();

        if (m_stagingAreaParent != null) {
            toReturn.add(appendDirs(m_stagingAreaParent, stagingDir));
        } else {
            final Path hdfsHome = hadoopFs.getHomeDirectory();
            if (hadoopFs.exists(hdfsHome)) {
                toReturn.add(appendDirs(hdfsHome.toUri().getPath(), stagingDir));
            }

            final Path hdfsTmp = new Path("/tmp");
            if (hadoopFs.exists(hdfsTmp)) {
                toReturn.add(appendDirs("/tmp", stagingDir));
            }

            if (toReturn.isEmpty()) {
                throw new IllegalArgumentException(
                    "Could not find suitable HDFS staging directory (neither user home nor /tmp exist).\n"
                        + "Please specify a staging directory in the node settings (see Advanced tab).");
            }
        }

        return toReturn;
    }

    private FileSystem getHadoopFS() throws Exception {
        final HDFSRemoteFile hdfsRemoteFile = (HDFSRemoteFile)RemoteFileFactory
            .createRemoteFile(m_connectionInformation.toURI(), m_connectionInformation, m_connectionMonitor);
        return hdfsRemoteFile.getConnection().getFileSystem();
    }

    private void tryCreateStagingArea(final String stagingArea) throws Exception {
        final URI stagingAreaURI =
            new URI(m_connectionInformation.toURI().toString() + NodeUtils.encodePath(stagingArea));

        LOG.debug("Trying to create staging area at: " + stagingAreaURI);

        final RemoteFile<? extends Connection> stagingAreaRemoteFile =
            RemoteFileFactory.createRemoteFile(stagingAreaURI, m_connectionInformation, m_connectionMonitor);

        stagingAreaRemoteFile.mkDirs(true);
        if (stagingAreaRemoteFile instanceof HDFSRemoteFile) {
            final HDFSRemoteFile hdfsStagingFolderRemoteFile = (HDFSRemoteFile)stagingAreaRemoteFile;
            hdfsStagingFolderRemoteFile.setPermission("-rwx------");
        }
    }

    private InputStream download(final String stagingFilename) throws Exception {
        final URI stagingFileURI = new URI(
            m_connectionInformation.toURI().toString() + NodeUtils.encodePath(m_stagingAreaPath + stagingFilename));

        final RemoteFile<? extends Connection> stagingAreaRemoteFile =
            RemoteFileFactory.createRemoteFile(stagingFileURI, m_connectionInformation, m_connectionMonitor);

        return new BufferedInputStream(stagingAreaRemoteFile.openInputStream());
    }

    /**
     * Closes the connection to the remote FS if necessary.
     */
    public void ensureClosed() {
        if (m_connectionMonitor != null) {
            // we don't actually delete the staging area here. This is done from inside Spark.
            m_connectionMonitor.closeAll();
            m_connectionMonitor = null;
        }
    }

    /**
     * Deletes the given staging file from the staging area in the remote FS.
     * 
     * @param stagingFilename
     * @throws Exception
     */
    public void delete(final String stagingFilename) throws Exception {
        final URI stagingFileURI = new URI(
            m_connectionInformation.toURI().toString() + NodeUtils.encodePath(m_stagingAreaPath + stagingFilename));

        final RemoteFile<? extends Connection> stagingAreaRemoteFile =
            RemoteFileFactory.createRemoteFile(stagingFileURI, m_connectionInformation, m_connectionMonitor);

        if (!stagingAreaRemoteFile.delete()) {
            throw new IOException("Failed to delete staging file at " + stagingFileURI.toString());
        }
    }

    /**
     * @return the path or full Hadoop-API URI of the staging area (see {@link #getStagingAreaReturnsPath()}.
     */
    public String getStagingArea() {
        return toDesiredFormat(m_stagingAreaPath);
    }

    private String toDesiredFormat(String path) {
        if (m_stagingAreaIsPath) {
            return path;
        } else {
            try {
                final URI stagingAreaURI =
                    new URI(m_connectionInformation.toURI().toString() + NodeUtils.encodePath(path));

                final CloudRemoteFile<?> stagingAreaRemoteFile = (CloudRemoteFile<?>)RemoteFileFactory
                    .createRemoteFile(stagingAreaURI, m_connectionInformation, m_connectionMonitor);

                return stagingAreaRemoteFile.getHadoopFilesystemURI().toString().toString();
            } catch (Exception e) {
                // should never happen
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @return when true, then the value returned by {@link #getStagingArea()} is a path, otherwise it is a full URI.
     */
    public boolean getStagingAreaReturnsPath() {
        return m_stagingAreaIsPath;
    }

    @SuppressWarnings("resource")
    @Override
    public Entry<String, OutputStream> newUploadStream() throws IOException {
        final String stagingFilename = UUID.randomUUID().toString();

        try {
            final URI stagingFileURI = new URI(
                m_connectionInformation.toURI().toString() + NodeUtils.encodePath(m_stagingAreaPath + stagingFilename));
    
            @SuppressWarnings("unchecked")
            final RemoteFile<Connection> stagingAreaRemoteFile = (RemoteFile<Connection>)RemoteFileFactory
                .createRemoteFile(stagingFileURI, m_connectionInformation, m_connectionMonitor);
            
            OutputStream out;
            try {
                out = stagingAreaRemoteFile.openOutputStream();
            } catch (UnsupportedOperationException e) {
                out = newTempFileBufferedUpload(stagingAreaRemoteFile);
            }
            
            return new Pair(stagingFilename, out);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
    @SuppressWarnings("resource")
    private static OutputStream newTempFileBufferedUpload(RemoteFile<Connection> stagingAreaRemoteFile)
        throws IOException {

        final java.nio.file.Path localTempFile = FileUtil.createTempFile("livy_upload", null).toPath();
        final OutputStream stream = Files.newOutputStream(localTempFile, StandardOpenOption.TRUNCATE_EXISTING);

        return new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                stream.write(b);
            }

            @Override
            public void write(final byte[] b) throws IOException {
                stream.write(b);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                stream.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                stream.flush();
            }

            @Override
            public void close() throws IOException {
                try {
                    stream.close();
                    final RemoteFile<Connection> localFile =
                        RemoteFileFactory.createRemoteFile(localTempFile.toUri(), null, null);
                    stagingAreaRemoteFile.write(localFile, null);
                } catch (final Exception ex) {
                    throw new IOException(ex.getMessage());
                } finally {
                    Files.delete(localTempFile);
                }
            }
        };
    }

    /**
     * Internal pair class to hold upload information.
     */
    private static class Pair implements Entry<String, OutputStream> {

        private final String m_stagingFilename;

        private final OutputStream m_out;

        Pair(String stagingFilename, OutputStream out) {
            m_stagingFilename = stagingFilename;
            m_out = out;
        }

        @Override
        public String getKey() {
            return m_stagingFilename;
        }

        @Override
        public OutputStream getValue() {
            return m_out;
        }

        @Override
        public OutputStream setValue(OutputStream value) {
            throw new RuntimeException("setValue not supported");
        }
    }

    @Override
    public InputStream newDownloadStream(String stagingFilename) throws IOException {
        try {
            return download(stagingFilename);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public java.nio.file.Path downloadToFile(InputStream in) throws IOException {
        final java.nio.file.Path toReturn = FileUtil.createTempFile("spark", null, false).toPath();
        try {
            Files.copy(in, toReturn, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            in.close();
        }

        return toReturn;
    }

    @Override
    public void deleteSafely(String stagingFilename) {
        try {
            delete(stagingFilename);
        } catch (Exception e) {
            LOG.warn(String.format("Failed to delete staging file %s (Reason: %s)", stagingFilename, e.getMessage()));
        }
    }
}
