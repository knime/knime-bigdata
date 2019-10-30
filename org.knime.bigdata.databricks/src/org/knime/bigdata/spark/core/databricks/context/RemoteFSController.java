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
package org.knime.bigdata.spark.core.databricks.context;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.dbfs.filehandler.DBFSRemoteFileHandler;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFile;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksJobSerializationUtils.StagingAreaAccess;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;

/**
 * KNIME-side implementation of {@link StagingAreaAccess} that accesses the staging area in a Databricks workspace
 * through the KNIME remote file handling API.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH
 */
public class RemoteFSController implements StagingAreaAccess {

    private static final NodeLogger LOG = NodeLogger.getLogger(RemoteFSController.class);

    private final ConnectionInformation m_connectionInformation;

    private final String m_stagingAreaParent;

    private final String m_clusterID;

    private ConnectionMonitor<?> m_connectionMonitor;

    private boolean m_stagingAreaIsPath;

    /**
     * Path of the staging area folder. The path always ends with a "/".
     */
    private String m_stagingAreaPath;

    /**
     * Default constructor.
     * @param connectionInformation
     * @param stagingAreaParent
     * @param clusterID
     */
    public RemoteFSController(final ConnectionInformation connectionInformation, final String stagingAreaParent,
        final String clusterID) {
        m_connectionInformation = connectionInformation;
        m_stagingAreaParent = stagingAreaParent;
        m_clusterID = clusterID;
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
        if (DBFSRemoteFileHandler.isSupportedConnection(connInfo)) {
            return true;
        } else if (connInfo instanceof CloudConnectionInformation) {
            // FIXME: this test does not always work, e.g. if the ingoing port object was persisted
            return false;
        } else {
            throw new IllegalArgumentException("Unsupported remote file system: " + connInfo.getProtocol());
        }
    }

    private List<String> generateStagingAreaCandidates() throws Exception {
        if (DBFSRemoteFileHandler.isSupportedConnection(m_connectionInformation)) {
            return generateStagingAreaCandidatesForDBFS();
        } else if (m_connectionInformation instanceof CloudConnectionInformation) {
            // FIXME: this test does not always work, e.g. if the ingoing port object was persisted
            return generateCloudStagingAreaCandidates();
        } else {
            throw new IllegalArgumentException(
                "Unsupported remote file system: " + m_connectionInformation.getProtocol());
        }
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

    private static String appendDirs(final String parent, final String childDir) {
        return String.format("%s%s%s/", parent, (parent.endsWith("/") ? "" : "/"), childDir);
    }

    private List<String> generateStagingAreaCandidatesForDBFS() {
        final String stagingDir = ".knime-spark-staging-" + m_clusterID;

        if (m_stagingAreaParent != null) {
            return Collections.singletonList(appendDirs(m_stagingAreaParent, stagingDir));
        } else {
            return Collections.singletonList(appendDirs("/tmp", stagingDir));
        }
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
     * @return the full Hadoop-API URI of the staging area (see {@link #getStagingAreaReturnsPath()}.
     */
    public String getStagingArea() {
        return toDesiredFormat(m_stagingAreaPath);
    }

    private String toDesiredFormat(final String path) {
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

    @Override
    public Entry<String, OutputStream> newUploadStream() throws IOException {
        return newUploadStream(UUID.randomUUID().toString());
    }

    @Override
    @SuppressWarnings("resource")
    public Entry<String, OutputStream> newUploadStream(final String stagingFilename) throws IOException {
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

    @Override
    public String uploadAdditionalFile(final File fileToUpload, final String stagingFilename) throws IOException {
        try {
            Entry<String, OutputStream> uploadStream = newUploadStream(stagingFilename);
            try (OutputStream out = uploadStream.getValue()) {
                Files.copy(fileToUpload.toPath(), out);
            }
            return toDesiredFormat(NodeUtils.encodePath(m_stagingAreaPath + stagingFilename));
        } catch (final URISyntaxException e) {
            throw new IOException("Unable to encode staging file URI", e);
        }
    }

    @SuppressWarnings("resource")
    private static OutputStream newTempFileBufferedUpload(final RemoteFile<Connection> stagingAreaRemoteFile)
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

        Pair(final String stagingFilename, final OutputStream out) {
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
        public OutputStream setValue(final OutputStream value) {
            throw new RuntimeException("setValue not supported");
        }
    }

    @Override
    public InputStream newDownloadStream(final String stagingFilename) throws IOException {
        try {
            return download(stagingFilename);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public java.nio.file.Path downloadToFile(final InputStream in) throws IOException {
        final java.nio.file.Path toReturn = FileUtil.createTempFile("spark", null, false).toPath();
        try {
            Files.copy(in, toReturn, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            in.close();
        }

        return toReturn;
    }

    @Override
    public void deleteSafely(final String stagingFilename) {
        try {
            delete(stagingFilename);
        } catch (Exception e) {
            LOG.warn(String.format("Failed to delete staging file %s (Reason: %s)", stagingFilename, e.getMessage()));
        }
    }
}
