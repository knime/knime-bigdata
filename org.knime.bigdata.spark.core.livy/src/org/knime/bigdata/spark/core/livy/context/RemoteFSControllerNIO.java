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
 *
 * History
 *   Nov 12, 2020 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.spark.core.livy.context;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.core.runtime.URIUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobSerializationUtils.StagingAreaAccess;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterConfig;
import org.knime.filehandling.core.connections.uriexport.URIExporterFactory;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.noconfig.NoConfigURIExporterFactory;

/**
 * KNIME-side implementation of {@link StagingAreaAccess} that accesses the staging area using a {@code FSConnection}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class RemoteFSControllerNIO implements RemoteFSController {

    private static final NodeLogger LOG = NodeLogger.getLogger(RemoteFSControllerNIO.class);

    private static final Set<PosixFilePermission> STAGING_AREA_PERMISSIONS =
        PosixFilePermissions.fromString("rwx------");

    private final FSConnection m_fsConnection;

    private final FSPath m_stagingAreaParent;

    private FSPath m_stagingArea;

    private String m_stagingAreaString;

    private boolean m_stagingAreaIsPath;

    /**
     * Constructor with staging area parent path.
     *
     * @param fsConnection file system to use for staging area
     * @param stagingAreaParent parent directory of staging area, or {@code null} if not set
     * @throws KNIMESparkException if file system is not a local file system and the staging area parent is not set
     */
    public RemoteFSControllerNIO(final FSConnection fsConnection, final String stagingAreaParent) throws KNIMESparkException {
        m_fsConnection = fsConnection;

        m_stagingAreaParent = detectStagingAreaParentPath(fsConnection, stagingAreaParent);
    }

    @SuppressWarnings("resource")
    private static FSPath detectStagingAreaParentPath(final FSConnection fsConnection, final String stagingAreaParent)
        throws KNIMESparkException {

        // check the configured staging area if set
        if (!StringUtils.isBlank(stagingAreaParent)) {
            final FSPath candidate = fsConnection.getFileSystem().getPath(stagingAreaParent);
            if (Files.isDirectory(candidate)) {
                return candidate;
            } else {
                throw new KNIMESparkException("Configured staging area directory does not exist or is not a directory (see Advanced tab).");
            }
        }

        // check tmp directory
        final FSPath candidate = fsConnection.getFileSystem().getPath("/tmp");
        if (Files.isDirectory(candidate)) {
            return candidate;
        }

        throw new KNIMESparkException(
            "Unable to setup staging area, please specify an existing directory in the Advanced tab.");
    }

    @SuppressWarnings("resource")
    @Override
    public void createStagingArea() throws KNIMESparkException {
        try {
            final String stagingDirPrefix = "knime-spark-staging-";
            m_stagingArea = FSFiles.createTempDirectory(m_stagingAreaParent, stagingDirPrefix, "");
            
            if (m_fsConnection.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                Files.setPosixFilePermissions(m_stagingArea, STAGING_AREA_PERMISSIONS);
            }
            final URI uri = convertPathToURI(m_stagingArea);
            m_stagingAreaString = URIUtil.toUnencodedString(uri);
            m_stagingAreaIsPath = uri.getScheme() == null;
        } catch (final IOException|URISyntaxException e) {
            throw new KNIMESparkException("Failed to create staging area: " + e.getMessage(), e);
        }
    }
    
    private URI convertPathToURI(final FSPath fsPath) throws URISyntaxException {
        final NoConfigURIExporterFactory uriExporterFactory =
            (NoConfigURIExporterFactory)m_fsConnection.getURIExporterFactory(URIExporterIDs.DEFAULT_HADOOP);
        final URIExporter uriExporter = uriExporterFactory.getExporter();
        final URI uri = uriExporter.toUri(fsPath);
        return uri;
    }

    @Override
    public String getStagingArea() {
        return m_stagingAreaString;
    }

    @Override
    public boolean getStagingAreaReturnsPath() {
        return m_stagingAreaIsPath;
    }

    @Override
    @SuppressWarnings("resource")
    public Entry<String, OutputStream> newUploadStream() throws IOException {
        final String stagingFilename = UUID.randomUUID().toString();
        return new AbstractMap.SimpleImmutableEntry<>( //
                stagingFilename, //
                Files.newOutputStream(m_stagingArea.resolve(stagingFilename), StandardOpenOption.CREATE_NEW));
    }

    @Override
    public InputStream newDownloadStream(final String stagingFilename) throws IOException {
        return Files.newInputStream(m_stagingArea.resolve(stagingFilename));
    }

    @Override
    public Path downloadToFile(final InputStream in) throws IOException {
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
            Files.delete(m_stagingArea.resolve(stagingFilename));
        } catch (Exception e) {
            LOG.warn(String.format("Failed to delete staging file %s (Reason: %s)", stagingFilename, e.getMessage()));
        }
    }

    @Override
    public void ensureClosed() {
        // nothing to do here, file system connection gets closed in the connector node model
    }

}
