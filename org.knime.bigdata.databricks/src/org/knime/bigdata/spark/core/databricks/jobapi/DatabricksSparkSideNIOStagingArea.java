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
 *   Created on Jul 2, 2018 by bjoern
 */
package org.knime.bigdata.spark.core.databricks.jobapi;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark-side utility class to access the staging area via the Java NIO API.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SparkClass
public class DatabricksSparkSideNIOStagingArea implements DatabricksSparkSideStagingAreaProvider {

    private static final Logger LOG = Logger.getLogger(DatabricksSparkSideNIOStagingArea.class);

    private static final Pattern STAGING_FILENAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    private final Path m_stagingArea;

    private final Path m_localTmpDir;

    DatabricksSparkSideNIOStagingArea(final String stagingArea, final File localTmpDir) {
        m_stagingArea = Paths.get(stagingArea);
        m_localTmpDir = localTmpDir.toPath();

        LOG.info("Using Java NIO based staging area for KNIME Spark jobs at " + m_stagingArea.toString());
        LOG.info("Local temp file directory for KNIME Spark jobs is at " + localTmpDir.getAbsolutePath());
    }

    /**
     * Downloads the contents of the given input stream to a new local file. The input stream will always be closed. It
     * is up the caller to clean up the temp file when it is not needed anymore.
     *
     * @param in The input stream to read from.
     * @return a new created local file that contains the contents of the input stream.
     * @throws IOException
     */
    @Override
    public Path downloadToFile(final InputStream in) throws IOException {
        final Path outFile = m_localTmpDir.resolve(UUID.randomUUID().toString().replace("-", ""));

        try {
            Files.copy(in, outFile);
        } finally {
            in.close();
        }
        return outFile;
    }

    @Override
    @SuppressWarnings("resource")
    public InputStream newDownloadStream(final String stagingFileName) throws IOException {
        validateStagingFileName(stagingFileName);

        return Files.newInputStream(m_stagingArea.resolve(stagingFileName));
    }

    public String uploadFromFile(final File localFile) throws IOException {
        try (final InputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            return uploadFromStream(in);
        }
    }

    @Override
    public String uploadAdditionalFile(final File fileToUpload, final String stagingFilename) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Entry<String, OutputStream> newUploadStream() throws IOException {
        final String stagingFileName = UUID.randomUUID().toString().replace("-", "");
        validateStagingFileName(stagingFileName);
        return newUploadStream(stagingFileName);
    }

    @Override
    @SuppressWarnings("resource")
    public Entry<String, OutputStream> newUploadStream(final String stagingFileName) throws IOException {
        LOG.info("Creating newUploadStream with " + stagingFileName + " via Java NIO API");
        final OutputStream out = Files.newOutputStream(m_stagingArea.resolve(stagingFileName));

        return new Entry<String, OutputStream>() {
            @Override
            public String getKey() {
                return stagingFileName;
            }

            @Override
            public OutputStream getValue() {
                return out;
            }

            @Override
            public OutputStream setValue(final OutputStream value) {
                throw new RuntimeException("setValue not supported");
            }
        };
    }

    public String uploadFromStream(final InputStream in) throws IOException {
        final Entry<String, OutputStream> outEntry = newUploadStream();

        final byte[] buffer = new byte[8192];
        try (final OutputStream out = outEntry.getValue()) {
            int read;
            while ((read = in.read(buffer)) >= 0) {
                out.write(buffer, 0, read);
            }
        }

        return outEntry.getKey();
    }

    @Override
    public void deleteSafely(final String stagingFilename) {
        try {
            delete(stagingFilename);
        } catch (IOException e) {
            LOG.warn(
                String.format("Failed to delete staging file %s (Reason: %s)", stagingFilename, e.getMessage()));
        }
    }

    @SuppressWarnings("resource")
    public void delete(final String stagingFileName) throws IOException {
        validateStagingFileName(stagingFileName);

        final Path stagingFile = m_stagingArea.resolve(stagingFileName);
        if (!Files.deleteIfExists(stagingFile)) {
            throw new IOException("Failed to delete staging file at " + stagingFile.toUri().toString());
        }
    }

    private void validateStagingFileName(final String stagingFileName) {
        if (!STAGING_FILENAME_PATTERN.matcher(stagingFileName).matches()) {
            throw new IllegalArgumentException("Illegal name for staging file: " + stagingFileName);
        }
    }

    @Override
    @SuppressWarnings("resource")
    public void cleanUp() {
        LOG.info("Cleaning up staging area for KNIME Spark jobs");
        try {
            deleteRecursively(m_stagingArea);
        } catch (IOException e) {
            LOG.error("Error while deleting staging area for KNIME Spark jobs: " + e.getMessage(), e);
        }

        LOG.info("Cleaning up local temp file directory for KNIME Spark jobs");
        try {
            if (Files.exists(m_localTmpDir)) {
                deleteRecursively(m_localTmpDir);
            }
        } catch (IOException e) {
            LOG.error("Error while deleting local temp file directory of KNIME Spark jobs: " + e.getMessage(), e);
        }
    }

    private static void deleteRecursively(final Path toDelete) throws IOException {
        try (final Stream<Path> walk = Files.walk(toDelete)) {
            final List<Path> filesToDelete = walk.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path fileToDelete : filesToDelete) {
                Files.deleteIfExists(fileToDelete);
            }
        }
    }

    @Override
    public URI getDistributedTempDirURI() {
        return m_stagingArea.toUri();
    }
}
