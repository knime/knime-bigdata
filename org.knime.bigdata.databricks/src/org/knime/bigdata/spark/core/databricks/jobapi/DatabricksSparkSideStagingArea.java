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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksJobSerializationUtils.StagingAreaAccess;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark-side utility class to access to staging area.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class DatabricksSparkSideStagingArea implements StagingAreaAccess {

    public static final DatabricksSparkSideStagingArea SINGLETON_INSTANCE = new DatabricksSparkSideStagingArea();

    private static final Logger LOG = Logger.getLogger(DatabricksSparkSideStagingArea.class);

    private static final Pattern STAGING_FILENAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    private volatile URI stagingAreaURI;

    private volatile Configuration hadoopConf;

    private volatile java.nio.file.Path localTmpDir;

    public synchronized static void ensureInitialized(final String stagingArea, final boolean stagingAreaIsPath,
        final File localTmpDir, final Configuration hadoopConf) throws URISyntaxException {

        if (SINGLETON_INSTANCE.stagingAreaURI == null) {
            if (stagingAreaIsPath) {
                SINGLETON_INSTANCE.stagingAreaURI = FileSystem.getDefaultUri(new Configuration()).resolve(stagingArea);
            } else {
                SINGLETON_INSTANCE.stagingAreaURI = URI.create(stagingArea);
            }
            SINGLETON_INSTANCE.hadoopConf = hadoopConf;
            SINGLETON_INSTANCE.localTmpDir = localTmpDir.toPath();

            LOG.info("Staging area for KNIME Spark jobs is at " + SINGLETON_INSTANCE.stagingAreaURI.toString());
            LOG.info("Local temp file directory for KNIME Spark jobs is at " + localTmpDir.getAbsolutePath());
        }
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
    public java.nio.file.Path downloadToFile(final InputStream in) throws IOException {
        final java.nio.file.Path outFile = Paths.get(localTmpDir.toString(), UUID.randomUUID().toString().replaceAll("-", ""));

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

        final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);
        return fs.open(new Path(new Path(stagingAreaURI), stagingFileName));
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
        final String stagingFileName = UUID.randomUUID().toString().replaceAll("-", "");
        validateStagingFileName(stagingFileName);
        return newUploadStream(stagingFileName);
    }

    @Override
    @SuppressWarnings("resource")
    public Entry<String, OutputStream> newUploadStream(final String stagingFileName) throws IOException {
        final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);

        final Path copyDestiation = new Path(new Path(stagingAreaURI), stagingFileName);
        final OutputStream out = fs.create(copyDestiation);

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

        final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);
        final Path stagingFile = new Path(new Path(stagingAreaURI), stagingFileName);
        if (!fs.delete(stagingFile, false)) {
            throw new IOException("Failed to delete staging file at " + stagingFile.toUri().toString());
        }
    }

    private void validateStagingFileName(final String stagingFileName) {
        if (!STAGING_FILENAME_PATTERN.matcher(stagingFileName).matches()) {
            throw new IllegalArgumentException("Illegal name for staging file: " + stagingFileName);
        }
    }

    @SuppressWarnings("resource")
    public void cleanUp() {
        LOG.info("Cleaning up staging area for KNIME Spark jobs");
        try {
            final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);

            // when the URI ends with a slash, then only the directory contents but not the directory itself will be deleted
            fs.delete(new Path(URI.create(stagingAreaURI.toString().replaceFirst("/$", ""))), true);
        } catch (IOException e) {
            LOG.error("Error while deleting staging area for KNIME Spark jobs: " + e.getMessage(), e);
        }

        LOG.info("Cleaning up local temp file directory for KNIME Spark jobs");
        try {
            if (Files.exists(localTmpDir)) {
                deleteRecursively(localTmpDir);
            }
        } catch (IOException e) {
            LOG.error("Error while deleting local temp file directory of KNIME Spark jobs: " + e.getMessage(), e);
        }
    }

    private static void deleteRecursively(final java.nio.file.Path toDelete) throws IOException {
        final List<java.nio.file.Path> filesToDelete =
            Files.walk(toDelete).sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        for (java.nio.file.Path fileToDelete : filesToDelete) {
            Files.deleteIfExists(fileToDelete);
        }
    }
}
