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
package org.knime.bigdata.spark.core.livy.jobapi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark-side utility class to access to staging area.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class StagingArea {

    private static final Logger LOG = Logger.getLogger(StagingArea.class);

    private static volatile URI stagingAreaURI;

    private static volatile Configuration hadoopConf;

    private static volatile File localTmpDir;

    private static final Pattern STAGING_FILENAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    public synchronized static void ensureInitialized(final String stagingArea, final boolean stagingAreaIsPath,
        final File localTmpDir, final Configuration hadoopConf) throws URISyntaxException {

        if (stagingAreaURI == null) {
            if (stagingAreaIsPath) {
                stagingAreaURI = FileSystem.getDefaultUri(new Configuration()).resolve(stagingArea);
            } else {
                stagingAreaURI = URI.create(stagingArea);
            }
            StagingArea.hadoopConf = hadoopConf;
            StagingArea.localTmpDir = localTmpDir;
            LOG.info("Staging area for KNIME Spark jobs is at " + stagingAreaURI.toString());
            LOG.info("Local temp file directory for KNIME Spark jobs is at " + localTmpDir.getAbsolutePath());
        }
    }

    public static File downloadToFileCached(final String stagingFileName) throws IOException {
        final File outFile = new File(localTmpDir, stagingFileName);
        if (outFile.exists()) {
            return outFile;
        }

        try (final OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))) {
            try (final InputStream in = createDownloadStream(stagingFileName)) {
                final byte[] buffer = new byte[8192];
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, read);
                }
            }
        }
        return outFile;
    }

    @SuppressWarnings("resource")
    public static InputStream createDownloadStream(final String stagingFileName) throws IOException {
        validateStagingFileName(stagingFileName);

        final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);
        final Path copySource = new Path(new Path(stagingAreaURI), stagingFileName);
        return new GZIPInputStream(fs.open(copySource));
    }

    public static String uploadFromFile(final File localFile) throws IOException {
        try (final InputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            return uploadFromStream(in);
        }
    }

    @SuppressWarnings("resource")
    public static Entry<String, OutputStream> createUploadStream() throws IOException {
        final String stagingFileName = UUID.randomUUID().toString();
        final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);

        final Path copyDestiation = new Path(new Path(stagingAreaURI), stagingFileName);
        final OutputStream out = new GZIPOutputStream(fs.create(copyDestiation));

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
            public OutputStream setValue(OutputStream value) {
                throw new RuntimeException("setValue not supported");
            }
        };
    }

    public static String uploadFromStream(final InputStream in) throws IOException {
        final Entry<String, OutputStream> outEntry = createUploadStream();

        final byte[] buffer = new byte[8192];
        try (final OutputStream out = outEntry.getValue()) {
            int read;
            while ((read = in.read(buffer)) >= 0) {
                out.write(buffer, 0, read);
            }
        }

        return outEntry.getKey();
    }

    @SuppressWarnings("resource")
    public static void delete(String stagingFileName) throws IOException {
        validateStagingFileName(stagingFileName);

        final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);
        final Path stagingFile = new Path(new Path(stagingAreaURI), stagingFileName);
        if (!fs.delete(stagingFile, false)) {
            throw new IOException("Failed to delete staging file at " + stagingFile.toUri().toString());
        }
    }

    private static void validateStagingFileName(String stagingFileName) {
        if (!STAGING_FILENAME_PATTERN.matcher(stagingFileName).matches()) {
            throw new IllegalArgumentException("Illegal name for staging file: " + stagingFileName);
        }
    }

    @SuppressWarnings("resource")
    public static void cleanUp() {
        LOG.info("Cleaning up staging area for KNIME Spark jobs");
        try {
            final FileSystem fs = FileSystem.get(stagingAreaURI, hadoopConf);
            
            // when the URI ends with a slash, then only the directory contents but not the directory itself will be deleted
            fs.delete(new Path(URI.create(stagingAreaURI.toString().replaceFirst("/$", ""))), true);
        } catch (IOException e) {
            LOG.error("Error while deleting staging are for KNIME Spark jobs: " + e.getMessage(), e);
        }

        LOG.info("Cleaning up local temp file directory for KNIME Spark jobs");
        try {
            final List<java.nio.file.Path> files = Files.walk(localTmpDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            for (java.nio.file.Path toDelete : files) {
                Files.deleteIfExists(toDelete);
            }
        } catch (IOException e) {
            LOG.error("Error while deleting local temp file directory of KNIME Spark jobs: " + e.getMessage(), e);
        }
    }
}
