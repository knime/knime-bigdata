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
 */
package org.knime.bigdata.spark3_4.api;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * File utilities using a distributed file system.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public final class DistributedFileUtilsHadoop implements DistributedFileUtilsProvider {

    private final URI m_stagingAreaURI;

    DistributedFileUtilsHadoop(final URI stagingAreaURI) {
        m_stagingAreaURI = stagingAreaURI;
    }

    /**
     * Create a local ZIP of a given model using {@link #m_stagingAreaURI} as temporary directory.
     *
     * Note: Do not close the Hadoop file system here, it might be in use in a parallel running job.
     */
    @SuppressWarnings("resource")
    @Override
    public Path zipModelViaDistributedFS(final SparkContext sparkContext, final PipelineModel model)
        throws IOException {

        final Configuration hadoopConf = sparkContext.hadoopConfiguration();
        final FileSystem fs = FileSystem.get(m_stagingAreaURI, hadoopConf);
        final org.apache.hadoop.fs.Path modelPath = toHadoopPath(Paths.get("ml-model-" + UUID.randomUUID().toString()));

        model.write().overwrite().save(modelPath.toUri().toString());

        final Path modelZip = FileUtils.createTempFile(sparkContext, "mlmodel", ".zip");
        try (final ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(modelZip))) {
            addFilesToZip(fs, modelPath, zs);
        } finally {
            fs.delete(modelPath, true);
        }

        return modelZip;
    }

    private static void addFilesToZip(final FileSystem fs, final org.apache.hadoop.fs.Path inputFiles,
        final ZipOutputStream zipOutput) throws IOException {

        final int prefixPathLength = inputFiles.toUri().getPath().length();
        final RemoteIterator<LocatedFileStatus> it = fs.listFiles(inputFiles, true);
        while (it.hasNext()) {
            final org.apache.hadoop.fs.Path file = it.next().getPath();
            final String relativePath = file.toUri().getPath().substring(prefixPathLength);
            try (final FSDataInputStream is = fs.open(file)) {
                zipOutput.putNextEntry(new ZipEntry(relativePath));
                final byte[] buffer = new byte[8192];
                int read;
                while ((read = is.read(buffer)) >= 0) {
                    zipOutput.write(buffer, 0, read);
                }
                zipOutput.closeEntry();
            }
        }
    }

    @SuppressWarnings("resource")
    @Override
    public PipelineModel unzipModelViaDistributedFS(final SparkContext sparkContext, final Path modelZip)
        throws IOException {

        final Configuration hadoopConf = sparkContext.hadoopConfiguration();
        final FileSystem fs = FileSystem.get(m_stagingAreaURI, hadoopConf);
        final org.apache.hadoop.fs.Path hadoopModelPath =
            toHadoopPath(Paths.get("ml-model-" + UUID.randomUUID().toString()));

        Path unzippedModelDir = null;
        try {
            unzippedModelDir = FileUtils.createTempDir(sparkContext, "namedmodel");
            FileUtils.unzipToDirectory(modelZip, unzippedModelDir);
            final List<Path> unzippedModelFiles = listUnzippedModelFiles(unzippedModelDir);

            fs.mkdirs(hadoopModelPath);
            for (final Path currUnzippedModelFile : unzippedModelFiles) {
                uploadUnzippedModelFile(fs, hadoopModelPath, unzippedModelDir, currUnzippedModelFile);
            }

            return PipelineModel.load(hadoopModelPath.toUri().toString());

        } finally {
            FileUtils.deleteRecursively(unzippedModelDir);
        }
    }

    private static void uploadUnzippedModelFile(final FileSystem fs, final org.apache.hadoop.fs.Path hadoopModelPath,
        final Path unzippedModelDir, final Path currUnzippedModelFile) throws IOException {

        final Path relLocalModelFile = unzippedModelDir.relativize(currUnzippedModelFile);
        final org.apache.hadoop.fs.Path hadoopModelFile = toHadoopPath(hadoopModelPath, relLocalModelFile);

        if (Files.isDirectory(currUnzippedModelFile)) {
            fs.mkdirs(hadoopModelFile);
        } else {
            fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(currUnzippedModelFile.toAbsolutePath().toUri()),
                hadoopModelFile);
        }
    }

    private static List<Path> listUnzippedModelFiles(final Path unzippedModelDir) throws IOException {
        final List<Path> localModelFiles;
        try (final Stream<Path> modelFileStream = Files.walk(unzippedModelDir)) {
            localModelFiles = modelFileStream.sorted().collect(Collectors.toList());
        }

        // remove the first entry as it is the unzippedModelDir itself
        localModelFiles.remove(0);

        return localModelFiles;
    }

    private org.apache.hadoop.fs.Path toHadoopPath(final Path relLocalFile) {
        return toHadoopPath(new org.apache.hadoop.fs.Path(m_stagingAreaURI), relLocalFile);
    }

    private static org.apache.hadoop.fs.Path toHadoopPath(final org.apache.hadoop.fs.Path parent,
        final Path relLocalFile) {
        if (relLocalFile.isAbsolute()) {
            throw new IllegalArgumentException("Given path must be relative");
        }

        org.apache.hadoop.fs.Path currHadoopFile = parent;
        for (final Path pathComp : relLocalFile) {
            currHadoopFile = new org.apache.hadoop.fs.Path(currHadoopFile, pathComp.toString());
        }

        return currHadoopFile;
    }
}
