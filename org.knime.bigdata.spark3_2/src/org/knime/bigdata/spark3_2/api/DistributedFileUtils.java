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
package org.knime.bigdata.spark3_2.api;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.SparkDistributedTempProvider;

/**
 * File utilities using a distributed file system.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public final class DistributedFileUtils {

    private static final Logger LOGGER = Logger.getLogger(DistributedFileUtils.class.getName());

    private static DistributedFileUtils singletonInstance;

    private final URI m_stagingAreaURI;

    private DistributedFileUtils(final URI stagingAreaURI) {
        m_stagingAreaURI = stagingAreaURI;
    }

    /**
     * Ensure initialized using a {@link SparkDistributedTempProvider}, useful on job managers using a distributed
     * staging directory.
     *
     * @param dfsTempProvider provider of a distributed temporary directory
     */
    public static synchronized void ensureInitialized(final SparkDistributedTempProvider dfsTempProvider) {
        if (singletonInstance == null) {
            singletonInstance = new DistributedFileUtils(dfsTempProvider.getDistributedTempDirURI());
        }
    }

    /**
     * Ensure initialized using the {@link SparkDistributedTempProvider#DISTRIBUTED_TMP_DIR_KEY} spark configuration or
     * a local temporary directory as fallback.
     */
    private static synchronized void ensureInitialized(final SparkContext sparkContext) throws IOException {
        if (singletonInstance == null) {
            final String dfsTmpDir = sparkContext.conf().get(SparkDistributedTempProvider.DISTRIBUTED_TMP_DIR_KEY, null);
            if (!StringUtils.isBlank(dfsTmpDir)) {
                singletonInstance = new DistributedFileUtils(URI.create(dfsTmpDir));
            } else {
                final URI tmpDir = FileUtils.createTempDir(sparkContext, "").toUri();
                LOGGER.warn("No distributed temporary directory setting found, using '" + tmpDir + "'. Consider to set "
                    + SparkDistributedTempProvider.DISTRIBUTED_TMP_DIR_KEY + " in spark configuration.");
                singletonInstance = new DistributedFileUtils(tmpDir);
            }
        }
    }

    /**
     * Create a local model ZIP using a distributed temporary directory to write the model.
     *
     * Note: The {@link DistributedFileUtils} must be initialized with
     * {@link #ensureInitialized(SparkDistributedTempProvider)} or using the
     * {@link SparkDistributedTempProvider#DISTRIBUTED_TMP_DIR_KEY} spark configuration, otherwise a local temporary
     * directory will be used as fallback and a warning will be logged.
     *
     * @param sparkContext current spark context
     * @param model the model to ZIP
     * @return local path to the created model ZIP
     * @throws IOException When something went wrong while zipping the given model.
     */
    public static Path zipModel(final SparkContext sparkContext, final PipelineModel model) throws IOException {
        ensureInitialized(sparkContext);
        return singletonInstance.zipModelViaDistributedFS(sparkContext, model);
    }

    /**
     * Create a local ZIP of a given model using {@link #m_stagingAreaURI} as temporary directory.
     *
     * Note: Do not close the Hadoop file system here, it might be in use in a parallel running job.
     */
    @SuppressWarnings("resource")
    private Path zipModelViaDistributedFS(final SparkContext sparkContext, final PipelineModel model) throws IOException {
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

    /**
     * Unzips a local ZIP of a given model to the staging area and loads a {@link PipelineModel} from there.
     *
     * Note: Do not close the Hadoop file system here, it might be in use in a parallel running job.
     *
     * @param sparkContext current spark context
     * @param modelZip The zipped pipeline model.
     * @return a {@link PipelineModel} loaded from the given zip file.
     * @throws IOException When something went wrong while unzipping or loading the given model.
     */
    public static PipelineModel unzipModel(final SparkContext sparkContext, final Path modelZip) throws IOException {
        ensureInitialized(sparkContext);
        return singletonInstance.unzipModelViaDistributedFS(sparkContext, modelZip);
    }

    @SuppressWarnings("resource")
    private PipelineModel unzipModelViaDistributedFS(final SparkContext sparkContext, final Path modelZip) throws IOException {
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
