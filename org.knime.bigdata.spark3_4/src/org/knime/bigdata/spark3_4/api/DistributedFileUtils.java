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
import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksSparkSideStagingAreaProvider;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.SparkDistributedTempProvider;

/**
 * File utilities using a distributed file system. This class delegates to {@link DistributedFileUtilsHadoop} or
 * {@link DistributedFileUtilsUnityCatalog}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public final class DistributedFileUtils {

    private static final Logger LOGGER = Logger.getLogger(DistributedFileUtils.class.getName());

    private static DistributedFileUtilsProvider singletonInstance;

    private DistributedFileUtils() {
        // Avoid instantiation of this utility class.
    }

    /**
     * Ensure initialized using a {@link SparkDistributedTempProvider}, useful on job managers using a distributed
     * staging directory. Databricks specific version that supports the Unity Catalog.
     *
     * @param provider provider of a distributed temporary directory
     */
    public static synchronized void ensureInitializedDatabricks(final DatabricksSparkSideStagingAreaProvider provider) {
        if (singletonInstance == null && provider.isUnityCatalog()) {
            singletonInstance = new DistributedFileUtilsUnityCatalog(provider.getDistributedTempDirURI());
        } else if (singletonInstance == null) {
            singletonInstance = new DistributedFileUtilsHadoop(provider.getDistributedTempDirURI());
        }
    }

    /**
     * Ensure initialized using a {@link SparkDistributedTempProvider}, useful on job managers using a distributed
     * staging directory. Livy specific version.
     *
     * @param dfsTempProvider provider of a distributed temporary directory
     */
    public static synchronized void ensureInitializedLivy(final SparkDistributedTempProvider dfsTempProvider) {
        if (singletonInstance == null) {
            singletonInstance = new DistributedFileUtilsHadoop(dfsTempProvider.getDistributedTempDirURI());
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
                singletonInstance = new DistributedFileUtilsHadoop(URI.create(dfsTmpDir));
            } else {
                final URI tmpDir = FileUtils.createTempDir(sparkContext, "").toUri();
                LOGGER.warn("No distributed temporary directory setting found, using '" + tmpDir + "'. Consider to set "
                    + SparkDistributedTempProvider.DISTRIBUTED_TMP_DIR_KEY + " in spark configuration.");
                singletonInstance = new DistributedFileUtilsHadoop(tmpDir);
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

}
