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
package org.knime.bigdata.spark3_5.api;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * File utilities using the Java NIO API and workarounds to support Databricks Unity Catalog volumes as staging
 * directory.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public final class DistributedFileUtilsUnityCatalog implements DistributedFileUtilsProvider {

    private final Path m_stagingArea;

    DistributedFileUtilsUnityCatalog(final URI stagingAreaURI) {
        m_stagingArea = Paths.get(stagingAreaURI);
    }

    @Override
    public Path zipModelViaDistributedFS(final SparkContext sparkContext, final PipelineModel model)
        throws IOException {

        final Path modelPath = m_stagingArea.resolve(Paths.get("ml-model-" + UUID.randomUUID().toString()));

        model.write().overwrite().save(toSparkPath(modelPath));

        try {
            final Path modelZip = FileUtils.createTempFile(sparkContext, "mlmodel", ".zip");
            FileUtils.zipDirectory(modelPath, modelZip);
            return modelZip;
        } finally {
            FileUtils.deleteRecursively(modelPath);
        }
    }

    @Override
    public PipelineModel unzipModelViaDistributedFS(final SparkContext sparkContext, final Path modelZip)
        throws IOException {

        final Path modelPath = m_stagingArea.resolve(Paths.get("ml-model-" + UUID.randomUUID().toString()));

        try {
            FileUtils.unzipToDirectory(modelZip, modelPath);
            return PipelineModel.load(toSparkPath(modelPath));

        } finally {
            FileUtils.deleteRecursively(modelPath);
        }
    }

    /**
     * Note that the Spark API, used to store/load the model, does not support {@code file://} URIs for Databricks Unity
     * Catalog volumes and we have to strip the {@code file://} schema from the path.
     *
     * @param path original path which might contain a file:// schema
     * @return the path as string for spark API without the file:// schema
     */
    private static String toSparkPath(final Path path) {
        final String result = path.toUri().toString();

        if (result.startsWith("file:///Volumes/")) {
            // remove file:// prefix
            return result.substring(7);
        }

        if (result.startsWith("file:/Volumes/")) {
            // remove file: prefix
            return result.substring(5);
        }

        return result;
    }

}
