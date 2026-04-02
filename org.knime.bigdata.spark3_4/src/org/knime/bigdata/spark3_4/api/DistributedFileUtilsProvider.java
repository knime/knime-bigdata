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
 *   Created on Mar 30, 2026 by Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
package org.knime.bigdata.spark3_4.api;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Interface that contains file utilities using a distributed file system.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SparkClass
public interface DistributedFileUtilsProvider {

    /**
     * Create a local model ZIP using a distributed temporary directory to write the model.
     *
     * @param sparkContext current spark context
     * @param model the model to ZIP
     * @return local path to the created model ZIP
     * @throws IOException When something went wrong while zipping the given model.
     */
    Path zipModelViaDistributedFS(final SparkContext sparkContext, final PipelineModel model) throws IOException;

    /**
     * Unzips a local ZIP of a given model to the staging area and loads a {@link PipelineModel} from there.
     *
     * @param sparkContext current spark context
     * @param modelZip The zipped pipeline model.
     * @return a {@link PipelineModel} loaded from the given zip file.
     * @throws IOException When something went wrong while unzipping or loading the given model.
     */
    PipelineModel unzipModelViaDistributedFS(final SparkContext sparkContext, final Path modelZip) throws IOException;

}
