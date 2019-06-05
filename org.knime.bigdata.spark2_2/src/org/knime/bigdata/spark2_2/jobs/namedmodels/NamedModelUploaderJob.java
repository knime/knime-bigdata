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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark2_2.jobs.namedmodels;

import java.nio.file.Path;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.model.NamedModelUploaderJobInput;
import org.knime.bigdata.spark2_2.api.FileUtils;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Helper job to manage named objects on the server side.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class NamedModelUploaderJob implements SimpleSparkJob<NamedModelUploaderJobInput> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = Logger.getLogger(NamedModelUploaderJob.class);

    @Override
    public void runJob(final SparkContext sparkContext, final NamedModelUploaderJobInput input,
        final NamedObjects namedObjects) throws Exception {

        final Path zippedModelPipeline = input.getZippedModelPipeline();

        Path unzippedModelDir = null;
        try {
            unzippedModelDir = FileUtils.createTempDir(sparkContext, "namedmodel");
            FileUtils.unzipToDirectory(zippedModelPipeline, unzippedModelDir);
            final PipelineModel model = PipelineModel.load(unzippedModelDir.toUri().toString());
            namedObjects.add(input.getNamedModelId(), model);
            LOG.info("Added named model with ID " + input.getNamedModelId());
        } finally {
            FileUtils.deleteRecursively(unzippedModelDir);
        }
    }
}
