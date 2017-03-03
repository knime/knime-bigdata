/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 29.01.2016 by koetter
 */
package com.knime.bigdata.spark2_0.jobs.pmml;

import java.io.File;
import java.util.List;

import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRun;
import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.node.pmml.predictor.AbstractSparkPMMLPredictorNodeModel;
import com.knime.bigdata.spark.node.pmml.predictor.PMMLPredictionJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLPredictionJobRunFactory extends DefaultJobWithFilesRunFactory<PMMLPredictionJobInput, EmptyJobOutput> {

    /**
     * Constructor.
     */
    public PMMLPredictionJobRunFactory() {
        super(AbstractSparkPMMLPredictorNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobWithFilesRun<PMMLPredictionJobInput, EmptyJobOutput> createRun(final PMMLPredictionJobInput input,
        final List<File> localFiles) {
        return new DefaultJobWithFilesRun<>(input, PMMLPredictionJob.class, EmptyJobOutput.class, localFiles,
                FileLifetime.JOB);
    }
}