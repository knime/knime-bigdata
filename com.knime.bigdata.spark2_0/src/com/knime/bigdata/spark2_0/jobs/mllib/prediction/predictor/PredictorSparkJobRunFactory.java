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
 *   Created on 29.04.2016 by koetter
 */
package com.knime.bigdata.spark2_0.jobs.mllib.prediction.predictor;

import java.io.File;
import java.util.List;

import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRun;
import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.node.mllib.prediction.predictor.MLlibPredictorNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PredictorSparkJobRunFactory extends DefaultJobWithFilesRunFactory<PredictionJobInput, EmptyJobOutput> {

    /**
     * Constructor
     */
    public PredictorSparkJobRunFactory() {
        super(MLlibPredictorNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobWithFilesRun<PredictionJobInput, EmptyJobOutput> createRun(final PredictionJobInput input, final List<File> localFiles) {
        return new DefaultJobWithFilesRun<PredictionJobInput, EmptyJobOutput>(input, PredictionJob.class, EmptyJobOutput.class, localFiles, FileLifetime.JOB);
    }
}
