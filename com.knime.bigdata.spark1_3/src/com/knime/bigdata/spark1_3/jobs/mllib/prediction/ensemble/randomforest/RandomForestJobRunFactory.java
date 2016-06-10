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
package com.knime.bigdata.spark1_3.jobs.mllib.prediction.ensemble.randomforest;

import com.knime.bigdata.spark.core.job.DefaultJobRun;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.MLlibRandomForestNodeModel;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.RandomForestJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class RandomForestJobRunFactory extends DefaultJobRunFactory<RandomForestJobInput, ModelJobOutput> {

    /**Constructor.*/
    public RandomForestJobRunFactory() {
        super(MLlibRandomForestNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRun<RandomForestJobInput, ModelJobOutput> createRun(final RandomForestJobInput input) {
        return new DefaultJobRun<>(input, RandomForestJob.class, ModelJobOutput.class);
    }
}
