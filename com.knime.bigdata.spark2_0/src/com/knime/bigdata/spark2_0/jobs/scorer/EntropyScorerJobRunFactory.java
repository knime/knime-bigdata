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
package com.knime.bigdata.spark2_0.jobs.scorer;

import com.knime.bigdata.spark.core.job.DefaultJobRun;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.node.scorer.entropy.EntropyScorerJobInput;
import com.knime.bigdata.spark.node.scorer.entropy.EntropyScorerJobOutput;
import com.knime.bigdata.spark.node.scorer.entropy.SparkEntropyScorerNodeModel;

/**
 *
 * @author Ole Ostergaard
 */
public class EntropyScorerJobRunFactory extends DefaultJobRunFactory<EntropyScorerJobInput, EntropyScorerJobOutput> {

    /**
     *
     */
    public EntropyScorerJobRunFactory() {
        super(SparkEntropyScorerNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRun<EntropyScorerJobInput, EntropyScorerJobOutput> createRun(final EntropyScorerJobInput input) {
        return new DefaultJobRun<>(input, EntropyScorerJob.class, EntropyScorerJobOutput.class);
    }
}
