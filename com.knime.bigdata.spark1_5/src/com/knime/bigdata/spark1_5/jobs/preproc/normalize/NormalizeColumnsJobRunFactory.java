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
package com.knime.bigdata.spark1_5.jobs.preproc.normalize;

import com.knime.bigdata.spark.core.job.DefaultJobRun;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.node.preproc.normalize.NormalizeJobInput;
import com.knime.bigdata.spark.node.preproc.normalize.NormalizeJobOutput;
import com.knime.bigdata.spark.node.preproc.normalize.SparkNormalizerPMMLNodeModel;

/**
 *
 * @author Ole Ostergaard
 */
public class NormalizeColumnsJobRunFactory extends DefaultJobRunFactory<NormalizeJobInput, NormalizeJobOutput> {

    /**
     *
     */
    public NormalizeColumnsJobRunFactory() {
        super(SparkNormalizerPMMLNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRun<NormalizeJobInput, NormalizeJobOutput> createRun(final NormalizeJobInput input) {
        return new DefaultJobRun<>(input, NormalizeColumnsJob.class, NormalizeJobOutput.class);
    }
}
