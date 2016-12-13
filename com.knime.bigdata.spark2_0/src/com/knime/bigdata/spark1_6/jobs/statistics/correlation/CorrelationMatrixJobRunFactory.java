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
package com.knime.bigdata.spark1_6.jobs.statistics.correlation;

import com.knime.bigdata.spark.core.job.DefaultJobRun;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.HalfDoubleMatrixJobOutput;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.node.statistics.correlation.CorrelationJobInput;
import com.knime.bigdata.spark.node.statistics.correlation.matrix.MLlibCorrelationMatrixNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class CorrelationMatrixJobRunFactory
extends DefaultJobRunFactory<CorrelationJobInput, HalfDoubleMatrixJobOutput> {

    /**
     * Constructor.
     */
    public CorrelationMatrixJobRunFactory() {
        super(MLlibCorrelationMatrixNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRun<CorrelationJobInput, HalfDoubleMatrixJobOutput> createRun(final CorrelationJobInput input) {
        return new DefaultJobRun<>(input, CorrelationMatrixJob.class, HalfDoubleMatrixJobOutput.class);
    }
}
