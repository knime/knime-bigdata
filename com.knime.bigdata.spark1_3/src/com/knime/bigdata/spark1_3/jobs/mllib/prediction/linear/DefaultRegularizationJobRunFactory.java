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
 *   Created on 06.05.2016 by koetter
 */
package com.knime.bigdata.spark1_3.jobs.mllib.prediction.linear;

import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.node.mllib.prediction.linear.LinearLearnerJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class DefaultRegularizationJobRunFactory extends DefaultJobRunFactory<LinearLearnerJobInput, ModelJobOutput> {

    /**
     * Constructor.
     *
     * @param jobId Unique identifier for job.
     * @param jobClass The class that implements the job.
     */
    protected DefaultRegularizationJobRunFactory(final String jobId,
        final Class<? extends AbstractRegularizationJob<LinearLearnerJobInput>> jobClass) {
        super(jobId, jobClass, ModelJobOutput.class);
    }
}
