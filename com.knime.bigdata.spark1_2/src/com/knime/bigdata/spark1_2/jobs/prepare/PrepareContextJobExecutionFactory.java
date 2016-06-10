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
 *   Created on Apr 27, 2016 by bjoern
 */
package com.knime.bigdata.spark1_2.jobs.prepare;

import com.knime.bigdata.spark.core.context.SparkContextConstants;
import com.knime.bigdata.spark.core.job.DefaultSimpleJobRun;
import com.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import com.knime.bigdata.spark.core.job.SimpleJobRun;
import com.knime.bigdata.spark.core.util.PrepareContextJobInput;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class PrepareContextJobExecutionFactory extends DefaultSimpleJobRunFactory<PrepareContextJobInput> {


    /**
     * Constructor
     */
    public PrepareContextJobExecutionFactory() {
        super(SparkContextConstants.PREPARE_CONTEXT_JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleJobRun<PrepareContextJobInput> createRun(final PrepareContextJobInput input) {

        return new DefaultSimpleJobRun<PrepareContextJobInput>(input, PrepareContextJob.class);
    }
}
