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
 *   Created on May 9, 2016 by oole
 */
package com.knime.bigdata.spark.node.preproc.sampling;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class SamplingJobOutput extends JobOutput {


    private static final String IS_SUCCESS = "isSuccess";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public SamplingJobOutput() {}

    public SamplingJobOutput(final boolean isSuccess) {
        set(IS_SUCCESS, isSuccess);
    }

    /**
     * @return whether the sampling was successfull
     */
    public boolean isSuccess() {
        return get(IS_SUCCESS);
    }

}
