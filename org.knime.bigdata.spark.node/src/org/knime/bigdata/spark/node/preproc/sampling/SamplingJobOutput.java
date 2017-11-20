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
 *   Created on May 9, 2016 by oole
 */
package org.knime.bigdata.spark.node.preproc.sampling;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class SamplingJobOutput extends JobOutput {
    private static final String SAMPLES_RDD_IS_INPUT_RDD = "samplesRddIsInputRdd";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public SamplingJobOutput() {}

    /**
     * @param samplesRddIsInputRdd true if first output RDD is input RDD (all rows are sampled)
     */
    public SamplingJobOutput(final boolean samplesRddIsInputRdd) {
        set(SAMPLES_RDD_IS_INPUT_RDD, samplesRddIsInputRdd);
    }

    /**
     * @return true if first output RDD is input RDD (all rows are sampled)
     */
    public boolean samplesRddIsInputRdd() {
        return get(SAMPLES_RDD_IS_INPUT_RDD);
    }

}
