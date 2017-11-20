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
 */
package com.knime.bigdata.spark.node.preproc.missingval.compute;

import java.io.Serializable;
import java.util.Map;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * Output containing aggregation/statistic operations results.
 * 
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SparkMissingValueJobOutput extends JobOutput {
    private static final String KEY_VALUES = "values";

    /** Empty serializer constructor */
    public SparkMissingValueJobOutput() {
    }

    /** @param fixedValues results of aggregation/static operations */
    public SparkMissingValueJobOutput(final Map<String, Serializable> fixedValues) {
        set(KEY_VALUES, fixedValues);
    }

    /** @return results of aggregation/static operations */
    public Map<String, Serializable> getValues() {
        return get(KEY_VALUES);
    }
}
