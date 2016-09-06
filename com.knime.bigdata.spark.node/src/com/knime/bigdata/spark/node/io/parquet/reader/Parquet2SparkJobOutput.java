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
 *   Created on Aug 11, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.parquet.reader;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Parquet2SparkJobOutput extends JobOutput {
    /**
     * Paramless constructor for automatic deserialization.
     */
    public Parquet2SparkJobOutput() {}

    /**
     * constructor - simply stores given spec
     */
    public Parquet2SparkJobOutput(final String namedOutputObject, final IntermediateSpec namedOutputObjectSpec) {
        withSpec(namedOutputObject, namedOutputObjectSpec);
    }
}
