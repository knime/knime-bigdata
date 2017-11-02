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
 */
package com.knime.bigdata.spark.node.sql;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class SparkSQLJobOutput extends JobOutput {

    /** Empty deserialization constructor. */
    public SparkSQLJobOutput() {}

    /**
     * Constructor - simply stores given spec
     * @param namedObject - the name of the output object (e.g. RDD)
     * @param spec -  the {@link IntermediateSpec} of the table
     */
    public SparkSQLJobOutput(final String namedObject, final IntermediateSpec spec) {
        withSpec(namedObject, spec);
    }
}
