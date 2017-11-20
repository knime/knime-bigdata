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
 *   Created on Nov 16, 2017 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.groupby;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Group by job output with result spec.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SparkGroupByJobOutput extends JobOutput {
    /** Deserialization constructor */
    public SparkGroupByJobOutput() {}

    /**
     * @param outputObject id of output object
     * @param outputSpec spec of output
     */
    public SparkGroupByJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        withSpec(outputObject, outputSpec);
    }
}
