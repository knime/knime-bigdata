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
 *   Created on Apr 27, 2016 by bjoern
 */
package org.knime.bigdata.spark.node.io.table.reader;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job input for the Table to Spark job.
 *
 * @author Bjoern Lohrman, KNIME.com
 */
@SparkClass
public class Table2SparkJobInput extends JobInput {

    /**
     * For serialization.
     */
    public Table2SparkJobInput() {
    }

    /**
     * Factory method.
     *
     * @param namedOutputObjectId The ID of the named output object to create.
     * @param intermediateSpec The spec of the output object to create.
     * @return a new instance that hold the given values.
     */
    public static Table2SparkJobInput create(final String namedOutputObjectId,
        final IntermediateSpec intermediateSpec) {
        Table2SparkJobInput input = new Table2SparkJobInput();
        input.addNamedOutputObject(namedOutputObjectId);
        input.withSpec(namedOutputObjectId, intermediateSpec);
        return input;
    }
}
