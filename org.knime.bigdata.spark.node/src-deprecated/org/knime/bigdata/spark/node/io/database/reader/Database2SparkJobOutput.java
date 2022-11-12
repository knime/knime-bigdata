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
 *   Created on Sep 5, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.database.reader;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
@Deprecated
public class Database2SparkJobOutput extends JobOutput {
    /** Empty deserialization constructor. */
    public Database2SparkJobOutput() {}

    /**
     * Constructor - simply stores given spec
     * @param namedObject - the name of the output object (e.g. RDD)
     * @param spec -  the {@link IntermediateSpec} of the table
     */
    public Database2SparkJobOutput(final String namedObject, final IntermediateSpec spec) {
        withSpec(namedObject, spec);
    }
}
