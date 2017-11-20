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
package org.knime.bigdata.spark.node.sql;

import java.util.List;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class SparkSQLFunctionsJobOutput extends JobOutput {
    private final static String KEY_FUNCTIONS = "functions";

    /** Empty deserialization constructor. */
    public SparkSQLFunctionsJobOutput() {}

    /**
     * Constructor with required parameters
     * @param functions - Spark SQL function names
     */
    public SparkSQLFunctionsJobOutput(final List<String> functions) {
        set(KEY_FUNCTIONS, functions);
    }

    /** @return Spark SQL function names */
    public List<String> getFunctions() {
        return get(KEY_FUNCTIONS);
    }
}
