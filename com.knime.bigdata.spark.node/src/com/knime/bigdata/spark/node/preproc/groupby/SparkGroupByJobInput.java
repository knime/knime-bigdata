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
package com.knime.bigdata.spark.node.preproc.groupby;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;

/**
 * Group by job input with aggregation functions.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SparkGroupByJobInput extends JobInput {
    private static final String GROUP_BY_FUNCTIONS = "groupByFunctions";
    private static final String AGG_FUNCTIONS = "aggregateFunctions";

    /** Deserialization constructor */
    public SparkGroupByJobInput() {}

    /**
     * @param inputObject id of input object
     * @param outputObject id of output object
     * @param groupByFunctions functions to use in group by
     * @param aggFunctions aggregation functions to uses
     */
    public SparkGroupByJobInput(final String inputObject, final String outputObject,
        final SparkSQLFunctionJobInput groupByFunctions[],
        final SparkSQLFunctionJobInput aggFunctions[]) {

        if (aggFunctions.length == 0) {
            throw new IllegalArgumentException("No aggregation functions given, no computation required.");
        }

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(GROUP_BY_FUNCTIONS, groupByFunctions);
        set(AGG_FUNCTIONS, aggFunctions);
    }

    /** @return function to use in group by */
    public SparkSQLFunctionJobInput[] getGroupByFunctions() {
        return get(GROUP_BY_FUNCTIONS);
    }

    /** @return aggregate functions */
    public SparkSQLFunctionJobInput[] getAggregateFunctions() {
        return get(AGG_FUNCTIONS);
    }
}
