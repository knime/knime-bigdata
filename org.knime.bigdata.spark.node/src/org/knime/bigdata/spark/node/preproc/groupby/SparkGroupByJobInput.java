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

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;

/**
 * Group by job input with aggregation functions.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SparkGroupByJobInput extends JobInput {
    private static final String GROUP_BY_FUNCTIONS = "groupByFunctions";
    private static final String PIVOT_COLUMN = "pivotColumn";
    private static final String PIVOT_COMPUTE_VALUES = "pivotComputeValues";
    private static final String PIVOT_COMPUTE_VALUES_LIMIT = "pivotComputeValuesLimit";
    private static final String PIVOT_VALUES = "pivotValues";
    private static final String PIVOT_IGNORE_MISSING_VALUES = "pivotIgnoreMissingValues";
    private static final String AGG_FUNCTIONS = "aggregateFunctions";
    private static final String PIVOT_VALIDATE_VALUES = "pivotValidateValues";

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

    /**
     * Enable pivot mode and set column to use in pivoting.
     * @param column name of column to use in pivot mode
     */
    public void setPivotColumn(final String column) {
        set(PIVOT_COLUMN, column);
    }

    /** @return <code>true</code> if pivoting should be used */
    public boolean usePivoting() {
        return has(PIVOT_COLUMN);
    }

    /** @return column name to use in pivot */
    public String getPivotColumn() {
        return get(PIVOT_COLUMN);
    }

    /**
     * Sets whether rows with missing values in the pivot colum should be ignored or not.
     *
     * @param ignoreMissingValues whether to ignore missing values or not.
     */
    public void setIgnoreMissingValuesInPivotColumn(final boolean ignoreMissingValues) {
        set(PIVOT_IGNORE_MISSING_VALUES, ignoreMissingValues);
    }

    /** @return <code>true</code> if rows with missing values in pivot column should be ignored */
    public boolean ignoreMissingValuesInPivotColumn() {
        return get(PIVOT_IGNORE_MISSING_VALUES);
    }

    /** @param enabled set to <code>true</code> if pivot labels should be automatic computed */
    public void setComputePivotValues(final boolean enabled) {
        set(PIVOT_COMPUTE_VALUES, enabled);
    }

    /** @return <code>true</code> if values list in pivot mode should be automatic computed */
    public boolean computePivotValues() {
        return get(PIVOT_COMPUTE_VALUES);
    }

    /** @param limit maximal values count in computed pivot values mode */
    public void setComputePivotValuesLimit(final int limit) {
        set(PIVOT_COMPUTE_VALUES_LIMIT, limit);
    }

    /** @return maximal value count in computed pivot values mode */
    public int getComputePivotValuesLimit() {
        return getInteger(PIVOT_COMPUTE_VALUES_LIMIT);
    }

    /**
     * @param values pivot values
     * @param validateValues <code>true</code> if values should be validated against pivot values from data frame
     */
    public void setPivotValues(final Serializable values[], final boolean validateValues) {
        set(PIVOT_VALUES, values);
        set(PIVOT_VALIDATE_VALUES, validateValues);
    }

    /** @return pivot values */
    public Object[] getPivotValues() {
        return get(PIVOT_VALUES);
    }

    /** @return <code>true</code> if values should be validated against pivot values from data frame */
    public boolean validateValues() {
        return has(PIVOT_VALIDATE_VALUES) && (boolean) get(PIVOT_VALIDATE_VALUES);
    }

    /** @return aggregate functions */
    public SparkSQLFunctionJobInput[] getAggregateFunctions() {
        return get(AGG_FUNCTIONS);
    }
}
