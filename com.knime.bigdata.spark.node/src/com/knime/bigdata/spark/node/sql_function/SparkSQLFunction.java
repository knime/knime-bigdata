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
package com.knime.bigdata.spark.node.sql_function;

import org.knime.core.data.DataTableSpec;

import com.knime.bigdata.spark.core.sql_function.SparkSQLFunctionFactory;
import com.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;

/**
 * Spark SQL function.
 * @author Sascha Wolke, KNIME GmbH
 */
public interface SparkSQLFunction {

    /**
     * The unique identifier of the function that is used for registration and
     * identification of the aggregation method. The id is an internal
     * used variable that is not displayed to the user.
     *
     * @return the unique id of this function
     */
    String getId();

    /**
     * Returns spark job input.
     *
     * @param factory Class name of {@link SparkSQLFunctionFactory}
     * @param inColName Name of the input column
     * @param outColName Name of the ouput column
     * @param inSpec Input table spec
     * @return Spark function input
     */
    public SparkSQLFunctionJobInput getSparkJobInput(final String factory,
        final String inColName, final String outColName, final DataTableSpec inSpec);

    /**
     * Generate a function(column) style column name.
     * @param columnName name of input column
     * @param inSpec input data table spec
     * @return generated column name
     */
    public String getFuncNameColNameLabel(final String columnName, final DataTableSpec inSpec);

    /**
     * Generate a column(function) style column name.
     * @param columnName name of input column
     * @param inSpec input data table spec
     * @return generated column name
     */
    public String getColNameFuncNameLabel(final String columnName, final DataTableSpec inSpec);
}
