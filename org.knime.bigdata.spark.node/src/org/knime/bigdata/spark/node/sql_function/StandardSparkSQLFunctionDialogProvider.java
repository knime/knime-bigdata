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
package org.knime.bigdata.spark.node.sql_function;

import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.node.sql_function.agg.FirstAggregation;
import org.knime.bigdata.spark.node.sql_function.agg.LastAggregation;
import org.knime.bigdata.spark.node.sql_function.agg.ListAggregation;
import org.knime.bigdata.spark.node.sql_function.agg.MultiColumnAggregation;
import org.knime.bigdata.spark.node.sql_function.agg.SetAggregation;
import org.knime.bigdata.spark.node.sql_function.agg.SimpleAggregation;
import org.knime.bigdata.spark.node.sql_function.agg.SumAggregation;
import org.knime.bigdata.spark.node.sql_function.agg.TwoColumnAggregation;
import org.knime.core.data.DoubleValue;

/**
 * Function provider with standard Spark functions.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class StandardSparkSQLFunctionDialogProvider extends SparkSQLFunctionDialogProvider {

    /** Default constructor */
    public StandardSparkSQLFunctionDialogProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE,
            new SimpleAggregation.Factory(
                "avg", "Returns the average of the values in a group", DoubleValue.class),
            new ListAggregation.Factory(),
            new SetAggregation.Factory(),
            new TwoColumnAggregation.Factory(
                "corr", "Returns the Pearson Correlation Coefficient for two columns.", DoubleValue.class),
            new SimpleAggregation.Factory(
                "count", "Returns the number of non-null values"),
            new MultiColumnAggregation.Factory(
                "count_distinct", "Returns the number of unique and non-null values"),
            new TwoColumnAggregation.Factory(
                "covar_pop", "Returns the population covariance for two columns", DoubleValue.class),
            new TwoColumnAggregation.Factory(
                "covar_samp", "Returns the sample covariance for two columns", DoubleValue.class),
            new FirstAggregation.Factory(),
            new SimpleAggregation.Factory(
                "kurtosis", "Returns the kurtosis value calculated from values of a group.", DoubleValue.class),
            new LastAggregation.Factory(),
            new SimpleAggregation.Factory(
                "max", "Returns the maximum value of a column"),
            new SimpleAggregation.Factory(
                "mean", "Returns the average of the values in a group (alias for avg)"),
            new SimpleAggregation.Factory(
                "min", "Returns the minimum value of a column"),
            new SimpleAggregation.Factory(
                "skewness", "Returns the skewness of the values in a group", DoubleValue.class),
            new SimpleAggregation.Factory(
                "stddev_samp", "Returns the sample standard deviation of values in a group", DoubleValue.class),
            new SimpleAggregation.Factory(
                "stddev_pop", "Returns the population standard deviation of values in a group", DoubleValue.class),
            new SumAggregation.Factory(
                "sum", "Returns the sum of all values in the given column"),
            new SumAggregation.Factory(
                "sum_distinct", "Returns the sum of distinct values in the given column"),
            new SimpleAggregation.Factory(
                "var_samp", "Returns the unbiased variance of the values in a group", DoubleValue.class),
            new SimpleAggregation.Factory(
                "var_pop", "Returns the population variance of the values in a group", DoubleValue.class)
        );
    }

    // unused aggregation functions
    //  new SimpleAggFunFactory("approx_count_distinct", ""),
    //  new SimpleAggFunFactory("approx_count_distinct", ""), // columnName, (double) getArg(agg, 1));
    //  new SimpleAggFunFactory("grouping", ""),
    //  new SimpleAggFunFactory("grouping_id", ""), // columnName, getAdditionalStrings(agg));

    // sort functions
    //  new SimpleAggFunFactory(asc", ""),
    //  new SimpleAggFunFactory(asc_nulls_first", ""),
    //  new SimpleAggFunFactory(asc_nulls_last", ""),
    //  new SimpleAggFunFactory(desc", ""),
    //  new SimpleAggFunFactory(desc_nulls_first", ""),
    //  new SimpleAggFunFactory(desc_nulls_last", ""),
}
