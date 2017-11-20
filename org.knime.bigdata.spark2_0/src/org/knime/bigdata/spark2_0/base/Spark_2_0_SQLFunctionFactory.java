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
package org.knime.bigdata.spark2_0.base;

import static org.apache.spark.sql.functions.approxCountDistinct;
import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.corr;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.covar_pop;
import static org.apache.spark.sql.functions.covar_samp;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.kurtosis;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.skewness;
import static org.apache.spark.sql.functions.stddev_pop;
import static org.apache.spark.sql.functions.stddev_samp;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.sumDistinct;
import static org.apache.spark.sql.functions.var_pop;
import static org.apache.spark.sql.functions.var_samp;
import static org.apache.spark.sql.functions.window;
import static scala.collection.JavaConversions.asScalaBuffer;

import java.util.ArrayList;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionFactory;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;
import org.knime.bigdata.spark2_0.api.TypeConverters;

import scala.collection.Seq;

/**
 * Factory of Spark 2.0 functions.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class Spark_2_0_SQLFunctionFactory implements SparkSQLFunctionFactory<Column> {

    /** Spark aggregation functions supported by this factory */
    public final static String SUPPORTED_FUNCTIONS[] = new String[] {
        "column",

        "asc", "desc",

        "approx_count_distinct", "avg", "collect_list", "collect_set", "corr", "count", "count_distinct", "covar_pop",
        "covar_samp", "first", "kurtosis", "last", "max", "mean", "min", "skewness",
        "stddev_samp", "stddev_pop", "sum", "sum_distinct", "var_samp", "var_pop",

        "window"
    };

    /** Empty default constructor */
    public Spark_2_0_SQLFunctionFactory() {}

    @Override
    public Column getFunctionColumn(final SparkSQLFunctionJobInput agg) {
        final String name = agg.getFunction().toLowerCase();
        final String columnName = (String) getArg(agg, 0);
        final int args = agg.getArgCount();
        final Column column;

        if ("asc".equals(name)) {
            column = asc(columnName);
        } else if (name.equals("approx_count_distinct") && args == 1) {
            column = approxCountDistinct(columnName);
        } else if (name.equals("approx_count_distinct") && args == 2) {
            column = approxCountDistinct(columnName, (double) getArg(agg, 1));
        } else if ("avg".equals(name)) {
            column = avg(columnName);
        } else if ("collect_list".equals(name)) {
            column = collect_list(columnName);
        } else if ("collect_set".equals(name)) {
            column = collect_set(columnName);
        } else if ("column".equals(name)) {
            column = column(columnName);
        } else if ("corr".equals(name) && args == 2) {
            column = corr(columnName, (String) getArg(agg, 1));
        } else if ("count".equals(name)) {
            column = count(columnName);
        } else if ("count_distinct".equals(name) && args >= 1) {
            column = countDistinct(columnName, getAdditionalStrings(agg));
        } else if ("covar_pop".equals(name) && args == 2) {
            column = covar_pop(columnName, (String) getArg(agg, 1));
        } else if ("covar_samp".equals(name) && args == 2) {
            column = covar_samp(columnName, (String) getArg(agg, 1));
        } else if ("desc".equals(name)) {
            column = desc(columnName);
        } else if ("first".equals(name) && args == 2) {
            column = first(columnName, (boolean) getArg(agg, 1));
        } else if ("first".equals(name)) {
            column = first(columnName);
        } else if ("kurtosis".equals(name)) {
            column = kurtosis(columnName);
        } else if ("last".equals(name) && args == 2) {
            column = last(columnName, (boolean) getArg(agg, 1));
        } else if ("last".equals(name)) {
            column = last(columnName);
        } else if ("max".equals(name)) {
            column = max(columnName);
        } else if ("mean".equals(name)) {
            column = mean(columnName);
        } else if ("min".equals(name)) {
            column = min(columnName);
        } else if ("skewness".equals(name)) {
            column = skewness(columnName);
        } else if ("stddev_samp".equals(name)) { // alias: stddev
            column = stddev_samp(columnName);
        } else if ("stddev_pop".equals(name)) {
            column = stddev_pop(columnName);
        } else if ("sum".equals(name)) {
            column = sum(columnName);
        } else if ("sum_distinct".equals(name)) {
            column = sumDistinct(columnName);
        } else if ("var_samp".equals(name)) { // alias: variance
            column = var_samp(columnName);
        } else if ("var_pop".equals(name)) {
            column = var_pop(columnName);
        } else if ("window".equals(name) && args == 2) {
            column = window(column(columnName), (String) getArg(agg, 1));
        } else if ("window".equals(name) && args == 3) {
            column = window(column(columnName), (String) getArg(agg, 1), (String) getArg(agg, 2));
        } else if ("window".equals(name) && args == 4) {
            column = window(column(columnName), (String) getArg(agg, 1), (String) getArg(agg, 2), (String) getArg(agg, 3));
        } else {
            throw new IllegalArgumentException("Unknown function or parameter (func: " + name + ", args: " + args + ")");
        }

        if (agg.getOutputName() != null) {
            return column.name(agg.getOutputName());
        } else {
            return column;
        }
    }

    /** @return converted function argument */
    private Object getArg(final SparkSQLFunctionJobInput agg, final int i) {
        final IntermediateToSparkConverter<? extends DataType> converter = TypeConverters.getConverter(agg.getArgType(i));
        return converter.convert(agg.getArg(i));
    }

    /** @return additional arguments as string (all arguments except the first) */
    private Seq<String> getAdditionalStrings(final SparkSQLFunctionJobInput agg) {
        final int args = agg.getArgCount();
        final ArrayList<String> result = new ArrayList<>(args - 1);
        for (int i = 1; i < args; i++) {
            result.add((String) getArg(agg, i));
        }
        return asScalaBuffer(result);
    }
}
