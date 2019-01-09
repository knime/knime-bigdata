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
package org.knime.bigdata.spark2_3.base;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.corr;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.covar_pop;
import static org.apache.spark.sql.functions.covar_samp;
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
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionFactory;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark2_3.api.TypeConverters;

import scala.collection.Seq;

/**
 * Factory of Spark 2.3 functions.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class Spark_2_3_SQLFunctionFactory implements SparkSQLFunctionFactory<Column> {

    /** Spark aggregation functions supported by this factory */
    public final static String SUPPORTED_FUNCTIONS[] = new String[] {
        "column",

        /* "asc", "asc_nulls_first", "asc_nulls_last", "desc", "desc_nulls_first", "desc_nulls_last", */

        /* "approx_count_distinct", */ "avg", "collect_list", "collect_set", "corr", "count", "count_distinct", "covar_pop",
        "covar_samp", "first", "kurtosis", "last", "max", "mean", "min", "skewness",
        "stddev_samp", "stddev_pop", "sum", "sum_distinct", "var_samp", "var_pop",

        /* special boolean types, that needs a cast of the input column to integer */
        "sum_boolean", "sum_distinct_boolean",

        "window"
    };

    /** Empty default constructor */
    public Spark_2_3_SQLFunctionFactory() {}

    @Override
    public Column getFunctionColumn(final SparkSQLFunctionJobInput input) {
        final String inputColumnName = (String) getArg(input, 0);
        final Column inputColumn = column(inputColumnName);
        return getFunctionColumn(input, inputColumn);
    }

    private Column getFunctionColumn(final SparkSQLFunctionJobInput input, final Column inputColumn) {
        final String name = input.getFunction().toLowerCase();
        final int args = input.getArgCount();
        final Column doubleColumn = inputColumn.cast(DataTypes.DoubleType);
        final Column column;

        if ("avg".equals(name)) {
            column = avg(doubleColumn);
        } else if ("collect_list".equals(name)) {
            column = collect_list(inputColumn);
        } else if ("collect_set".equals(name)) {
            column = collect_set(inputColumn);
        } else if ("column".equals(name)) {
            column = inputColumn;
        } else if ("corr".equals(name) && args == 2) {
            column = corr(doubleColumn, getDoubleCol(input, 1));
        } else if ("count".equals(name)) {
            column = count(inputColumn);
        } else if ("count_distinct".equals(name) && args >= 1) {
            column = countDistinct(inputColumn, getAdditionalColumns(input));
        } else if ("covar_pop".equals(name) && args == 2) {
            column = covar_pop(doubleColumn, getDoubleCol(input, 1));
        } else if ("covar_samp".equals(name) && args == 2) {
            column = covar_samp(doubleColumn, getDoubleCol(input, 1));
        } else if ("first".equals(name) && args == 2) {
            column = first(inputColumn, (boolean) getArg(input, 1));
        } else if ("kurtosis".equals(name)) {
            column = kurtosis(doubleColumn);
        } else if ("last".equals(name) && args == 2) {
            column = last(inputColumn, (boolean) getArg(input, 1));
        } else if ("max".equals(name)) {
            column = max(inputColumn);
        } else if ("mean".equals(name)) {
            column = mean(doubleColumn);
        } else if ("min".equals(name)) {
            column = min(inputColumn);
        } else if ("skewness".equals(name)) {
            column = skewness(doubleColumn);
        } else if ("stddev_samp".equals(name)) { // alias: stddev
            column = stddev_samp(doubleColumn);
        } else if ("stddev_pop".equals(name)) {
            column = stddev_pop(doubleColumn);
        } else if ("sum".equals(name)) {
            column = sum(inputColumn);
        } else if ("sum_boolean".equals(name)) {
            column = sum(inputColumn.cast(DataTypes.IntegerType));
        } else if ("sum_distinct".equals(name)) {
            column = sumDistinct(inputColumn);
        } else if ("sum_distinct_boolean".equals(name)) {
            column = sumDistinct(inputColumn.cast(DataTypes.IntegerType));
        } else if ("var_samp".equals(name)) { // alias: variance
            column = var_samp(doubleColumn);
        } else if ("var_pop".equals(name)) {
            column = var_pop(doubleColumn);
        } else if ("window".equals(name) && args == 2) {
            column = window(inputColumn, (String) getArg(input, 1));
        } else if ("window".equals(name) && args == 3) {
            column = window(inputColumn, (String) getArg(input, 1), (String) getArg(input, 2));
        } else if ("window".equals(name) && args == 4) {
            column = window(inputColumn, (String) getArg(input, 1), (String) getArg(input, 2), (String) getArg(input, 3));
        } else {
            throw new IllegalArgumentException("Unknown function or parameter (func: " + name + ", args: " + args + ")");
        }

        return column.name(input.getOutputName());
    }

    /**
     * Try to resolve the function return type using literals as input columns.
     * We have to be sure that ALL input columns are resolved / literals.
     *
     * {@inheritDoc}
     */
    @Override
    public IntermediateField getFunctionResultField(final SparkVersion sparkVersion, final IntermediateSpec inputSpec,
        final SparkSQLFunctionJobInput input) {

        TypeConverters.ensureConvertersInitialized(sparkVersion);
        final String function = input.getFunction().toLowerCase();
        final StructType inputSchema = TypeConverters.convertSpec(inputSpec);
        final StructField inputFields[] = inputSchema.fields();
        final String inputColName = (String) getArg(input, 0);
        final Expression outputExpr;

        if ("count".equals(function) || "count_distinct".equals(function)) { // handle count(*) and count(distinct ...)
            return new IntermediateField(input.getOutputName(), IntermediateDataTypes.LONG);

        } else if ("covar_pop".equals(function) || "covar_samp".equals(function)) { // lookup second column argument
            final Column inputColumnA = new Column(
                Literal.create(null, inputFields[inputSchema.fieldIndex(inputColName)].dataType())).name(inputColName);
            final String inputColNameB = (String) getArg(input, 0);
            final Column inputColumnB = new Column(
                Literal.create(null, inputFields[inputSchema.fieldIndex(inputColNameB)].dataType())).name(inputColNameB);
            if ("covar_pop".equals(function)) {
                outputExpr = covar_pop(inputColumnA, inputColumnB).expr();
            } else {
                outputExpr = covar_samp(inputColumnA, inputColumnB).expr();
            }

        } else { // use getFunctionColumn to get expression
            final Column inputColumn = new Column(
                Literal.create(null, inputFields[inputSchema.fieldIndex(inputColName)].dataType())).name(inputColName);
            outputExpr = getFunctionColumn(input, inputColumn).expr();
        }

        final IntermediateToSparkConverter<?> converter = TypeConverters.getConverter(outputExpr.dataType());
        if (converter != null) {
            return new IntermediateField(input.getOutputName(), converter.getIntermediateDataType());
        } else {
            return new IntermediateField(input.getOutputName(), TypeConverters.getDefaultConverter().getIntermediateDataType());
        }
    }

    /** @return converted function argument */
    private static Object getArg(final SparkSQLFunctionJobInput agg, final int i) {
        final IntermediateToSparkConverter<? extends DataType> converter = TypeConverters.getConverter(agg.getArgType(i));
        return converter.convert(agg.getArg(i));
    }

    /** @return additional arguments as string (all arguments except the first) */
    private Seq<Column> getAdditionalColumns(final SparkSQLFunctionJobInput agg) {
        final int args = agg.getArgCount();
        final ArrayList<Column> result = new ArrayList<>(args - 1);
        for (int i = 1; i < args; i++) {
            result.add(column((String) getArg(agg, i)));
        }
        return asScalaBuffer(result);
    }

    /**
     * @param input function job input
     * @param i argument index containing the column name
     * @return input column casted to double
     */
    private static Column getDoubleCol(final SparkSQLFunctionJobInput input, final int i) {
        return column((String) getArg(input, i)).cast(DataTypes.DoubleType);
    }
}
