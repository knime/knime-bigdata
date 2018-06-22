/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package org.knime.bigdata.spark2_3.jobs.preproc.missingval;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput.KEY_FIXED_VALUE;
import static org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput.KEY_OP_TYPE;
import static org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput.KEY_OUTPUT_TYPE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput.ReplaceOperation;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobOutput;
import org.knime.bigdata.spark2_3.api.ModelUtils;
import org.knime.bigdata.spark2_3.api.NamedObjects;
import org.knime.bigdata.spark2_3.api.SparkJob;
import org.knime.bigdata.spark2_3.api.TypeConverters;

/**
 * Replace missing values job.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class MissingValueJob implements SparkJob<SparkMissingValueJobInput, SparkMissingValueJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(MissingValueJob.class.getName());

    @Override
    public SparkMissingValueJobOutput runJob(final SparkContext sparkContext, final SparkMissingValueJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException {

        final Dataset<Row> inputDF = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final StructType schema = inputDF.schema();
        final HashSet<String> dropRows = new HashSet<>();
        final HashMap<String, Object> fixedValues = new HashMap<>();
        final HashSet<String> rounded = new HashSet<>();
        final HashSet<String> truncated = new HashSet<>();
        final ArrayList<Column> aggregations = new ArrayList<>();
        final HashSet<String> medianExactColumns = new HashSet<>();
        final HashSet<String> medianApproxColumns = new HashSet<>();
        final HashSet<String> mostFreqColumns = new HashSet<>();
        final HashMap<String, Serializable> outputValues = new HashMap<>();
        final HashMap<String, DataType> outputTypes = new HashMap<>(); // contains casts of columns
        Dataset<Row> outputDF = inputDF;

        LOGGER.info("Loading job input");
        for(StructField field : schema.fields()) {
            Map<String, Serializable> config = input.getConfig(field.name());

            if (config != null) {
                switch((ReplaceOperation) config.get(KEY_OP_TYPE)) {
                    case FIXED_VALUE:
                        final Object value = TypeConverters.getConverter(field.dataType()).convert(config.get(KEY_FIXED_VALUE));
                        fixedValues.put(field.name(), fixedSparkValue(field, value));
                        outputValues.put(field.name(), config.get(KEY_FIXED_VALUE));
                        break;
                    case AVG_ROUNDED:
                        rounded.add(field.name());
                        aggregations.add(avg(field.name()).name(field.name()));
                        break;
                    case AVG_TRUNCATED:
                        truncated.add(field.name());
                        aggregations.add(avg(field.name()).name(field.name()));
                        break;
                    case AVG:
                        aggregations.add(avg(field.name()).name(field.name()));
                        break;
                    case MIN:
                        aggregations.add(min(field.name()).name(field.name()));
                        break;
                    case MAX:
                        aggregations.add(max(field.name()).name(field.name()));
                        break;
                    case MEDIAN_APPROX:
                        medianApproxColumns.add(field.name());
                        break;
                    case MEDIAN_EXACT:
                        medianExactColumns.add(field.name());
                        break;
                    case MOST_FREQ:
                        mostFreqColumns.add(field.name());
                        break;
                    case DROP:
                        dropRows.add(field.name());
                        break;
                }

                if (config.containsKey(KEY_OUTPUT_TYPE)) {
                    outputTypes.put(field.name(),
                        TypeConverters.getConverter((IntermediateDataType)config.get(KEY_OUTPUT_TYPE)).getSparkDataType());
                }
            }
        }

        // cast columns
        if (!outputTypes.isEmpty()) {
            final List<Column> columns = new ArrayList<>(schema.fieldNames().length);
            for (String colName : schema.fieldNames()) {
                if (outputTypes.containsKey(colName)) {
                    columns.add(col(colName).cast(outputTypes.get(colName)));
                } else {
                    columns.add(col(colName));
                }
            }
            outputDF = outputDF.select(columns.toArray(new Column[0]));
        }

        // run standard aggregations
        if (!aggregations.isEmpty()) {
            LOGGER.info("Running aggregations");
            final Dataset<Row> result = outputDF
                    .agg(aggregations.get(0), aggregations.subList(1, aggregations.size()).toArray(new Column[0]));
            final Row row = result.collectAsList().get(0);
            final StructField fields[] = result.schema().fields();
            for(int i = 0; i < fields.length; i++) {
                String column = fields[i].name();
                if (row.isNullAt(i)) {
                    final String agg = input.getConfig(column).get(KEY_OP_TYPE).toString().toLowerCase();
                    throw new KNIMESparkException("Unable to compute " + agg + " for column " + column + ", possibly because there were no values in the column.");
                } else if (rounded.contains(column)) {
                    double value = Math.round(row.getDouble(i));
                    fixedValues.put(column, fixedSparkValue(schema, column, value));
                    outputValues.put(column, intermediateValue(schema, column, value));
                } else if (truncated.contains(column)) {
                    long value = (long) row.getDouble(i);
                    fixedValues.put(column, fixedSparkValue(schema, column, value));
                    outputValues.put(column, intermediateValue(schema, column, value));
                } else {
                    fixedValues.put(column, fixedSparkValue(schema, column, row.get(i)));
                    outputValues.put(column, intermediateValue(schema, column, row.get(i)));
                }
            }
        }

        if (!medianExactColumns.isEmpty()) {
            LOGGER.info("Calculating exact medians");
            calcMedian(outputDF, fixedValues, outputValues, medianExactColumns, 0);
        }

        if (!medianApproxColumns.isEmpty()) {
            LOGGER.info("Calculating approximated medians");
            calcMedian(outputDF, fixedValues, outputValues, medianApproxColumns, 0.001);
        }

        if (!mostFreqColumns.isEmpty()) {
            final String tmpColumn = ModelUtils.getTemporaryColumnName("most-freq");
            for (String column : mostFreqColumns) {
                LOGGER.info("Calculating most frequent on " + column);
                List<Row> results = outputDF.na().drop("any", new String[] { column })
                        .groupBy(column)
                        .agg(count("*").name(tmpColumn))
                        .sort(desc(tmpColumn))
                        .limit(1).collectAsList();
                if (results.isEmpty()) {
                    throw new KNIMESparkException("Unable to compute most frequent value for column " + column + ", possibly because there were no values in the column.");
                } else {
                    Object result = results.get(0).get(0);
                    fixedValues.put(column, fixedSparkValue(schema, column, result));
                    outputValues.put(column, intermediateValue(schema, column, result));
                }
            }
        }

        // drop rows with missing values (this needs to be done last because it changes the results of
        // aggregations).
        if (!dropRows.isEmpty()) {
            LOGGER.info("Dropping rows");
            outputDF = outputDF.na().drop(dropRows.toArray(new String[0]));
        }

        // apply fixed values and aggregation results
        if (!fixedValues.isEmpty()) {
            LOGGER.info("Replacing missing values");
            outputDF = outputDF.na().fill(fixedValues);
        }

        LOGGER.info("Storing result under key: " + input.getFirstNamedOutputObject());
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), outputDF);

        return new SparkMissingValueJobOutput(outputValues);
    }

    /**
     * Converts given value into spark na.fill compatible value.
     * @see DataFrameNaFunctions#fill(Map)
     */
    private Object fixedSparkValue(final StructType schema, final String column, final Object input) {
        return fixedSparkValue(schema.fields()[schema.fieldIndex(column)], input);
    }

    /**
     * Converts given value into spark na.fill compatible value.
     * @see DataFrameNaFunctions#fill(Map)
     */
    private Object fixedSparkValue(final StructField field, final Object input) {
        if (field.dataType() instanceof NumericType || field.dataType().equals(DataTypes.BooleanType)) {
            return input;
        } else {
            return input.toString();
        }
    }

    /** Converts given value into intermediate value. */
    private Serializable intermediateValue(final StructType schema, final String column, final Object input) {
        final DataType sparkType = schema.fields()[schema.fieldIndex(column)].dataType();
        return TypeConverters.getConverter(sparkType).convert(input);
    }

    /**
     * Calculate median using approximate multiple quantiles at once.
     * Available since Spark 2.0, multiple at once since 2.2
     */
    private void calcMedian(final Dataset<Row> input, final Map<String, Object> fixedValues,
        final Map<String, Serializable> outputValues, final Set<String> medianColumns, final double relativeError) throws KNIMESparkException {

        final String columns[] = medianColumns.toArray(new String[0]);
        final StructField fields[] = input.schema().fields();
        final double probabilities[] = new double[] { 0.5 };
        final double medians[][] = input.stat().approxQuantile(columns, probabilities, relativeError);
        for(int i = 0; i < medians.length; i++) {
            final int fieldIndex = input.schema().fieldIndex(columns[i]);
            final double value;
            if (medians[i].length == 0) {
                throw new KNIMESparkException("Unable to compute median for column " + columns[i] + ", possibly because there were no values in the column.");
            } else if (fields[fieldIndex].dataType() instanceof IntegerType) {
                value = new Double(medians[i][0]).intValue();
            } else if (fields[fieldIndex].dataType() instanceof LongType) {
                value = new Double(medians[i][0]).longValue();
            } else {
                value = medians[i][0];
            }
            fixedValues.put(columns[i], fixedSparkValue(input.schema(), columns[i], value));
            outputValues.put(columns[i], intermediateValue(input.schema(), columns[i], value));
        }
    }
}
