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
package org.knime.bigdata.spark3_4.jobs.preproc.groupby;

import static org.apache.spark.sql.functions.col;
import static scala.collection.JavaConversions.asScalaBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionFactory;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.preproc.groupby.SparkGroupByJobInput;
import org.knime.bigdata.spark.node.preproc.groupby.SparkGroupByJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

/**
 * Executes a Spark group by or pivot job.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class GroupByJob implements SparkJob<SparkGroupByJobInput, SparkGroupByJobOutput> {
    private static final long serialVersionUID = 1L;
    private final HashMap<String, SparkSQLFunctionFactory<Column>> m_factories = new HashMap<>();

    @Override
    public SparkGroupByJobOutput runJob(final SparkContext sparkContext, final SparkGroupByJobInput input,
        final NamedObjects namedObjects) throws Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final String namedOutputObject = input.getFirstNamedOutputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);
        ensureFunctionFactoriesLoaded(input.getGroupByFunctions());
        ensureFunctionFactoriesLoaded(input.getAggregateFunctions());
        final List<Column> aggColumns = getFunctionColumns(input.getAggregateFunctions());

        final Dataset<Row> resultFrame;
        boolean pivotValuesWereDropped = false;
        if (input.usePivoting()) {
            // pivoting mode

            List<Object> pivotValues = getPivotValues(inputFrame, input);
            if (input.computePivotValues() && pivotValues.size() > input.getComputePivotValuesLimit()) {
                pivotValuesWereDropped = true;
                pivotValues = pivotValues.subList(0, input.getComputePivotValuesLimit());
            }

            resultFrame = doPivoting(inputFrame, input, aggColumns, pivotValues);

        } else { // group by mode
            resultFrame = inputFrame
                    .groupBy(asScalaBuffer(getFunctionColumns(input.getGroupByFunctions())))
                    .agg(aggColumns.get(0), aggColumns.subList(1, aggColumns.size()).toArray(new Column[0]));
        }

        namedObjects.addDataFrame(namedOutputObject, resultFrame);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(resultFrame.schema());
        return new SparkGroupByJobOutput(namedOutputObject, outputSchema, pivotValuesWereDropped);
    }

    private Dataset<Row> doPivoting(final Dataset<Row> inputFrame, final SparkGroupByJobInput input,
        final List<Column> aggCols, final List<Object> pivotValues) throws Exception {

        // validate that pivot list contains all values
        final Dataset<Row> validatedFrame;
        if (input.validateValues()) {
            final int colIdx = inputFrame.schema().fieldIndex(input.getPivotColumn());
            validatedFrame =
                inputFrame.map(new ValidateValuesMapper(colIdx, pivotValues), RowEncoder.apply(inputFrame.schema()));
        } else {
            validatedFrame = inputFrame;
        }

        // final column names, in the order as they are produced by Spark
        final List<String> outputColumns = computeOutputColumns(input, pivotValues);

        // uniquify groupBy columns to avoid collisions with new pivot columns
        final ArrayList<Column> groupByAliased = new ArrayList<>();
        for (SparkSQLFunctionJobInput groupByCol : input.getGroupByFunctions()) {
            final String tmpAlias = groupByCol.getOutputName() + UUID.randomUUID().toString();
            groupByAliased.add(col(groupByCol.getOutputName()).as(tmpAlias));
        }

        return validatedFrame
                .groupBy(asScalaBuffer(groupByAliased))
                .pivot(input.getPivotColumn(), pivotValues)
                .agg(aggCols.get(0), aggCols.subList(1, aggCols.size()).toArray(new Column[0]))
                .toDF(outputColumns.toArray(new String[0])); // rewrite column names from uniquified/Spark-generated column names
    }

    private List<String> computeOutputColumns(final SparkGroupByJobInput input, final List<Object> pivotValues)
        throws KNIMESparkException {

        final Set<String> outputColumns = new LinkedHashSet<>();

        // grouping columns come first
        for (SparkSQLFunctionJobInput groupByCol : input.getGroupByFunctions()) {
            final String groupColumnName = groupByCol.getOutputName();
            outputColumns.add(groupColumnName);
        }

        for (Object pivotValue : pivotValues) {
            final String pivotString = (pivotValue != null) ? pivotValue.toString() : "?";

            for (SparkSQLFunctionJobInput aggFunc : input.getAggregateFunctions()) {
                final String outputPivotColumnName = String.format("%s+%s", pivotString, aggFunc.getOutputName());

                if (!outputColumns.add(outputPivotColumnName)) {
                    String msg = String.format("Duplicate column '%s' in resulting table.\n", outputPivotColumnName);
                    if (pivotValue == null || pivotString.equals("?")) {
                        msg +=
                            " Please adjust the column naming scheme and/or 'Ignore missing values' to prevent this.";
                    } else {
                        // conflict is unrelated to missing values -> conflict is between a group column and pivot value -> solvable via column naming scheme.
                        msg +=
                            " Please adjust the column naming strategy, or rename the grouping column before pivoting.";
                    }

                    throw new KNIMESparkException(msg);
                }
            }
        }

        return new LinkedList<String>(outputColumns);
    }

    /** Load required {@link SparkSQLFunctionFactory}s */
    @SuppressWarnings("unchecked")
    private void ensureFunctionFactoriesLoaded(final SparkSQLFunctionJobInput[] functions) throws Exception {
        for (SparkSQLFunctionJobInput agg : functions) {
            if (!m_factories.containsKey(agg.getFactoryName())) {
                final Object obj = getClass().getClassLoader().loadClass(agg.getFactoryName()).newInstance();
                if (obj instanceof SparkSQLFunctionFactory<?>) {
                    m_factories.put(agg.getFactoryName(), (SparkSQLFunctionFactory<Column>) obj);
                } else {
                    throw new IllegalArgumentException("Unknown aggregation function factory type");
                }
            }
        }
    }

    /** @return List of column expressions of given functions */
    private List<Column> getFunctionColumns(final SparkSQLFunctionJobInput[] functions) throws Exception {
        final ArrayList<Column> columns = new ArrayList<>();

        for (SparkSQLFunctionJobInput function : functions) {
            columns.add(m_factories.get(function.getFactoryName()).getFunctionColumn(function));
        }

        return columns;
    }

    /** @return List of manual provided or computed pivoting values or empty list in group by mode */
    private List<Object> getPivotValues(final Dataset<Row> data, final SparkGroupByJobInput input) throws KNIMESparkException {
        if (input.computePivotValues()) {

            final Dataset<Row> tmp;

            if (input.ignoreMissingValuesInPivotColumn()) {
                // filter out missing values
                tmp = data.select(input.getPivotColumn()).na().drop();
            } else {
                tmp = data.select(input.getPivotColumn());
            }

            return tmp
                    .distinct()
                    .sort(input.getPivotColumn())
                    // take one more value than the limit, so we know when we are dropping values
                    .takeAsList(input.getComputePivotValuesLimit() + 1)
                    .stream()
                    .map(r -> r.get(0))
                    .collect(Collectors.toList());

        } else {
            return Arrays.<Object>asList(input.getPivotValues());
        }
    }
}
