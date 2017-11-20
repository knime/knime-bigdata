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
package com.knime.bigdata.spark2_2.jobs.preproc.groupby;

import static scala.collection.JavaConversions.asScalaBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.sql_function.SparkSQLFunctionFactory;
import com.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.node.preproc.groupby.SparkGroupByJobInput;
import com.knime.bigdata.spark.node.preproc.groupby.SparkGroupByJobOutput;
import com.knime.bigdata.spark2_2.api.NamedObjects;
import com.knime.bigdata.spark2_2.api.SparkJob;
import com.knime.bigdata.spark2_2.api.TypeConverters;

/**
 * Executes a Spark group by and/or aggregation.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class GroupByJob implements SparkJob<SparkGroupByJobInput, SparkGroupByJobOutput> {
    private final static long serialVersionUID = 1L;
    private final HashMap<String, SparkSQLFunctionFactory<Column>> m_factories = new HashMap<>();

    @Override
    public SparkGroupByJobOutput runJob(final SparkContext sparkContext, final SparkGroupByJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final String namedOutputObject = input.getFirstNamedOutputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);
        final List<Column> groupBy = getFunctionColumns(input.getGroupByFunctions());
        final List<Column> aggColumns = getFunctionColumns(input.getAggregateFunctions());
        final Dataset<Row> resultFrame = inputFrame
                .groupBy(asScalaBuffer(groupBy))
                .agg(aggColumns.get(0), aggColumns.subList(1, aggColumns.size()).toArray(new Column[0]));
        namedObjects.addDataFrame(namedOutputObject, resultFrame);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(resultFrame.schema());
        return new SparkGroupByJobOutput(namedOutputObject, outputSchema);
    }

    /** @return List of column expressions of given functions */
    @SuppressWarnings("unchecked")
    private List<Column> getFunctionColumns(final SparkSQLFunctionJobInput[] functions) throws Exception {
        final ArrayList<Column> columns = new ArrayList<>();
        for (SparkSQLFunctionJobInput agg : functions) {
            if (!m_factories.containsKey(agg.getFactoryName())) {
                final Object obj = getClass().getClassLoader().loadClass(agg.getFactoryName()).newInstance();
                if (obj instanceof SparkSQLFunctionFactory<?>) {
                    m_factories.put(agg.getFactoryName(), (SparkSQLFunctionFactory<Column>) obj);
                } else {
                    throw new IllegalArgumentException("Unknown aggregation function factory type");
                }
            }

            columns.add(m_factories.get(agg.getFactoryName()).getFunctionColumn(agg));
        }

        return columns;
    }
}
