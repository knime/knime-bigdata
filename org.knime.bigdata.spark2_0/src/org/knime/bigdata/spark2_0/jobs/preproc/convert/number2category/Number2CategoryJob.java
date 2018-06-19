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
 *   Created on 21.08.2015 by koetter
 */
package org.knime.bigdata.spark2_0.jobs.preproc.convert.number2category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.node.preproc.convert.number2category.Number2CategoryJobInput;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.RDDUtilsInJava;
import org.knime.bigdata.spark2_0.api.RowBuilder;
import org.knime.bigdata.spark2_0.api.SimpleSparkJob;
import org.knime.bigdata.spark2_0.jobs.fetchrows.FetchRowsJob;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Number2CategoryJob implements SimpleSparkJob<Number2CategoryJobInput>{
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FetchRowsJob.class.getName());

    @Override
    public void runJob(final SparkContext SparkContext, final Number2CategoryJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException {
        LOGGER.info("Start column mapping job");
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final ColumnBasedValueMapping map = input.getMapping();
        final Dataset<Row> mappedDataset = execute(inputDataset, map, input.keepOriginalColumns(), input.getColSuffix());
        LOGGER.info("Mapping done");
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), mappedDataset);
    }

    /**
     * @param inputRDD
     * @param map
     * @return
     */
    private static Dataset<Row> execute(final Dataset<Row> input, final ColumnBasedValueMapping map,
            final boolean keepOriginalColumns, final String colSuffix) {

        final Set<Integer> idxs = new TreeSet<>(map.getColumnIndices());
        final MapFunction<Row, Row> function = new MapFunction<Row, Row>(){
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row r) throws Exception {
                final RowBuilder rowBuilder;

                if (keepOriginalColumns) {
                    rowBuilder = RowBuilder.fromRow(r);
                } else {
                    rowBuilder = RDDUtilsInJava.dropColumnsFromRow(idxs,  r);
                }

                for (final Integer idx : idxs) {
                    final Object object = r.get(idx);
                    final Object mapVal = map.map(idx, object);
                    rowBuilder.add(mapVal);
                }

                return rowBuilder.build();
            }
        };
        final StructType schema = createSchema(input, map, keepOriginalColumns, colSuffix);
        return input.map(function, RowEncoder.apply(schema));
    }

    /**
     * Create a number to category schema.
     *
     * @param dataset
     * @param map
     * @param keepOriginalColumns
     * @param colSuffix column suffix to append
     * @return Number to category schema
     */
    public static StructType createSchema(final Dataset<Row> dataset, final ColumnBasedValueMapping map,
            final boolean keepOriginalColumns, final String colSuffix) {

        StructType schema = dataset.schema();
        List<Integer> columnIdsList = map.getColumnIndices();
        Collections.sort(columnIdsList);
        List<StructField> fields = new ArrayList<>();
        StructField oldFields[] = schema.fields();

        for (int i = 0; i < oldFields.length; i++) {
            if (keepOriginalColumns || !columnIdsList.contains(i)) {
                fields.add(oldFields[i]);
            }
        }

        for (int idx : columnIdsList) {
            String name = oldFields[idx].name() + colSuffix;
            fields.add(DataTypes.createStructField(name, DataTypes.StringType, false));
        }

        return DataTypes.createStructType(fields);
    }
}
