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
 *
 * History
 *   Created on 21.08.2015 by koetter
 */
package com.knime.bigdata.spark2_2.jobs.preproc.convert.number2category;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import com.knime.bigdata.spark.node.preproc.convert.number2category.Number2CategoryJobInput;
import com.knime.bigdata.spark2_2.api.NamedObjects;
import com.knime.bigdata.spark2_2.api.RDDUtilsInJava;
import com.knime.bigdata.spark2_2.api.RowBuilder;
import com.knime.bigdata.spark2_2.api.SimpleSparkJob;
import com.knime.bigdata.spark2_2.jobs.fetchrows.FetchRowsJob;

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
        final SparkSession spark = SparkSession.builder().getOrCreate();
        final List<Integer> idxs = map.getColumnIndices();
        final Function<Row, Row> function = new Function<Row, Row>(){
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
        final StructType schema = RDDUtilsInJava.createSchema(input, map, keepOriginalColumns, colSuffix);

        return spark.createDataFrame(input.javaRDD().map(function), schema);
    }
}
