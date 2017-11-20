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
package com.knime.bigdata.spark1_6.jobs.preproc.convert.number2category;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import com.knime.bigdata.spark.node.preproc.convert.number2category.Number2CategoryJobInput;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.RDDUtilsInJava;
import com.knime.bigdata.spark1_6.api.RowBuilder;
import com.knime.bigdata.spark1_6.api.SimpleSparkJob;
import com.knime.bigdata.spark1_6.jobs.fetchrows.FetchRowsJob;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Number2CategoryJob implements SimpleSparkJob<Number2CategoryJobInput>{

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(FetchRowsJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext SparkContext, final Number2CategoryJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {
        LOGGER.info("Start column mapping job");
        final JavaRDD<Row> inputRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final ColumnBasedValueMapping map = input.getMapping();
        final JavaRDD<Row> mappedRDD = execute(inputRDD, map, input.keepOriginalColumns());
        LOGGER.info("Mapping done");
        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), mappedRDD);
    }

    /**
     * @param inputRDD
     * @param map
     * @return
     */
    private static JavaRDD<Row> execute(final JavaRDD<Row> inputRDD, final ColumnBasedValueMapping map, final boolean keepOriginalColumns) {
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
        return inputRDD.map(function);
    }

}
