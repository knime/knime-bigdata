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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark1_5.jobs.preproc.sorter;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.MultiValueSortKey;
import com.knime.bigdata.spark.node.preproc.sorter.SortJobInput;
import com.knime.bigdata.spark1_5.base.NamedObjects;
import com.knime.bigdata.spark1_5.base.SimpleSparkJob;

/**
 * sorts input RDD by given indices, in given order
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class SortJob implements SimpleSparkJob<SortJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(SortJob.class.getName());


    @Override
    public void runJob(final SparkContext sparkContext, final SortJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {
        LOGGER.info("starting RDD Sort job...");
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final Integer[] colIdxs = input.getFeatureColIdxs();
        final Boolean[] sortOrders = input.isSortDirectionAscending();
        final Boolean missingToEnd = input.missingToEnd();
        System.out.println("Missing to end? " + missingToEnd);
        final JavaRDD<Row> res = execute(sparkContext.defaultMinPartitions(), rowRDD, colIdxs, sortOrders, missingToEnd);
        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), res);

        LOGGER.info("RDD Sort done");

    }

    static JavaRDD<Row> execute(final int numPartitions, final JavaRDD<Row> rowRDD, final Integer[] colIdxs,
        final Boolean[] sortOrders, final Boolean missingToEnd) {
        //special (and more efficient) handling of sorting by a single key:
        if (colIdxs.length == 1) {
            return rowRDD.sortBy(new Function<Row, Object>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object call(final Row aRow) throws Exception {
                    return aRow.get(colIdxs[0]);
                }
            }, sortOrders[0], numPartitions);
        } else {
            return rowRDD.sortBy(new Function<Row, MultiValueSortKey>() {
                private static final long serialVersionUID = 1L;

                @Override
                public MultiValueSortKey call(final Row aRow) throws Exception {
                    final Object[] values = new Object[colIdxs.length];
                    final Boolean[] isAscending = new Boolean[sortOrders.length];
                    for (int i=0; i<values.length; i++) {
                        values[i] = aRow.get(colIdxs[i]);
                        isAscending[i] = sortOrders[i];
                    }
                    return new MultiValueSortKey(values, isAscending, missingToEnd);
                }
            }, true, numPartitions);
        }
    }
}
