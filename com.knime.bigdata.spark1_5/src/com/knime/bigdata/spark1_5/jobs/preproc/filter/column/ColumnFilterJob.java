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
package com.knime.bigdata.spark1_5.jobs.preproc.filter.column;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.ColumnsJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark1_5.base.NamedObjects;
import com.knime.bigdata.spark1_5.base.RDDUtilsInJava;
import com.knime.bigdata.spark1_5.base.SimpleSparkJob;

/**
 * select given columns from input table and store result in new RDD
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class ColumnFilterJob implements SimpleSparkJob<ColumnsJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(ColumnFilterJob.class.getName());


    @Override
    public void runJob(final SparkContext sparkContext, final ColumnsJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {
        LOGGER.info("starting Column Selection job...");
        final List<String> rddNames = input.getNamedInputObjects();
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(rddNames.get(0));
        final List<Integer> colIdxs = input.getColumnIdxs();

        //use only the column indices when converting to vector
        final JavaRDD<Row> res = RDDUtilsInJava.selectColumnsFromRDD(rowRDD, colIdxs);
        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), res);

        LOGGER.info("Column Selection done");
    }
}
