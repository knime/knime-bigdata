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
package com.knime.bigdata.spark1_2.jobs.preproc.concatenate;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.concatenate.ConcatenateRDDsJobInput;
import com.knime.bigdata.spark1_2.api.NamedObjects;
import com.knime.bigdata.spark1_2.api.SimpleSparkJob;

/**
 * append the given input RDDs and store result in new RDD
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class ConcatenateRDDsJob implements SimpleSparkJob<ConcatenateRDDsJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(ConcatenateRDDsJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final ConcatenateRDDsJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.info("starting RDD Concatenation job...");
        final List<String> rddNames = input.getNamedInputObjects();
        final JavaRDD<Row> firstRDD = namedObjects.getJavaRdd(rddNames.get(0));
        final List<JavaRDD<Row>> restRDDs = new ArrayList<>();
        for (int i = 1; i < rddNames.size(); i++) {
            restRDDs.add(namedObjects.getJavaRdd(rddNames.get(i)));
        }
        final JavaSparkContext js = JavaSparkContext.fromSparkContext(sparkContext);
        final JavaRDD<Row> res = js.union(firstRDD, restRDDs);
        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), res);
        LOGGER.info("RDD Concatenation done");
    }
}
