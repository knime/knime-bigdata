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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark1_5.jobs.util;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.util.rdd.unpersist.UnpersistJobInput;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SimpleSparkJob;

/**
 * Unpersists the given RDD.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class UnpersistJob implements SimpleSparkJob<UnpersistJobInput> {

    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(UnpersistJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final UnpersistJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        final String name = input.getFirstNamedInputObject();
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(name);
        LOGGER.info("Unpersisting: " + name);
        rowRDD.unpersist();
        LOGGER.info(name + " successful unpersisted");
    }
}
