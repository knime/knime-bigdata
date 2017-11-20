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
package org.knime.bigdata.spark2_2.jobs.util;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.util.rdd.unpersist.UnpersistJobInput;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Unpersists the given named RDD and remove it from the named objects.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class UnpersistJob implements SimpleSparkJob<UnpersistJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(UnpersistJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final UnpersistJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        final String key = input.getFirstNamedInputObject();
        final Dataset<Row> dataFrame = namedObjects.getDataFrame(key);
        LOGGER.info("Unpersisting: " + key);
        dataFrame.unpersist();
        namedObjects.deleteNamedDataFrame(key);
        LOGGER.info(key + " successful unpersisted");
    }
}
