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
package com.knime.bigdata.spark2_2.jobs.hive;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobInput;
import com.knime.bigdata.spark2_2.api.NamedObjects;
import com.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Executes given SQL statement and puts result into a (named) data frame.
 *
 * @author dwk, jfr
 */
@SparkClass
public class Hive2SparkJob implements SimpleSparkJob<Hive2SparkJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(Hive2SparkJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final Hive2SparkJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        ensureHiveSupport(spark);

        final Dataset<Row> dataFrame = spark.sql(input.getQuery());

        for (final StructField field : dataFrame.schema().fields()) {
            LOGGER.debug("Field '" + field.name() + "' of type '" + field.dataType() + "'");
        }

        final String key = input.getFirstNamedOutputObject();
        LOGGER.info("Storing Hive query result under key: " + key);
        namedObjects.addDataFrame(key, dataFrame);
    }

    private void ensureHiveSupport(final SparkSession spark) throws KNIMESparkException {
        if (!spark.conf().get("spark.sql.catalogImplementation", "").equals("hive")) {
            throw new KNIMESparkException("Spark session does not support hive!"
                + " Please set spark.sql.catalogImplementation = \"hive\" in environment.conf.");
        }
    }
}
