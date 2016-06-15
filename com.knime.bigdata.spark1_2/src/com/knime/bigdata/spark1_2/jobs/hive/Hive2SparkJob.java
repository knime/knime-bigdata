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
package com.knime.bigdata.spark1_2.jobs.hive;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobInput;
import com.knime.bigdata.spark1_2.api.NamedObjects;
import com.knime.bigdata.spark1_2.api.SimpleSparkJob;

/**
 * executes given sql statement and puts result into a (named) JavaRDD
 *
 * @author dwk, jfr
 */
@SparkClass
public class Hive2SparkJob implements SimpleSparkJob<Hive2SparkJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Hive2SparkJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final Hive2SparkJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        LOGGER.log(Level.INFO, "reading hive table...");

        final JavaHiveContext hiveContext = new JavaHiveContext(JavaSparkContext.fromSparkContext(sparkContext));
        final String sqlStatement = input.getQuery();
        LOGGER.log(Level.INFO, "sql statement: " + sqlStatement);

        final JavaSchemaRDD schemaInputRDD = hiveContext.sql(sqlStatement);

        for (final StructField field : schemaInputRDD.schema().getFields()) {
            LOGGER.log(Level.FINE, "Field '" + field.getName() + "' of type '" + field.getDataType() + "'");
        }
        final RDD<Row> rdd = schemaInputRDD.rdd();
        final JavaRDD<Row> javaRDD = new JavaRDD<>(rdd, rdd.elementClassTag());

        final String key = input.getFirstNamedOutputObject();
        LOGGER.log(Level.INFO, "Storing Hive query result under key: " + key);
        namedObjects.addJavaRdd(key, javaRDD);
        LOGGER.log(Level.INFO, "done");
    }
}
