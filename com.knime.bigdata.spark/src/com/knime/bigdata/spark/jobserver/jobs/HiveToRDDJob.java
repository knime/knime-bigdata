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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.knime.sparkClient.jobs.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.typesafe.config.Config;

/**
 * executes given HIVE hql statement and puts result into a (named) JavaRDD
 *
 * @author dwk, jfr
 */
public class HiveToRDDJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String PARAM_HQL = ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_HQL_STATEMENT;

    private final static Logger LOGGER = Logger.getLogger(HiveToRDDJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validateWithContext(final SparkContext sc, final Config config) {
        String msg = null;

        if (!config.hasPath(PARAM_HQL)) {
            msg = "Input parameter '" + PARAM_HQL + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is stored in the map of named
     * RDDs
     *
     * @return rdd key
     */
    @Override
    public Object runJobWithContext(final SparkContext sc, final Config aConfig) {
        LOGGER.log(Level.INFO, "reading hive table...");

        final JavaHiveContext hiveContext = new JavaHiveContext(JavaSparkContext.fromSparkContext(sc));
        final String hqlStatement = aConfig.getString(PARAM_HQL);

        final JavaSchemaRDD schemaInputRDD = hiveContext.hql(hqlStatement);

        for (final StructField field : schemaInputRDD.schema().getFields()) {
            LOGGER.log(Level.INFO, "Field '" + field.getName() + "' of type '" + field.getDataType() + "'");
        }

        final RDD<Row> rdd = schemaInputRDD.rdd();

        LOGGER.log(Level.INFO, "done");

        LOGGER.log(Level.INFO, "Storing predicted data under key: " + aConfig.getString(PARAM_HQL));
        addToNamedRdds(aConfig.getString(PARAM_HQL), new JavaRDD<Row>(rdd, rdd.elementClassTag()));
        return aConfig.getString(PARAM_HQL);
    }
}
