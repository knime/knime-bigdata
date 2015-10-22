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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * append the given input RDDs and store result in new RDD
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
public class ConcatenateRDDsJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(ConcatenateRDDsJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_INPUT_TABLE) || (getInputTableNames(aConfig).size()  < 2)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing or too few tables (minimum 2 required).";
        }

        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     *
     * @param aConfig
     * @return List of named input RDDs
     */
    static List<String> getInputTableNames(final JobConfig aConfig) {
        return aConfig.getInputListParameter(PARAM_INPUT_TABLE, String.class);
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        final List<String> keys = getInputTableNames(aConfig);
        for (String key : keys) {
            if (!validateNamedRdd(key)) {
                msg = "Input data table " + key + " missing!";
                break;
            }
        }

        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting RDD Concatenation job...");
        final List<String> rddNames = getInputTableNames(aConfig);
        final JavaRDD<Row> firstRDD = getFromNamedRdds(rddNames.get(0));
        final List<JavaRDD<Row>> restRDDs = new ArrayList<>();
        for (int i = 1; i < rddNames.size(); i++) {
            JavaRDD<Row> tmp = getFromNamedRdds(rddNames.get(i));
            restRDDs.add(tmp);
        }

        final JavaSparkContext js = JavaSparkContext.fromSparkContext(sc);
        final JavaRDD<Row> res = js.union(firstRDD, restRDDs);
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), res);

        LOGGER.log(Level.INFO, "RDD Concatenation done");
        return JobResult.emptyJobResult().withMessage("OK");
    }
}
