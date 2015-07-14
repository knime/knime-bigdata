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

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;

/**
 * helper job to manage named RDDs on the server side
 *
 * @author dwk
 */
public class NamedRDDUtilsJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String PARAM_TABLE_KEY = ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(NamedRDDUtilsJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final Config config) {
        String msg = null;

        if (!config.hasPath(PARAM_TABLE_KEY)) {
            msg = "Input parameter '" + PARAM_TABLE_KEY + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * remove the given named RDD from the map of named RDDs
     *
     * @return "OK" result of named rdd does not exist anymore after execution, "ERROR" result otherwise
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig)  {
        final String rddName = aConfig.getString(PARAM_TABLE_KEY);
        LOGGER.log(Level.INFO, "deleting reference to named RDD " + rddName);
        deleteNamedRdd(rddName);
        if (validateNamedRdd(rddName)) {
            LOGGER.log(Level.INFO, "failed");
            return JobResult.emptyJobResult().withMessage("ERROR");
        } else {
            LOGGER.log(Level.INFO, "done");
            return JobResult.emptyJobResult().withMessage("OK");
        }
    }
}
