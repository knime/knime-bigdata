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
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;

/**
 * helper job to manage named RDDs on the server side
 *
 * @author dwk
 */
public class NamedRDDUtilsJob extends KnimeSparkJob implements Serializable {

    /**
     * delete operation
     */
    public static final String OP_DELETE = "delete";

    /**
     * list names of active named RDDs operation
     */
    public static final String OP_INFO = "info";

    private static final long serialVersionUID = 1L;

    private static final String PARAM_TABLE_KEY = ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_OP = ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_STRING;

    private final static Logger LOGGER = Logger.getLogger(NamedRDDUtilsJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final Config config) {
        String msg = null;

        if (!config.hasPath(PARAM_OP)) {
            msg = "Input parameter '" + PARAM_OP + "' missing.";
        }

        if (msg != null && !config.hasPath(PARAM_TABLE_KEY) && config.getString(PARAM_OP).equalsIgnoreCase(OP_DELETE)) {
            msg = "Input parameter '" + PARAM_TABLE_KEY + "' missing.";
        }


        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * executes operations on named RDDs, currently one of:
     * - remove the given named RDD from the map of named RDDs
     * - return list of known named RDDs
     *
     * @return "OK" result if named rdd does not exist anymore after execution (deletion),
     * names of known named RDDS (info), "ERROR" result otherwise
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig)  {
        if (aConfig.getString(PARAM_OP).equalsIgnoreCase(OP_DELETE)) {
            return deleteNamedRDD(aConfig);
        }
        JobResult res = JobResult.emptyJobResult();
        for (String name : RDDUtils.activeNamedRDDs(this)) {
            res = res.withTable(name, null);
        }
        return res.withMessage("OK");

    }

    /**
     * @param aConfig
     * @return
     */
    private JobResult deleteNamedRDD(final Config aConfig) {
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
