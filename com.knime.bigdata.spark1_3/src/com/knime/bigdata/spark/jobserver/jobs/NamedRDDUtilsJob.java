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
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;

import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * helper job to manage named RDDs on the server side
 *
 * @author dwk
 */
public class NamedRDDUtilsJob extends KnimeSparkJob implements Serializable {

    /**The parameter for the input tables to process*/
    public static final String PARAM_INPUT_TABLES = "InputTables";

    /**
     * delete operation
     */
    public static final String OP_DELETE = "delete";

    /**
     * list names of active named RDDs operation
     */
    public static final String OP_INFO = "info";

    private static final long serialVersionUID = 1L;

    /**
     * type of operation
     */
    public static final String PARAM_OP = "OpType";

    private final static Logger LOGGER = Logger.getLogger(NamedRDDUtilsJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig config) {
        String msg = null;

        if (!config.hasInputParameter(PARAM_OP)) {
            msg = "Input parameter '" + PARAM_OP + "' missing.";
        }

        if (msg == null && !config.hasInputParameter(PARAM_INPUT_TABLES)
            && OP_DELETE.equalsIgnoreCase(config.getInputParameter(PARAM_OP).toString())) {
            msg = "Input parameter '" + PARAM_INPUT_TABLES + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * executes operations on named RDDs, currently one of: - remove the given named RDDs from the map of named RDDs -
     * return list of known named RDDs
     *
     * @return "OK" result if named rdds do not exist anymore after execution (deletion), names of known named RDDS
     *         (info), "ERROR" result otherwise
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig) {
        if (aConfig.getInputParameter(PARAM_OP).equalsIgnoreCase(OP_DELETE)) {
            return deleteNamedRDDs(aConfig);
        }
        JobResult res = JobResult.emptyJobResult().withMessage("OK");
        for (String name : RDDUtils.activeNamedRDDs(this)) {
            res = res.withTable(name, null);
        }
        return res;
    }

    /**
     * @param aConfig
     * @return
     */
    private JobResult deleteNamedRDDs(final JobConfig aConfig) {
        final List<String> rddNames = aConfig.getInputListParameter(PARAM_INPUT_TABLES, String.class);
        final List<String> failedDeletes = new LinkedList<String>();
        for (String rddName : rddNames) {
            LOGGER.log(Level.INFO, "deleting reference to named RDD " + rddName);
            deleteNamedRdd(rddName);
            if (validateNamedRdd(rddName)) {
                LOGGER.log(Level.INFO, "failed to delete named RDD " + rddName);
                failedDeletes.add(rddName);
            } else {
                LOGGER.log(Level.INFO, "done deleting named RDD " + rddName);
            }
        }
        if (!failedDeletes.isEmpty()) {
            //TODO: Send the failed rdd names back to the caller
            return JobResult.emptyJobResult().withMessage("ERROR");
        }
        return JobResult.emptyJobResult().withMessage("OK");
    }
}
