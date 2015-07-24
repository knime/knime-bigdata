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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;

import scala.Tuple2;
import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.JoinMode;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * executes join of two JavaRDD<Row> and puts result into a JavaRDD<Row>
 *
 * @author dwk
 */
public class JoinJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_LEFT_RDD = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_RIGHT_RDD = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_2;

    private static final String PARAM_JOIN_MODE = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_STRING;

    private static final String PARAM_JOIN_INDEXES_LEFT = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 0);

    private static final String PARAM_JOIN_INDEXES_RIGHT = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 1);

    private static final String PARAM_SELECT_INDEXES_LEFT = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2);

    private static final String PARAM_SELECT_INDEXES_RIGHT = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 3);

    private static final String PARAM_RESULT_TABLE_KEY = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(JoinJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;

        if (!aConfig.hasPath(PARAM_LEFT_RDD)) {
            msg = "Input parameter '" + PARAM_LEFT_RDD + "' missing.";
        }

        if (msg == null && !aConfig.hasPath(PARAM_RIGHT_RDD)) {
            msg = "Input parameter '" + PARAM_RIGHT_RDD + "' missing.";
        }

        if (msg == null) {
            msg = checkIntegerListParam(aConfig, PARAM_JOIN_INDEXES_LEFT);
        }
        if (msg == null) {
            msg = checkIntegerListParam(aConfig, PARAM_JOIN_INDEXES_RIGHT);
        }
        if (msg == null) {
            msg = checkIntegerListParam(aConfig, PARAM_SELECT_INDEXES_LEFT);
        }
        if (msg == null) {
            msg = checkIntegerListParam(aConfig, PARAM_SELECT_INDEXES_RIGHT);
        }

        if (msg == null && !aConfig.hasPath(PARAM_JOIN_MODE)) {
            msg = "Input parameter '" + PARAM_JOIN_MODE + "' missing.";
        } else {
            try {
                JoinMode.fromKnimeJoinMode(aConfig.getString(PARAM_JOIN_MODE));
            } catch (Exception e) {
                msg = "Input parameter '" + PARAM_JOIN_MODE + "' has an invalid value.";
            }
        }

        if (msg == null && !aConfig.hasPath(PARAM_RESULT_TABLE_KEY)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE_KEY + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * check the parameter with the given name
     *
     * @param aConfig
     * @param aParamName
     * @return null if NO error is encountered, error message otherwise
     */
    private static String checkIntegerListParam(final Config aConfig, final String aParamName) {
        if (!aConfig.hasPath(aParamName)) {
            return "Input parameter '" + aParamName + "' missing.";
        } else {
            try {
                List<Integer> vals = aConfig.getIntList(aParamName);
                if (vals.size() < 1) {
                    return "Input parameter '" + aParamName + "' is empty.";
                }
            } catch (ConfigException e) {
                return "Input parameter '" + aParamName + "' is not of expected type 'integer list'.";
            }
        }
        return null;
    }

    private void validateInput(final Config aConfig) throws GenericKnimeSparkException {
        String msg = null;
        {
            final String key = aConfig.getString(PARAM_LEFT_RDD);
            if (key == null) {
                msg = "Input parameter at port 1 is missing!";
            } else if (!validateNamedRdd(key)) {
                msg = "Left join data table missing!";
            }
        }
        {
            final String key = aConfig.getString(PARAM_RIGHT_RDD);
            if (key == null) {
                msg = "Input parameter at port 2 is missing!";
            } else if (!validateNamedRdd(key)) {
                msg = "Right join data table missing!";
            }
        }

        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is stored in the map of named
     * RDDs
     *
     * @return OK
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        validateInput(aConfig);
        final JoinMode mode = JoinMode.fromKnimeJoinMode(aConfig.getString(PARAM_JOIN_MODE));
        LOGGER.log(Level.INFO, "computing " + mode.toString() + " of two RDDs...");

        List<Integer> joinIdxLeft = aConfig.getIntList(PARAM_JOIN_INDEXES_LEFT);
        JavaPairRDD<Object[], Row> leftRdd = RDDUtilsInJava.extractKeys(getFromNamedRdds(aConfig.getString(PARAM_LEFT_RDD)), joinIdxLeft.toArray(new Integer[joinIdxLeft.size()]));
        List<Integer> joinIdxRight = aConfig.getIntList(PARAM_JOIN_INDEXES_RIGHT);
        JavaPairRDD<Object[], Row> rightRdd = RDDUtilsInJava.extractKeys(getFromNamedRdds(aConfig.getString(PARAM_RIGHT_RDD)), joinIdxRight.toArray(new Integer[joinIdxRight.size()]));

        JavaPairRDD<Object[], Tuple2<Row, Row>> joinedRdd = leftRdd.join(rightRdd);
        List<Integer> colIdxLeft = aConfig.getIntList(PARAM_SELECT_INDEXES_LEFT);
        List<Integer> colIdxRight = aConfig.getIntList(PARAM_SELECT_INDEXES_RIGHT);
        JavaRDD<Row> resultRdd = RDDUtilsInJava.mergeRows(joinedRdd.values(), colIdxLeft, colIdxRight);

        LOGGER.log(Level.INFO, "done");

        final String key = aConfig.getString(PARAM_RESULT_TABLE_KEY);
        LOGGER.log(Level.INFO, "Storing join result under key: " + key);
        addToNamedRdds(key,  resultRdd);
        return JobResult.emptyJobResult().withMessage("OK");
    }
}
