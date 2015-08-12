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

import com.google.common.base.Optional;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.JoinMode;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.MyJoinKey;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * executes join of two JavaRDD<Row> and puts result into a JavaRDD<Row>
 *
 * @author dwk
 */
public class JoinJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_LEFT_RDD = ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_RIGHT_RDD = ParameterConstants.PARAM_TABLE_2;

    private static final String PARAM_JOIN_MODE = ParameterConstants.PARAM_STRING;

    private static final String PARAM_JOIN_INDEXES_LEFT = ParameterConstants.NUMBERED_PARAM(
        ParameterConstants.PARAM_COL_IDXS, 0);

    private static final String PARAM_JOIN_INDEXES_RIGHT = ParameterConstants.NUMBERED_PARAM(
        ParameterConstants.PARAM_COL_IDXS, 1);

    private static final String PARAM_SELECT_INDEXES_LEFT = ParameterConstants.NUMBERED_PARAM(
        ParameterConstants.PARAM_COL_IDXS, 2);

    private static final String PARAM_SELECT_INDEXES_RIGHT = ParameterConstants.NUMBERED_PARAM(
        ParameterConstants.PARAM_COL_IDXS, 3);

    private static final String PARAM_RESULT_TABLE_KEY = ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(JoinJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_LEFT_RDD)) {
            msg = "Input parameter '" + PARAM_LEFT_RDD + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_RIGHT_RDD)) {
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

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_JOIN_MODE)) {
                msg = "Input parameter '" + PARAM_JOIN_MODE + "' missing.";
            } else {
                try {
                    JoinMode.fromKnimeJoinMode(aConfig.getInputParameter(PARAM_JOIN_MODE));
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_JOIN_MODE + "' has an invalid value.";
                }
            }
        }

        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE_KEY)) {
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
    private static String checkIntegerListParam(final JobConfig aConfig, final String aParamName) {
        if (!aConfig.hasInputParameter(aParamName)) {
            return "Input parameter '" + aParamName + "' missing.";
        } else {
            try {
                List<Integer> vals = aConfig.getInputListParameter(aParamName, Integer.class);
                if (vals.size() < 1) {
                    return "Input parameter '" + aParamName + "' is empty.";
                }
            } catch (Exception e) {
                return "Input parameter '" + aParamName + "' is not of expected type 'integer list'.";
            }
        }
        return null;
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        {
            final String key = aConfig.getInputParameter(PARAM_LEFT_RDD);
            if (key == null) {
                msg = "Input parameter at port 1 is missing!";
            } else if (!validateNamedRdd(key)) {
                msg = "Left join data table missing!";
            }
        }
        {
            final String key = aConfig.getInputParameter(PARAM_RIGHT_RDD);
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
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        final JoinMode mode = JoinMode.fromKnimeJoinMode(aConfig.getInputParameter(PARAM_JOIN_MODE));
        LOGGER.log(Level.INFO, "computing " + mode.toString() + " of two RDDs...");

        final List<Integer> joinIdxLeft = aConfig.getInputListParameter(PARAM_JOIN_INDEXES_LEFT, Integer.class);
        JavaPairRDD<MyJoinKey, Row> leftRdd =
            RDDUtilsInJava.extractKeys(getFromNamedRdds(aConfig.getInputParameter(PARAM_LEFT_RDD)),
                joinIdxLeft.toArray(new Integer[joinIdxLeft.size()]));
        final List<Integer> joinIdxRight = aConfig.getInputListParameter(PARAM_JOIN_INDEXES_RIGHT, Integer.class);
        JavaPairRDD<MyJoinKey, Row> rightRdd =
            RDDUtilsInJava.extractKeys(getFromNamedRdds(aConfig.getInputParameter(PARAM_RIGHT_RDD)),
                joinIdxRight.toArray(new Integer[joinIdxRight.size()]));

        final List<Integer> colIdxLeft = aConfig.getInputListParameter(PARAM_SELECT_INDEXES_LEFT, Integer.class);
        final List<Integer> colIdxRight = aConfig.getInputListParameter(PARAM_SELECT_INDEXES_RIGHT, Integer.class);

        //printRDD(leftRdd.collect(), "Left table:");
        //printRDD(rightRdd.collect(), "Right table:");
        final JavaRDD<Row> resultRdd;
        switch (mode) {
            case InnerJoin: {
                JavaRDD<Tuple2<Row, Row>> joinedRdd = leftRdd.join(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case LeftOuterJoin: {
                JavaRDD<Tuple2<Row, Optional<Row>>> joinedRdd = leftRdd.leftOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case RightOuterJoin: {
                JavaRDD<Tuple2<Optional<Row>, Row>> joinedRdd = leftRdd.rightOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case FullOuterJoin: {
                JavaRDD<Tuple2<Optional<Row>, Optional<Row>>> joinedRdd = leftRdd.fullOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            default: {
                throw new GenericKnimeSparkException("ERROR: unsupported join mode: " + mode);
            }
        }

        //printJoinedRDD(joinedRdd.collect(), "Joined table:");

        //printSelectedRDD(resultRdd.collect(), "Result table:");

        LOGGER.log(Level.INFO, "done");

        final String key = aConfig.getOutputStringParameter(PARAM_RESULT_TABLE_KEY);
        LOGGER.log(Level.INFO, "Storing join result under key: " + key);
        addToNamedRdds(key, resultRdd);
        return JobResult.emptyJobResult().withMessage("OK");
    }

    /**
     * @param collect
     * @param string
     */
    @SuppressWarnings("unused")
    private void printSelectedRDD(final List<Row> aRdd, final String aMsg) {
        LOGGER.log(Level.INFO, aMsg);
        for (Row tuple : aRdd) {
            LOGGER.log(Level.INFO, tuple.toString());
        }
        LOGGER.log(Level.INFO, "<---- END OF TABLE");
    }

    /**
     * @param collect
     * @param aMsg
     */
    @SuppressWarnings("unused")
    private void printJoinedRDD(final List<Tuple2<String, Tuple2<Row, Row>>> aRdd, final String aMsg) {
        LOGGER.log(Level.INFO, aMsg);
        for (Tuple2<String, Tuple2<Row, Row>> tuple : aRdd) {
            LOGGER.log(Level.INFO, "keys:\t" + (tuple._1) + "\tvalues: " + tuple._2);
        }
        LOGGER.log(Level.INFO, "<---- END OF TABLE");
    }

    /**
     * @param collect
     */
    @SuppressWarnings("unused")
    private void printRDD(final List<Tuple2<String, Row>> aRdd, final String aMsg) {
        LOGGER.log(Level.INFO, aMsg);
        for (Tuple2<String, Row> tuple : aRdd) {
            LOGGER.log(Level.INFO, "keys:\t" + (tuple._1) + "\tvalues: " + tuple._2);
        }
        LOGGER.log(Level.INFO, "<---- END OF TABLE");
    }

}
