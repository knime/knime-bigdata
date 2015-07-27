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
 *   Created on 27.07.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.CheckForNull;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;

import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 *
 * @author dwk
 */
public class SupervisedLearnerUtils {

    /**
     * name of parameter with training data
     */
    public static final String PARAM_TRAINING_RDD = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    /**
     * selector for columns to be used for learning
     */
    public static final String PARAM_COL_IDXS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_COL_IDXS;

    /**
     * names of the columns (must include label column), required for value mapping info
     */
    public static final String PARAM_COL_NAMES = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_COL_IDXS + ParameterConstants.PARAM_STRING;

    /**
     * index of label column
     */
    public static final String PARAM_LABEL_INDEX = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_LABEL_INDEX;

    /**
     * table with feature and label mappings
     */
    public static final String PARAM_MAPPING_TABLE = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_2;

    /**
     * parameter name for table with model predictions
     */
    public static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    /**
     * @param aConfig
     * @param aRowRDD
     * @return LabeledPoint RDD with training data
     */
    public static JavaRDD<LabeledPoint> getTrainingData(final Config aConfig, final JavaRDD<Row> aRowRDD) {
        final List<Integer> colIdxs = aConfig.getIntList(PARAM_COL_IDXS);

        //note: requires that all features (including the label) are numeric !!!
        final int labelIndex = aConfig.getInt(PARAM_LABEL_INDEX);
        final JavaRDD<LabeledPoint> inputRdd = RDDUtilsInJava.toJavaLabeledPointRDD(aRowRDD, colIdxs, labelIndex);
        return inputRdd;
    }

    /**
     * @param aConfig
     * @return error message if at least one param is invalid, null otherwise
     */
    @CheckForNull
    public static String checkConfig(final Config aConfig) {
        if (!aConfig.hasPath(PARAM_LABEL_INDEX)) {
            return "Input parameter '" + PARAM_LABEL_INDEX + "' missing.";
        } else {
            try {
                aConfig.getInt(PARAM_LABEL_INDEX);
            } catch (ConfigException e) {
                return "Input parameter '" + PARAM_LABEL_INDEX + "' is not of expected type 'integer'.";
            }
        }

        if (!aConfig.hasPath(PARAM_COL_IDXS)) {
            return "Input parameter '" + PARAM_COL_IDXS + "' missing.";
        } else {
            try {
                aConfig.getIntList(PARAM_COL_IDXS);
            } catch (ConfigException e) {
                e.printStackTrace();
                return "Input parameter '" + PARAM_COL_IDXS + "' is not of expected type 'integer list'.";
            }
        }

        if (!aConfig.hasPath(PARAM_COL_NAMES)) {
            return "Input parameter '" + PARAM_COL_NAMES + "' missing.";
        } else {
            try {
                List<String> names = aConfig.getStringList(PARAM_COL_NAMES);
                if (names.size() != aConfig.getIntList(PARAM_COL_IDXS).size() + 1) {
                    return "Input parameter '"
                        + PARAM_COL_NAMES
                        + "' is of unexpected length. It must have one entry for each select input column and 1 for the label column.";
                }
            } catch (ConfigException e) {
                return "Input parameter '" + PARAM_COL_NAMES + "' is not of expected type 'string list'.";
            }
        }
        return null;
    }

    /**
     * validate that required named RDD exist
     *
     * @param aConfig
     * @param aJob
     * @param aLogger
     * @throws GenericKnimeSparkException
     */
    public static void validateInput(final Config aConfig, final KnimeSparkJob aJob, final Logger aLogger)
        throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getString(SupervisedLearnerUtils.PARAM_TRAINING_RDD);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!aJob.validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (aConfig.hasPath(PARAM_MAPPING_TABLE)) {
            final String mappingTable = aConfig.getString(PARAM_MAPPING_TABLE);
            if (!aJob.validateNamedRdd(mappingTable)) {
                msg = "Input table with value mappings is missing!";
            }
        }

        if (msg != null) {
            aLogger.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * @param aConfig
     * @param model
     * @throws GenericKnimeSparkException
     */
    public static void storePredictions(final SparkContext sc, final Config aConfig, final KnimeSparkJob aJob,
        final JavaRDD<Row> aInputRdd, final JavaRDD<Vector> aFeatures, final Serializable aModel, final Logger aLogger)
        throws GenericKnimeSparkException {
        aLogger.log(Level.INFO, "Storing predicted data under key: " + aConfig.getString(PARAM_OUTPUT_DATA_PATH));
        //TODO - revert the label to int mapping ????
        JavaRDD<Row> predictedData = ModelUtils.predict(sc, aFeatures, aInputRdd, aModel);
        try {
            aJob.addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH), predictedData);
        } catch (Exception e) {
            aLogger.severe("ERROR: failed to predict and store results for training data.");
            aLogger.severe(e.getMessage());
        }
    }


    /**
     * @param aJob
     * @param nominalFeatureInfo
     * @return
     */
    public static Long extractFeatureInfo(final Config aConfig, final KnimeSparkJob aJob,
        final Map<Integer, Integer> nominalFeatureInfo) {
        nominalFeatureInfo.clear();
        final Long numClasses;
        if (aConfig.hasPath(PARAM_MAPPING_TABLE)) {
            final List<String> names = aConfig.getStringList(SupervisedLearnerUtils.PARAM_COL_NAMES);
            final JavaRDD<Row> mappingRDD = aJob.getFromNamedRdds(aConfig.getString(PARAM_MAPPING_TABLE));
            numClasses = ConvertNominalValuesJob.getNumberValuesOfColumn(mappingRDD, names.get(names.size() - 1));
            nominalFeatureInfo.putAll(ConvertNominalValuesJob.extractNominalFeatureInfo(names, mappingRDD));
        } else {
            //TK_TODO: Get the number of classes from the inputdata rdd
            numClasses = new Long(2);
        }
        return numClasses;
    }
}
