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

/**
 *
 * @author dwk
 */
public class SupervisedLearnerUtils {

    /**
     * name of parameter with training data
     */
    public static final String PARAM_TRAINING_RDD = ParameterConstants.PARAM_TABLE_1;

    /**
     * selector for columns to be used for learning
     */
    private static final String PARAM_COL_IDXS = ParameterConstants.PARAM_COL_IDXS;

    /**
     * names of the columns (must include label column), required for value mapping info
     */
    public static final String PARAM_COL_NAMES = ParameterConstants.PARAM_COL_IDXS + ParameterConstants.PARAM_STRING;

    /**
     * index of label column
     */
    public static final String PARAM_LABEL_INDEX = ParameterConstants.PARAM_LABEL_INDEX;

    /**
     * table with feature and label mappings
     */
    public static final String PARAM_MAPPING_TABLE = ParameterConstants.PARAM_TABLE_2;

    /**
     * parameter name for table with model predictions
     */
    public static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_TABLE_1;

    /**
     * @param aConfig
     * @param aRowRDD
     * @return LabeledPoint RDD with training data
     */
    public static JavaRDD<LabeledPoint> getTrainingData(final JobConfig aConfig, final JavaRDD<Row> aRowRDD) {
        final List<Integer> colIdxs = getSelectedColumnIds(aConfig);

        //note: requires that all features (including the label) are numeric !!!
        final int labelIndex = aConfig.getInputParameter(PARAM_LABEL_INDEX, Integer.class);
        final JavaRDD<LabeledPoint> inputRdd = RDDUtilsInJava.toJavaLabeledPointRDD(aRowRDD, colIdxs, labelIndex);
        return inputRdd;
    }

    /**
     *
     * @param aConfig
     * @return List of selected column ids
     */
    public static List<Integer> getSelectedColumnIds(final JobConfig aConfig) {
        return aConfig.getInputListParameter(PARAM_COL_IDXS, Integer.class);
    }

    /**
     * @param aConfig
     * @return error message if at least one param is invalid, null otherwise
     */
    @CheckForNull
    public static String checkConfig(final JobConfig aConfig) {

        if (!aConfig.hasInputParameter(SupervisedLearnerUtils.PARAM_TRAINING_RDD)) {
            return "Input parameter '" + SupervisedLearnerUtils.PARAM_TRAINING_RDD + "' missing.";
        }

        String msg = checkLableColumnParameter(aConfig);

        if (msg == null) {
            msg = checkSelectedColumnIdsParameter(aConfig);
        }
        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_COL_NAMES)) {
                return "Input parameter '" + PARAM_COL_NAMES + "' missing.";
            } else {
                try {
                    List<String> names = aConfig.getInputListParameter(PARAM_COL_NAMES, String.class);
                    if (names.size() != getSelectedColumnIds(aConfig).size() + 1) {
                        return "Input parameter '"
                            + PARAM_COL_NAMES
                            + "' is of unexpected length. It must have one entry for each select input column and 1 for the label column.";
                    }
                } catch (Exception e) {
                    return "Input parameter '" + PARAM_COL_NAMES + "' is not of expected type 'string list'.";
                }
            }
        }
        return msg;
    }

    /**
     * @param aConfig
     */
    public static String checkLableColumnParameter(final JobConfig aConfig) {
        if (!aConfig.hasInputParameter(PARAM_LABEL_INDEX)) {
            return "Input parameter '" + PARAM_LABEL_INDEX + "' missing.";
        } else {
            try {
                final int ix = aConfig.getInputParameter(PARAM_LABEL_INDEX, Integer.class);
                if (ix < 0) {
                    return "Input parameter '" + PARAM_LABEL_INDEX + "' must be positive, got " + ix + ".";
                }
            } catch (Exception e) {
                return "Input parameter '" + PARAM_LABEL_INDEX + "' is not of expected type 'integer'.";
            }
        }
        return null;
    }

    /**
     * @param aConfig
     * @return error message if column ids parameter is not set or incorrect
     */
    public static String checkSelectedColumnIdsParameter(final JobConfig aConfig) {
        if (!aConfig.hasInputParameter(PARAM_COL_IDXS)) {
            return "Input parameter '" + PARAM_COL_IDXS + "' missing.";
        } else {
            try {
                getSelectedColumnIds(aConfig);
            } catch (Exception e) {
                e.printStackTrace();
                return "Input parameter '" + PARAM_COL_IDXS + "' is not of expected type 'integer list'.";
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
    public static void validateInput(final JobConfig aConfig, final KnimeSparkJob aJob, final Logger aLogger)
        throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getInputParameter(SupervisedLearnerUtils.PARAM_TRAINING_RDD);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!aJob.validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (aConfig.hasInputParameter(PARAM_MAPPING_TABLE)) {
            final String mappingTable = aConfig.getInputParameter(PARAM_MAPPING_TABLE);
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
     * @param sc
     * @param aConfig
     * @param aJob
     * @param aInputRdd
     * @param aFeatures
     * @param aModel
     * @param aLogger
     * @throws GenericKnimeSparkException
     */
    public static void storePredictions(final SparkContext sc, final JobConfig aConfig, final KnimeSparkJob aJob,
        final JavaRDD<Row> aInputRdd, final JavaRDD<Vector> aFeatures, final Serializable aModel, final Logger aLogger)
        throws GenericKnimeSparkException {
        if (aConfig.hasOutputParameter(PARAM_OUTPUT_DATA_PATH)) {
            aLogger.log(Level.INFO,
                "Storing predicted data under key: " + aConfig.getOutputStringParameter(PARAM_OUTPUT_DATA_PATH));
            //TODO - revert the label to int mapping ????
            JavaRDD<Row> predictedData = ModelUtils.predict(sc, aFeatures, aInputRdd, aModel);
            aJob.addToNamedRdds(aConfig.getOutputStringParameter(PARAM_OUTPUT_DATA_PATH), predictedData);
        }
    }

    /**
     * @param aConfig configuration with column names and mapping table name
     * @param aJob
     * @param nominalFeatureInfo - will be filled with feature information as a side effect ! TODO - refactor and write
     *            tests for this method !!!
     * @return number of classes
     */
    public static Long extractFeatureInfo(final JobConfig aConfig, final KnimeSparkJob aJob,
        final Map<Integer, Integer> nominalFeatureInfo) {
        nominalFeatureInfo.clear();
        final Long numClasses;
        if (aConfig.hasInputParameter(PARAM_MAPPING_TABLE)) {
            //final int labelColIx = aConfig.getInt(PARAM_LABEL_INDEX);
            final List<String> names =
                aConfig.getInputListParameter(SupervisedLearnerUtils.PARAM_COL_NAMES, String.class);
            final String classColName = names.remove(names.size() - 1);
            final JavaRDD<Row> mappingRDD = aJob.getFromNamedRdds(aConfig.getInputParameter(PARAM_MAPPING_TABLE));
            numClasses = ConvertNominalValuesJob.getNumberValuesOfColumn(mappingRDD, classColName);
            nominalFeatureInfo.putAll(ConvertNominalValuesJob.extractNominalFeatureInfo(names, mappingRDD));
        } else {
            //TK_TODO: Get the number of classes from the inputdata rdd
            numClasses = new Long(2);
        }
        return numClasses;
    }
}
