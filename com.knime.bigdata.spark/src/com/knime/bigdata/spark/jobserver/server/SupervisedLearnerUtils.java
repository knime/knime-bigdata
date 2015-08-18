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
     * table with feature and label mappings
     */
    public static final String PARAM_MAPPING_TABLE = "MappingInputTable";

    /**
     * @param aConfig
     * @param aRowRDD
     * @return LabeledPoint RDD with training data
     */
    public static JavaRDD<LabeledPoint> getTrainingData(final JobConfig aConfig, final JavaRDD<Row> aRowRDD) {
        final List<Integer> colIdxs = getSelectedColumnIds(aConfig);

        //note: requires that all features (including the label) are numeric !!!
        final int labelIndex = aConfig.getInputParameter(ParameterConstants.PARAM_LABEL_INDEX, Integer.class);
        final JavaRDD<LabeledPoint> inputRdd = RDDUtilsInJava.toJavaLabeledPointRDD(aRowRDD, colIdxs, labelIndex);
        return inputRdd;
    }

    /**
     *
     * @param aConfig
     * @return List of selected column ids
     */
    public static List<Integer> getSelectedColumnIds(final JobConfig aConfig) {
        return aConfig.getInputListParameter(ParameterConstants.PARAM_COL_IDXS, Integer.class);
    }

    /**
     * @param aConfig
     * @return error message if at least one param is invalid, null otherwise
     */
    @CheckForNull
    public static String checkConfig(final JobConfig aConfig) {

        if (!aConfig.hasInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE)) {
            return "Input parameter '" + KnimeSparkJob.PARAM_INPUT_TABLE + "' missing.";
        }

        String msg = checkLableColumnParameter(aConfig);

        if (msg == null) {
            msg = checkSelectedColumnIdsParameter(aConfig);
        }
        if (msg == null) {
            if (!aConfig.hasInputParameter(ParameterConstants.PARAM_COL_NAMES)) {
                return "Input parameter '" + ParameterConstants.PARAM_COL_NAMES + "' missing.";
            } else {
                try {
                    List<String> names = aConfig.getInputListParameter(ParameterConstants.PARAM_COL_NAMES, String.class);
                    if (names.size() != getSelectedColumnIds(aConfig).size() + 1) {
                        return "Input parameter '"
                            + ParameterConstants.PARAM_COL_NAMES
                            + "' is of unexpected length. It must have one entry for each select input column and 1 for the label column.";
                    }
                } catch (Exception e) {
                    return "Input parameter '" + ParameterConstants.PARAM_COL_NAMES + "' is not of expected type 'string list'.";
                }
            }
        }
        return msg;
    }

    /**
     * @param aConfig
     * @return message if anything is wrong, null otherwise
     */
    public static String checkLableColumnParameter(final JobConfig aConfig) {
        if (!aConfig.hasInputParameter(ParameterConstants.PARAM_LABEL_INDEX)) {
            return "Input parameter '" + ParameterConstants.PARAM_LABEL_INDEX + "' missing.";
        } else {
            try {
                final int ix = aConfig.getInputParameter(ParameterConstants.PARAM_LABEL_INDEX, Integer.class);
                if (ix < 0) {
                    return "Input parameter '" + ParameterConstants.PARAM_LABEL_INDEX + "' must be positive, got " + ix + ".";
                }
            } catch (Exception e) {
                return "Input parameter '" + ParameterConstants.PARAM_LABEL_INDEX + "' is not of expected type 'integer'.";
            }
        }
        return null;
    }

    /**
     * @param aConfig
     * @return error message if column ids parameter is not set or incorrect
     */
    public static String checkSelectedColumnIdsParameter(final JobConfig aConfig) {
        if (!aConfig.hasInputParameter(ParameterConstants.PARAM_COL_IDXS)) {
            return "Input parameter '" + ParameterConstants.PARAM_COL_IDXS + "' missing.";
        } else {
            try {
                getSelectedColumnIds(aConfig);
            } catch (Exception e) {
                e.printStackTrace();
                return "Input parameter '" + ParameterConstants.PARAM_COL_IDXS + "' is not of expected type 'integer list'.";
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
        final String key = aConfig.getInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE);
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
        if (aConfig.hasOutputParameter(KnimeSparkJob.PARAM_RESULT_TABLE)) {
            aLogger.log(Level.INFO,
                "Storing predicted data under key: " + aConfig.getOutputStringParameter(KnimeSparkJob.PARAM_RESULT_TABLE));
            //TODO - revert the label to int mapping ????
            JavaRDD<Row> predictedData = ModelUtils.predict(aFeatures, aInputRdd, aModel);
            aJob.addToNamedRdds(aConfig.getOutputStringParameter(KnimeSparkJob.PARAM_RESULT_TABLE), predictedData);
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
                aConfig.getInputListParameter(ParameterConstants.PARAM_COL_NAMES, String.class);
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
