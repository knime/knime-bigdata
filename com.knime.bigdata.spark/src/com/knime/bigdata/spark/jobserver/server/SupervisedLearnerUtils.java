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
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.CheckForNull;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;

/**
 *
 * @author dwk
 */
public class SupervisedLearnerUtils {
    private final static Logger LOGGER = Logger.getLogger(SupervisedLearnerUtils.class.getName());
    /**
     * array with the indices of the nominal columns
     */
    public static final String PARAM_NOMINAL_FEATURE_INFO = "NominalFeatureInfo";


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
//        if (msg == null) {
//            if (!aConfig.hasInputParameter(ParameterConstants.PARAM_COL_NAMES)) {
//                return "Input parameter '" + ParameterConstants.PARAM_COL_NAMES + "' missing.";
//            } else {
//                try {
//                    List<String> names = aConfig.getInputListParameter(ParameterConstants.PARAM_COL_NAMES, String.class);
//                    if (names.size() != getSelectedColumnIds(aConfig).size() + 1) {
//                        return "Input parameter '"
//                            + ParameterConstants.PARAM_COL_NAMES
//                            + "' is of unexpected length. It must have one entry for each select input column and 1 for the label column.";
//                    }
//                } catch (Exception e) {
//                    return "Input parameter '" + ParameterConstants.PARAM_COL_NAMES + "' is not of expected type 'string list'.";
//                }
//            }
//        }
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
//        if (aConfig.hasInputParameter(PARAM_NOMINAL_COL_IDXS)) {
//        }
//        if (aConfig.hasInputParameter(PARAM_NOMINAL_COL_COUNTS)) {
//        }
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
     * @param aConfig configuration with column names and counts
     * @return number of classes
     * @throws GenericKnimeSparkException if the {@link NominalFeatureInfo} object is invalid
     */
    public static NominalFeatureInfo extractNominalFeatureInfo(final JobConfig aConfig) throws GenericKnimeSparkException {
        LOGGER.fine("Extract nominal feature info");
        final NominalFeatureInfo info;
        if (aConfig.hasInputParameter(PARAM_NOMINAL_FEATURE_INFO) && aConfig.hasInputParameter(PARAM_NOMINAL_FEATURE_INFO)) {
            LOGGER.fine("Nominal feature info found");
            info = aConfig.decodeFromInputParameter(PARAM_NOMINAL_FEATURE_INFO);
            LOGGER.fine("Extracted Nominal feature info: " + info);
        } else {
            LOGGER.fine("No nominal feature info found");
            //we assume that there are no nominal (input) features
            info = new NominalFeatureInfo();
        }
        return info;
    }

    /**
     * compute the number of classes (or distinct values of a feature)
     * @param aRDD
     * @param aColumn
     * @return the number of distinct values for the given column index
     */
    public static long getNumberValuesOfColumn(final JavaRDD<Row> aRDD, final int aColumn) {
        return aRDD.map(new Function<Row, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object call(final Row aRow) throws Exception {
                return aRow.get(aColumn);
            }
        }).distinct().count();
    }

    /**
     * compute the number of classes
     * @param aRDD
     * @return the number of distinct labels
     */
    public static long getNumberOfLabels(final JavaRDD<LabeledPoint> aRDD) {
        return aRDD.map(new Function<LabeledPoint, Double>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call(final LabeledPoint aPoint) throws Exception {
                return aPoint.label();
            }
        }).distinct().count();
    }
}
