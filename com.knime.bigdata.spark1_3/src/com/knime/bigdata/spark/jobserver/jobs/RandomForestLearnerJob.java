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

import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * runs MLlib RandomForest on a given RDD to create a random forest, model is returned as result
 *
 * @author koetter, dwk
 */
public class RandomForestLearnerJob extends AbstractTreeLearnerJob {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(RandomForestLearnerJob.class.getName());

    /**
     * Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird". If
     * "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest)
     * set to "sqrt".
     */
    public static final String PARAM_FEATURE_SUBSET_STRATEGY = "featureSubsetStrategy";

    /**
     * number of trees in the forest
     */
    public static final String PARAM_NUM_TREES = "numTrees";

    /**
     * train a classifier or a regression
     */
    public static final String PARAM_IS_CLASSIFICATION = "isClassification";

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_FEATURE_SUBSET_STRATEGY)) {
            msg = "Input parameter '" + PARAM_FEATURE_SUBSET_STRATEGY + "' missing.";
        } else {
            final String val = aConfig.getInputParameter(PARAM_FEATURE_SUBSET_STRATEGY, String.class);
            try {
                EnumContainer.RandomForestFeatureSubsetStrategies.fromKnimeEnum(val);
            } catch (Exception e) {
                msg = "Input parameter '" + PARAM_FEATURE_SUBSET_STRATEGY + "' has incompatible value '" + val + "'.";
            }
        }
        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_NUM_TREES)) {
                msg = "Input parameter '" + PARAM_NUM_TREES + "' missing.";
            } else {
                try {
                    if ((Integer)aConfig.getInputParameter(PARAM_NUM_TREES, Integer.class) == null) {
                        msg = "Input parameter '" + PARAM_NUM_TREES + "' is empty.";
                    }
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_NUM_TREES + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_IS_CLASSIFICATION)) {
            msg = "Input parameter '" + PARAM_IS_CLASSIFICATION  + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(ParameterConstants.PARAM_SEED)) {
            msg = "Input parameter '" + ParameterConstants.PARAM_SEED  + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return super.validate(aConfig);
    }

    /**
     * run the actual job, the result is serialized back to the client
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, LOGGER);
        LOGGER.log(Level.INFO, "starting Random Forest learner job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(aConfig, rowRDD);
        final boolean isClassification = aConfig.getInputParameter(PARAM_IS_CLASSIFICATION, Boolean.class);
        final RandomForestModel model;
        if (isClassification) {
            model = executeClassification(sc, aConfig, inputRdd);
        } else {
            model = executeRegression(sc, aConfig, inputRdd);
        }

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            SupervisedLearnerUtils.storePredictions(sc, aConfig, this, rowRDD,
                RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd), model, LOGGER);
        }
        LOGGER.log(Level.INFO, "Random Forest Learner done");
        // note that with Spark 1.4 we can use PMML instead
        return res;

    }

    /**
     *
     * @param aContext
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
     * @return RandomForestModel
     * @throws GenericKnimeSparkException
     */
    private static RandomForestModel executeClassification(final SparkContext aContext, final JobConfig aConfig,
        final JavaRDD<LabeledPoint> aInputData) throws GenericKnimeSparkException {
        aInputData.cache();

        final Map<Integer, Integer> nominalFeatureInfo =
            SupervisedLearnerUtils.extractNominalFeatureInfo(aConfig).getMap();
        final Integer labelIndex = aConfig.getInputParameter(ParameterConstants.PARAM_LABEL_INDEX, Integer.class);
        final Long numClasses;
        if (aConfig.hasInputParameter(SupervisedLearnerUtils.PARAM_NO_OF_CLASSES)) {
            numClasses = aConfig.getInputParameter(SupervisedLearnerUtils.PARAM_NO_OF_CLASSES, Long.class);
        } else if (nominalFeatureInfo.containsKey(labelIndex)) {
            numClasses = nominalFeatureInfo.get(labelIndex).longValue();
        } else {
            //Get number of classes from the input data
            numClasses = SupervisedLearnerUtils.getNumberOfLabels(aInputData);
        }

        final int maxDepth = aConfig.getInputParameter(PARAM_MAX_DEPTH, Integer.class);
        final int maxBins = aConfig.getInputParameter(PARAM_MAX_BINS, Integer.class);
        final String impurity = aConfig.getInputParameter(PARAM_INFORMATION_GAIN);
        final int numTrees = aConfig.getInputParameter(PARAM_NUM_TREES, Integer.class);

        final String featureSubSetStrategy = aConfig.getInputParameter(PARAM_FEATURE_SUBSET_STRATEGY);
        final int seed = aConfig.getInputParameter(ParameterConstants.PARAM_SEED, Integer.class);

        LOGGER.log(Level.FINE, "Training Random Forest for " + numClasses + " classes.");
        for (Entry<Integer, Integer> entry : nominalFeatureInfo.entrySet()) {
            LOGGER.log(Level.FINE, "Feature[" + entry.getKey() + "] has " + entry.getValue() + " distinct values.");
        }
        return RandomForest.trainClassifier(aInputData, numClasses.intValue(), nominalFeatureInfo, numTrees,
            featureSubSetStrategy, impurity, maxDepth, maxBins, seed);
    }

    /**
     *
     * @param aContext
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
     * @return RandomForestModel
     * @throws GenericKnimeSparkException
     */
    private static RandomForestModel executeRegression(final SparkContext aContext, final JobConfig aConfig,
        final JavaRDD<LabeledPoint> aInputData) throws GenericKnimeSparkException {
        aInputData.cache();

        final Map<Integer, Integer> nominalFeatureInfo =
            SupervisedLearnerUtils.extractNominalFeatureInfo(aConfig).getMap();

        final int maxDepth = aConfig.getInputParameter(PARAM_MAX_DEPTH, Integer.class);
        final int maxBins = aConfig.getInputParameter(PARAM_MAX_BINS, Integer.class);
        final String impurity = aConfig.getInputParameter(PARAM_INFORMATION_GAIN);
        final int numTrees = aConfig.getInputParameter(PARAM_NUM_TREES, Integer.class);

        final String featureSubSetStrategy = aConfig.getInputParameter(PARAM_FEATURE_SUBSET_STRATEGY);
        final int seed = aConfig.getInputParameter(ParameterConstants.PARAM_SEED, Integer.class);

        for (Entry<Integer, Integer> entry : nominalFeatureInfo.entrySet()) {
            LOGGER.log(Level.FINE, "Feature[" + entry.getKey() + "] has " + entry.getValue() + " distinct values.");
        }
        return RandomForest.trainRegressor(aInputData, nominalFeatureInfo, numTrees, featureSubSetStrategy, impurity,
            maxDepth, maxBins, seed);
    }

}
