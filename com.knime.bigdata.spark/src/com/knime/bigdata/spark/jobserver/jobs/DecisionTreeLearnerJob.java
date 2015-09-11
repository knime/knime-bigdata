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
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * runs MLlib DecisionTree on a given RDD to create a decision tree, model is returned as result
 *
 * @author koetter, dwk
 */
public class DecisionTreeLearnerJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Criterion used for information gain calculation. Supported values: "gini" (recommended) or "entropy".
     */
    public static final String PARAM_INFORMATION_GAIN = "impurity";

    /**
     * supported information gain criterion
     */
    public static final String VALUE_GINI = "gini";

    /**
     * supported information gain criterion
     */
    public static final String VALUE_ENTROPY = "entropy";

    /**
     * maxDepth - Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf
     * nodes. (suggested value: 5)
     */
    public static final String PARAM_MAX_DEPTH = ParameterConstants.PARAM_MAX_DEPTH;

    /**
     * maxBins - maximum number of bins used for splitting features (suggested value: 32)
     */
    public static final String PARAM_MAX_BINS = ParameterConstants.PARAM_MAX_BINS;

    /** Number of classes. **/
    public static final String PARAM_NO_OF_CLASSES = "NumberOfClasses";

    private final static Logger LOGGER = Logger.getLogger(DecisionTreeLearnerJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_MAX_DEPTH)) {
            msg = "Input parameter '" + PARAM_MAX_DEPTH + "' missing.";
        } else {
            try {
                if ((Integer)aConfig.getInputParameter(PARAM_MAX_DEPTH, Integer.class) == null) {
                    msg = "Input parameter '" + PARAM_MAX_DEPTH + "' is empty.";
                }
            } catch (Exception e) {
                msg = "Input parameter '" + PARAM_MAX_BINS + "' is not of expected type 'integer'.";
            }
        }
        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_MAX_BINS)) {
                msg = "Input parameter '" + PARAM_MAX_BINS + "' missing.";
            } else {
                try {
                    if ((Integer)aConfig.getInputParameter(PARAM_MAX_BINS, Integer.class) == null) {
                        msg = "Input parameter '" + PARAM_MAX_BINS + "' is empty.";
                    }
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_MAX_BINS + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_INFORMATION_GAIN)) {
            msg = "Input parameter '" + PARAM_INFORMATION_GAIN + "' missing.";
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkConfig(aConfig);
        }
        //      input - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
        //      numClasses - number of classes for classification.
        //      categoricalFeaturesInfo - Map storing arity of categorical features.
        //       E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, LOGGER);
        LOGGER.log(Level.INFO, "starting Decision Tree learner job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(aConfig, rowRDD);

        final DecisionTreeModel model = execute(sc, aConfig, inputRdd);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            SupervisedLearnerUtils.storePredictions(sc, aConfig, this, rowRDD,
                RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd), model, LOGGER);
        }
        LOGGER.log(Level.INFO, "Decision Tree Learner done");
        // note that with Spark 1.4 we can use PMML instead
        return res;

    }

    /**
     *
     * @param aContext
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
     * @return DecisionTreeModel
     * @throws GenericKnimeSparkException
     */
    private DecisionTreeModel execute(final SparkContext aContext, final JobConfig aConfig,
        final JavaRDD<LabeledPoint> aInputData) throws GenericKnimeSparkException {
        aInputData.cache();

        final Map<Integer, Integer> nominalFeatureInfo =
            SupervisedLearnerUtils.extractNominalFeatureInfo(aConfig).getMap();
        final Integer labelIndex = aConfig.getInputParameter(ParameterConstants.PARAM_LABEL_INDEX, Integer.class);
        final Long numClasses;
        if (aConfig.hasInputParameter(PARAM_NO_OF_CLASSES)) {
            numClasses = aConfig.getInputParameter(PARAM_NO_OF_CLASSES, Long.class);
        } else if (nominalFeatureInfo.containsKey(labelIndex)) {
            numClasses = nominalFeatureInfo.get(labelIndex).longValue();
        } else {
            //Get number of classes from the input data
            numClasses = SupervisedLearnerUtils.getNumberOfLabels(aInputData);
        }

        final int maxDepth = aConfig.getInputParameter(PARAM_MAX_DEPTH, Integer.class);
        final int maxBins = aConfig.getInputParameter(PARAM_MAX_BINS, Integer.class);
        final String impurity = aConfig.getInputParameter(PARAM_INFORMATION_GAIN);

        LOGGER.log(Level.FINE, "Training decision tree for " + numClasses + " classes.");
        LOGGER.log(Level.FINE, "Training decision tree with info for " + nominalFeatureInfo.size()
            + " nominal features: ");
        for (Entry<Integer, Integer> entry : nominalFeatureInfo.entrySet()) {
            LOGGER.log(Level.FINE, "Feature[" + entry.getKey() + "] has " + entry.getValue() + " distinct values.");
        }
        return DecisionTree.trainClassifier(aInputData, numClasses.intValue(), nominalFeatureInfo, impurity, maxDepth,
            maxBins);
    }

}
