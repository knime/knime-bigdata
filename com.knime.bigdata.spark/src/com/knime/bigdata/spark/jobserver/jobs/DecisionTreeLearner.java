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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * runs MLlib DecisionTree on a given RDD to create a decision tree, model is returned as result
 *
 * @author koetter, dwk
 */
public class DecisionTreeLearner extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * impurity - Criterion used for information gain calculation. Supported values: "gini" (recommended) or "entropy".
     */
    private static final String PARAM_IMPURITY = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_INFORMATION_GAIN;

    /**
     * maxDepth - Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf
     * nodes. (suggested value: 5)
     */
    private static final String PARAM_MAX_DEPTH = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_MAX_DEPTH;

    /**
     * maxBins - maximum number of bins used for splitting features (suggested value: 32)
     */
    private static final String PARAM_MAX_BINS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_MAX_BINS;

    private static final String PARAM_TRAINING_RDD = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_LABEL_INDEX = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_LABEL_INDEX;

    private static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(DecisionTreeLearner.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;

        if (!aConfig.hasPath(PARAM_MAX_DEPTH)) {
            msg = "Input parameter '" + PARAM_MAX_DEPTH + "' missing.";
        } else {
            try {
                aConfig.getInt(PARAM_MAX_DEPTH);
            } catch (ConfigException e) {
                msg = "Input parameter '" + PARAM_MAX_DEPTH + "' is not of expected type 'integer'.";
            }
        }
        if (msg == null) {
            if (!aConfig.hasPath(PARAM_MAX_BINS)) {
                msg = "Input parameter '" + PARAM_MAX_BINS + "' missing.";
            } else {
                try {
                    aConfig.getInt(PARAM_MAX_BINS);
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_MAX_BINS + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null) {
            if (!aConfig.hasPath(PARAM_LABEL_INDEX)) {
                msg = "Input parameter '" + PARAM_LABEL_INDEX + "' missing.";
            } else {
                try {
                    aConfig.getInt(PARAM_LABEL_INDEX);
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_LABEL_INDEX + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null && !aConfig.hasPath(PARAM_IMPURITY)) {
            msg = "Input parameter '" + PARAM_IMPURITY + "' missing.";
        }

        if (msg == null && !aConfig.hasPath(PARAM_TRAINING_RDD)) {
            msg = "Input parameter '" + PARAM_TRAINING_RDD + "' missing.";
        }

        //	    input - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
        //	    numClasses - number of classes for classification.
        //	    categoricalFeaturesInfo - Map storing arity of categorical features.
        //       E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final Config aConfig) throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getString(PARAM_TRAINING_RDD);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting Decision Tree learner job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(PARAM_TRAINING_RDD));


            JobResult res = JobResult.emptyJobResult().withMessage("OK");

            //note: requires that all features (including for the label) are numeric !!!
            final int labelIndex = aConfig.getInt(PARAM_LABEL_INDEX);
            final JavaRDD<LabeledPoint> inputRdd = RDDUtilsInJava.toJavaLabeledPointRDD(rowRDD, labelIndex);

            final DecisionTreeModel model = execute(sc, aConfig, inputRdd, info.getNumberClasses());
            res = res.withObjectResult(model);

            if (aConfig.hasPath(PARAM_OUTPUT_DATA_PATH)) {
                LOGGER
                    .log(Level.INFO, "Storing predicted data under key: " + aConfig.getString(PARAM_OUTPUT_DATA_PATH));
                JavaRDD<Vector> features = RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd);
                //TODO - revert the label to int mapping ????
                JavaRDD<Row> predictedData = ModelUtils.predict(sc, features, rowRDD, model);
                try {
                    addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH), predictedData);
                } catch (Exception e) {
                    LOGGER.severe("ERROR: failed to predict and store results for training data.");
                    LOGGER.severe(e.getMessage());
                }
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
     * @param aNumClasses - number of classes for classification.
     * @return DecisionTreeModel
     */
    private DecisionTreeModel execute(final SparkContext aContext, final Config aConfig,
        final JavaRDD<LabeledPoint> aInputData, final int aNumClasses) {
        aInputData.cache();

        //Map storing arity of categorical features. E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.
        final Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();

        final int maxDepth = aConfig.getInt(PARAM_MAX_DEPTH);
        final int maxBins = aConfig.getInt(PARAM_MAX_BINS);
        final String impurity = aConfig.getString(PARAM_IMPURITY);

        // Cluster the data into m_noOfCluster classes using KMeans
        return DecisionTree.trainClassifier(aInputData, aNumClasses, categoricalFeaturesInfo, impurity, maxDepth,
            maxBins);
    }

}
