/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark2_2.jobs.mllib.prediction.ensemble.randomforest;

import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.RandomForestJobInput;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SparkJob;
import org.knime.bigdata.spark2_2.api.SupervisedLearnerUtils;

/**
 * runs MLlib RandomForest on a given RDD to create a random forest, model is returned as result
 *
 * @author koetter, dwk
 */
@SparkClass
public class RandomForestJob implements SparkJob<RandomForestJobInput, ModelJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(RandomForestJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelJobOutput runJob(final SparkContext sparkContext, final RandomForestJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "starting Random Forest learner job...");
        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(input, dataset);
        final boolean isClassification = input.isClassification();
        //cache the input object to speed up computation
        inputRdd.cache();
        final Map<Integer, Integer> nominalFeatureInfo = input.getNominalFeatureInfo().getMap();
        for (Entry<Integer, Integer> entry : nominalFeatureInfo.entrySet()) {
            LOGGER.log(Level.FINE, "Feature[" + entry.getKey() + "] has " + entry.getValue() + " distinct values.");
        }
        final int maxDepth = input.getMaxDepth();
        final int maxBins = input.getMaxNoOfBins();
        final String impurity = input.getQualityMeasure().name();
        final int numTrees = input.getNoOfTrees();
        final String featureSubSetStrategy = input.getFeatureStrategy();
        final int seed = input.getSeed();
        final RandomForestModel model;
        if (isClassification) {
            final Long numClasses = SupervisedLearnerUtils.getNoOfClasses(input, inputRdd);
            LOGGER.log(Level.FINE, "Training Random Forest for " + numClasses + " classes.");
            model = RandomForest.trainClassifier(inputRdd, numClasses.intValue(), nominalFeatureInfo, numTrees,
                featureSubSetStrategy, impurity, maxDepth, maxBins, seed);
        } else {
            LOGGER.log(Level.FINE, "Training Random Forest regression.");
            model = RandomForest.trainRegressor(inputRdd, nominalFeatureInfo, numTrees, featureSubSetStrategy, impurity,
                maxDepth, maxBins, seed);
        }
        SupervisedLearnerUtils.storePredictions(sparkContext, namedObjects, input, dataset.javaRDD(), inputRdd, model);
        LOGGER.log(Level.INFO, "Random Forest Learner done");
        // note that with Spark 1.4 we can use PMML instead
        return new ModelJobOutput(model);

    }
}
