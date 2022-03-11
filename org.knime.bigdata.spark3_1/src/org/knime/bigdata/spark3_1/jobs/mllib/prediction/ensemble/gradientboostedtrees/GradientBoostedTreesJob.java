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
package org.knime.bigdata.spark3_1.jobs.mllib.prediction.ensemble.gradientboostedtrees;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.configuration.Strategy;
import org.apache.spark.mllib.tree.impurity.Entropy;
import org.apache.spark.mllib.tree.loss.AbsoluteError$;
import org.apache.spark.mllib.tree.loss.LogLoss$;
import org.apache.spark.mllib.tree.loss.Loss;
import org.apache.spark.mllib.tree.loss.SquaredError$;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.LossFunction;
import org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees.GradientBoostedTreesJobInput;
import org.knime.bigdata.spark3_1.api.NamedObjects;
import org.knime.bigdata.spark3_1.api.SparkJob;
import org.knime.bigdata.spark3_1.api.SupervisedLearnerUtils;

import scala.Enumeration.Value;

/**
 * runs MLlib GradientBoostedTrees on a given RDD to create a forest, model is returned as result
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class GradientBoostedTreesJob implements SparkJob<GradientBoostedTreesJobInput, ModelJobOutput> {


    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(GradientBoostedTreesJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelJobOutput runJob(final SparkContext sparkContext, final GradientBoostedTreesJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "starting 'Gradient Boosted Trees' learner job...");

        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(input, dataset);

        final GradientBoostedTreesModel model = execute(input, inputRdd);
        SupervisedLearnerUtils.storePredictions(sparkContext, namedObjects, input, dataset.javaRDD(),
                inputRdd, model);
        LOGGER.log(Level.INFO, "'Gradient Boosted Trees' done");
        // note that with Spark 1.4 we can use PMML instead
        return new ModelJobOutput(model);

    }

    /**
     * Train a GradientBoostedTrees model.
     *
     * @param aConfig
     * @param inputData - Training dataset: RDD of LabeledPoint.
     * @return GradientBoostedTreesModel
     * @throws KNIMESparkException
     */
    private static GradientBoostedTreesModel execute(final GradientBoostedTreesJobInput input,
        final JavaRDD<LabeledPoint> inputData)
        throws KNIMESparkException {
        inputData.cache();
        final Map<Integer, Integer> nominalFeatureInfo = input.getNominalFeatureInfo().getMap();
        final Value value;
        final Long noOfClasses;
        if (input.isClassification()) {

            value = Algo.Classification();
            noOfClasses = SupervisedLearnerUtils.getNoOfClasses(input, inputData);
            if (noOfClasses > 2) {
                throw new KNIMESparkException("Only binary classification is supported for boosting.");
            }
            LOGGER.log(Level.INFO, "Training GBT for " + noOfClasses + " classes.");
        } else {
            LOGGER.log(Level.INFO, "Training Regression GBT.");
            value = Algo.Regression();
            //no of classes is anyway ignored for regression
            noOfClasses = Long.valueOf(2);
        }
        final Strategy treeStrategy = new Strategy(value, Entropy.instance(), input.getMaxDepth(),
            noOfClasses.intValue(), input.getMaxNoOfBins(), nominalFeatureInfo);
        final BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams(value);
        boostingStrategy.setTreeStrategy(treeStrategy);
        boostingStrategy.setLoss(LossFactory.getLossFunction(input.getLossFunction()));
        boostingStrategy.setNumIterations(input.getNoOfIterations());
        boostingStrategy.setLearningRate(input.getLearningRate());

        //further options (default values are shown):
        //final Option<String> checkpointDir = scala.None;
        //treeStrategy.setCheckpointDir(checkpointDir);
        //final int checkpointInterval = 10;
        //treeStrategy.setCheckpointInterval(checkpointInterval);

        //Map<Integer, Integer> quantileCalculationStrategy = new HashMap<>();
        // quantileCalculationStrategy: QuantileStrategy.QuantileStrategy = ..., categoricalFeaturesInfo: Map[Int, Int] = ...,
        //treeStrategy.setQuantileCalculationStrategy(quantileCalculationStrategy);

        //        final int minInstancesPerNode = 1;
        //        treeStrategy.setMinInstancesPerNode(minInstancesPerNode);
        //        final double minInfoGain = 0.0;
        //        treeStrategy.setMinInfoGain(minInfoGain);
        //        final int maxMemoryInMB = 256;
        //        treeStrategy.setMaxMemoryInMB(maxMemoryInMB);
        //        final double subsamplingRate = 1d;
        //        treeStrategy.setSubsamplingRate(subsamplingRate);
        //        final boolean useNodeIdCache = false;
        //        treeStrategy.setUseNodeIdCache(useNodeIdCache);

        //impurity setting is ignored by GBT
        //treeStrategy.setImpurity(IGNORE);
        return GradientBoostedTrees.train(inputData, boostingStrategy);
    }

    private static class LossFactory {
        static Loss getLossFunction(final LossFunction aLossFunction) {
            //one of     AbsoluteError, LogLoss, SquaredError
            switch (aLossFunction) {
                case LogLoss:
                    return LogLoss$.MODULE$;
                case SquaredError:
                    return SquaredError$.MODULE$;
                case AbsoluteError:
                    return AbsoluteError$.MODULE$;
                    default:
                        throw new IllegalArgumentException("Unsupported loss function: "+aLossFunction.toString());
            }
        }
    }

}
