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
 *   Created on Jun 12, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.gbt;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeLearnerJobInput;

/**
 * Abstract superclass of GBT learner job inputs (classificatio and regression).
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public abstract class MLGradientBoostedTreesLearnerJobInput extends MLDecisionTreeLearnerJobInput {

    /**
     * Strategy for picking features to consider for splits at each node.
     */
    private static final String KEY_FEATURE_SUBSET_STRATEGY = "featureSubsetStrategy";

    /**
     * Fraction of the training data to use for learning each decision tree in the random forest.
     */
    private static final String KEY_SUBSAMPLING_RATE = "subsamplingRate";

    private static final String KEY_MAX_ITERATIONS = "maxIterations";

    private static final String KEY_LEARNING_RATE = "learningRate";


    /**
     * Paramless constructor for automatic deserialization.
     */
    public MLGradientBoostedTreesLearnerJobInput() {}

    /**
     * @param namedInputObject
     * @param namedOutputModel
     * @param targetColIdx
     * @param featureColIdxs
     * @param maxDepth
     * @param maxNoOfBins
     * @param minRowsPerNodeChild
     * @param minInformationGain
     * @param seed
     * @param featureSubsetStrategy
     * @param subsamplingRate
     * @param maxIterations
     * @param learningRate
     */
    public MLGradientBoostedTreesLearnerJobInput(final String namedInputObject,
        final String namedOutputModel,
        final int targetColIdx,
        final Integer[] featureColIdxs,
        final int maxDepth,
        final int maxNoOfBins,
        final int minRowsPerNodeChild,
        final double minInformationGain,
        final long seed,
        final String featureSubsetStrategy,
        final double subsamplingRate,
        final int maxIterations,
        final double learningRate) {

        super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs, maxDepth, maxNoOfBins,
            minRowsPerNodeChild, minInformationGain, seed);

        set(KEY_FEATURE_SUBSET_STRATEGY, featureSubsetStrategy);
        set(KEY_SUBSAMPLING_RATE, subsamplingRate);
        set(KEY_MAX_ITERATIONS, maxIterations);
        set(KEY_LEARNING_RATE, learningRate);
    }

    /**
     *
     * @return the number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2",
     *         "onethird".
     */
    public String getFeatureSubsetStrategy() {
        return get(KEY_FEATURE_SUBSET_STRATEGY);
    }

    /**
     * @return Fraction of the training data to use for learning each decision tree in the random forest, in range
     *         (0,1].
     */
    public double getSubsamplingRate() {
        return getDouble(KEY_SUBSAMPLING_RATE);
    }

    /**
     * @return maximum number of iterations (>= 0).
     */
    public int getMaxIterations() {
        return getInteger(KEY_MAX_ITERATIONS);
    }

    /**
     *
     * @return learning rate (a.k.a. step size) in interval (0, 1] for decreasiing the contribution of each estimator.
     */
    public double getLearningRate() {
        return getDouble(KEY_LEARNING_RATE);
    }
}
