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
 *   Created on Jun 8, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.randomforest;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeLearnerJobInput;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLRandomForestLearnerJobInput extends MLDecisionTreeLearnerJobInput {

    /**
     * Number of trees in the forest.
     */
    private static final String KEY_NUM_TREES = "numTrees";

    /**
     * Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird". If
     * "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest)
     * set to "sqrt".
     */
    private static final String KEY_FEATURE_SUBSET_STRATEGY = "featureSubsetStrategy";

    /**
     * Fraction of the training data to use for learning each decision tree in the random forest.
     */
    private static final String KEY_SUBSAMPLING_RATE = "subsamplingRate";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public MLRandomForestLearnerJobInput() {
    }

    /**
     *
     * @param namedInputObject Key/ID of the named input object (DataFrame/RDD) to learn on.
     * @param namedOutputModel Key/ID for the model that shall be produced by the job.
     * @param targetColIdx The column index of the target/class column.
     * @param featureColIdxs the feature column indices starting with 0
     * @param maxDepth
     * @param maxNoOfBins
     * @param minRowsPerNodeChild
     * @param minInformationGain
     * @param numberOfTrees Number of trees in the forest.
     * @param featureSubsetStrategy Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird".
     * @param seed
     * @param subsamplingRate
     */
    public MLRandomForestLearnerJobInput(final String namedInputObject,
       final String namedOutputModel,
       final int targetColIdx,
       final Integer[] featureColIdxs,
       final int maxDepth,
       final int maxNoOfBins,
       final int minRowsPerNodeChild,
       final double minInformationGain,
       final int seed,
       final int numberOfTrees,
       final String featureSubsetStrategy,
       final double subsamplingRate) {

       super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs, maxDepth, maxNoOfBins,
           minRowsPerNodeChild, minInformationGain, seed);
       set(KEY_NUM_TREES, numberOfTrees);
       set(KEY_FEATURE_SUBSET_STRATEGY, featureSubsetStrategy);
       set(KEY_SUBSAMPLING_RATE, subsamplingRate);
   }

    /**
     *
     * @return the number of trees in the forest.
     */
    public int getNumberOfTrees() {
        return getInteger(KEY_NUM_TREES);
    }

    /**
     *
     * @return the number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird".
     */
    public String getFeatureSubsetStrategy() {
        return get(KEY_FEATURE_SUBSET_STRATEGY);
    }

    /**
     * @return Fraction of the training data to use for learning each decision tree in the random forest, in range (0,1].
     */
    public double getSubsamplingRate() {
        return getDouble(KEY_SUBSAMPLING_RATE);
    }

}
