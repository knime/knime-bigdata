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
package com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.FeatureSubsetStrategy;
import com.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import com.knime.bigdata.spark.core.job.util.NominalFeatureInfo;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class RandomForestJobInput extends DecisionTreeJobInput {

    /**
     * Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird". If
     * "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest)
     * set to "sqrt".
     */
    private static final String FEATURE_SUBSET_STRATEGY = "featureSubsetStrategy";

    /**
     * number of trees in the forest
     */
    private static final String NUM_TREES = "numTrees";

    private static final String RANDOM_SEED = "seed";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public RandomForestJobInput() {}

    /**
     * @param aInputRDD
     * @param featureColIdxs
     * @param nominalFeatureInfo
     * @param classColIdx
     * @param noOfClasses
     * @param maxDepth
     * @param maxNoOfBins
     * @param aNumTrees
     * @param aIsClassification
     * @param aFSStrategy
     * @param aRandomSeed
     * @param qualityMeasure
     */
    public RandomForestJobInput(final String aInputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final int classColIdx, final Long noOfClasses, final int maxDepth,
        final int maxNoOfBins, final int aNumTrees, final boolean aIsClassification,
        final FeatureSubsetStrategy aFSStrategy, final Integer aRandomSeed, final InformationGain qualityMeasure) {
        super(aInputRDD, featureColIdxs, nominalFeatureInfo, classColIdx, noOfClasses, aIsClassification, maxDepth, maxNoOfBins,
            qualityMeasure);
        set(NUM_TREES, aNumTrees);
        set(FEATURE_SUBSET_STRATEGY, aFSStrategy.name());
        set(RANDOM_SEED, aRandomSeed);
    }

    /**
     * @return number of trees in the forest
     */
    public int getNoOfTrees() {
        return getInteger(NUM_TREES);
    }

    /**
     * @return the {@link FeatureSubsetStrategy} that defines the number of features to consider for splits at each
     * node. Supported: "auto", "all", "sqrt", "log2", "onethird". If "auto" is set, this parameter is set based on
     * numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest)
     * set to "sqrt".
     */
    public FeatureSubsetStrategy getFeatureStrategy() {
        final String stragetyName = get(FEATURE_SUBSET_STRATEGY);
        return FeatureSubsetStrategy.valueOf(stragetyName);
    }

    /**
     * @return the seed to use for random number generation
     */
    public int getSeed() {
        return getInteger(RANDOM_SEED);
    }
}
