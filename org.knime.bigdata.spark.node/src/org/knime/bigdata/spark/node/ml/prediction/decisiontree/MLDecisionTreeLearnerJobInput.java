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
 *   Created on May 13, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import org.knime.bigdata.spark.core.job.NamedModelLearnerJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeLearnerJobInput extends NamedModelLearnerJobInput {

    /**
     * maxDepth - Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf
     * nodes. (suggested value: 5)
     */
    private static final String KEY_MAX_DEPTH = "maxDepth";

    /**maxBins - maximum number of bins used for splitting features (suggested value: 32)*/
    private static final String KEY_MAX_BINS = "maxBins";

    /**
     * Minimum number of rows each split's children must have. Splits where the left or right child has
     * fewer than minRowsPerNode will be discarded.
     */
    private static final String KEY_MIN_ROWS_PER_NODE_CHILD = "minRowsPerNodeChild";

    private static final String KEY_MIN_INFORMATION_GAIN = "minInformationGain";

    private static final String KEY_SEED = "seed";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public MLDecisionTreeLearnerJobInput() {}

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
     */
    public MLDecisionTreeLearnerJobInput(final String namedInputObject,
        final String namedOutputModel,
        final int targetColIdx,
        final Integer[] featureColIdxs,
        final int maxDepth,
        final int maxNoOfBins,
        final int minRowsPerNodeChild,
        final double minInformationGain,
        final long seed) {

        super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs);
        set(KEY_MAX_DEPTH, maxDepth);
        set(KEY_MAX_BINS, maxNoOfBins);
        set(KEY_MIN_ROWS_PER_NODE_CHILD, minRowsPerNodeChild);
        set(KEY_MIN_INFORMATION_GAIN, minInformationGain);
        set(KEY_SEED, seed);
    }

    /**
     * @return the maximum tree depth
     */
    public int getMaxDepth() {
        return getInteger(KEY_MAX_DEPTH);
    }
    /**
     * @return the maximum number of bins
     */
    public int getMaxNoOfBins() {
        return getInteger(KEY_MAX_BINS);
    }

    /**
     * @return the minimum number of desired of rows per node in the decision tree.
     */
    public int getMinRowsPerTreeNode() {
        return getInteger(KEY_MIN_ROWS_PER_NODE_CHILD);
    }

    /**
     * @return the min information gain for a split to be considered (>= 0)
     */
    public double getMinInformationGain() {
        return getDouble(KEY_MIN_INFORMATION_GAIN);
    }

    /**
     * @return the random seed
     */
    public long getSeed() {
        return getInteger(KEY_SEED);
    }
}
