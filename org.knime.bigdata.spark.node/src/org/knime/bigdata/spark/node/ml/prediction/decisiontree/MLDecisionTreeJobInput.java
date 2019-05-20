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
public abstract class MLDecisionTreeJobInput extends NamedModelLearnerJobInput {

    /**
     * maxDepth - Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf
     * nodes. (suggested value: 5)
     */
    private static final String MAX_DEPTH = "maxDepth";

    /**maxBins - maximum number of bins used for splitting features (suggested value: 32)*/
    private static final String MAX_BINS = "maxBins";

    /**
     * Minimum number of rows each split's children must have. Splits where the left or right child has
     * fewer than minRowsPerNode will be discarded.
     */
    private static final String MIN_ROWS_PER_NODE_CHILD = "minRowsPerNodeChild";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public MLDecisionTreeJobInput() {}

    /**
     * @param namedInputObject
     * @param namedOutputModel
     * @param targetColIdx
     * @param featureColIdxs
     * @param maxDepth
     * @param maxNoOfBins
     * @param minRowsPerNodeChild
     */
    protected MLDecisionTreeJobInput(final String namedInputObject,
        final String namedOutputModel,
        final int targetColIdx,
        final Integer[] featureColIdxs,
        final int maxDepth,
        final int maxNoOfBins,
        final int minRowsPerNodeChild) {

        super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs);
        set(MAX_DEPTH, maxDepth);
        set(MAX_BINS, maxNoOfBins);
        set(MIN_ROWS_PER_NODE_CHILD, minRowsPerNodeChild);
    }

    /**
     * @return the maximum tree depth
     */
    public int getMaxDepth() {
        return getInteger(MAX_DEPTH);
    }
    /**
     * @return the maximum number of bins
     */
    public int getMaxNoOfBins() {
        return getInteger(MAX_BINS);
    }

    /**
     * @return <code>true</code> if this is a classification
     */
    public int getMinRowsPerNodeChild() {
        return getInteger(MIN_ROWS_PER_NODE_CHILD);
    }
}
