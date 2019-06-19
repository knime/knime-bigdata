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
 *   Created on Jun 17, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Class to hold metadata about a decision tree ensemble.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeEnsembleMetaData extends GenericMLDecisionTreeMetaData {

    private static final String KEY_NUM_TREES = "numTrees";

    private static final String KEY_TOTAL_NUM_NODES = "totalNumNodes";

    private static final String KEY_TREE_WEIGHTS = "treeWeights";

    /**
     * Constructor for (de)serialization.
     */
    public MLDecisionTreeEnsembleMetaData() {
    }

    /**
     *
     * @param numTrees Number of trees in the ensemble.
     * @param totalNumNodes Total number of tree nodes over all trees in the ensemble.
     * @param treeWeights Weights for each tree.
     * @param featureImportances Estimate of the importance of each feature.
     */
    public MLDecisionTreeEnsembleMetaData(final int numTrees, final int totalNumNodes, final double[] treeWeights,
        final double[] featureImportances) {
        super(featureImportances);

        setInteger(KEY_NUM_TREES, numTrees);
        setInteger(KEY_TOTAL_NUM_NODES, totalNumNodes);
        setDoubleArray(KEY_TREE_WEIGHTS, treeWeights);
    }

    /**
     *
     * @return the number of trees in the ensemble.
     */
    public int getNoOfTrees() {
        return getInteger(KEY_NUM_TREES);
    }

    /**
     *
     * @return the total number of tree nodes over all trees in the ensemble.
     */
    public int getTotalNoOfTreeNodes() {
        return getInteger(KEY_TOTAL_NUM_NODES);
    }

    /**
     *
     * @return the weights for each tree.
     */
    public double[] getTreeWeights() {
        return getDoubleArray(KEY_TREE_WEIGHTS);
    }
}
