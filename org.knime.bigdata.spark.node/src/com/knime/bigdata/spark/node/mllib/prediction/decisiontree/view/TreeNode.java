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
 *   Created on May 26, 2016 by oole
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree.view;

import java.util.List;

/**
 * A Wrapper for the Node class in spark
 * @author Ole Ostergaard
 */
public interface TreeNode {

    /**
     * @return whether the node is a leaf or not
     */
    boolean isLeaf();

    /**
     * @return the prediction at the given node
     */
    double getPrediction();

    /**
     * @return the probability of the prediction of the node
     */
    double getProbability();

    /**
     * @return whether the node has statistics attached
     */
    boolean hasStats();

    /**
     * @return the gain at this node's split
     */
    double getGain();

    /**
     * @return the impurity at this node's split
     */
    double getImpurity();

    /**
     * @return the left impurity at this node's split
     */
    double getLeftImpurity();

    /**
     * @return the right impurity at this node's split
     */
    double getRightImpurity();

    /**
     * @return the number of this node's descendants
     */
    Integer numDescendants();

    /**
     * @return whether the split criterion in the node's parent is categorical
     */
    boolean isCategorical();

    /**
     * @return the threshold of the node given the split in the parents node.
     *          Use only if split is continuous!
     */
    double getThreshold();

    /**
     * @return whether the node is a left child
     */
    boolean isLeftChild();

    /**
     * @return the list of categories given the parents split.
     *          Use only if split is categorical!
     */
    List<Object> getCategories();

    /**
     * @return the node's parent's split feature identifier
     */
    Integer getParentSplitFeature();

    /**
     * @return the split feature of the actual node
     */
    Integer getSplitFeature();

    /**
     * @return the left child node
     */
    TreeNode getLeftNode();

    /**
     * @return the right child node
     */
    TreeNode getRightNode();

    /**
     * @return the node's parent node id
     */
    Integer getParentId();


}
