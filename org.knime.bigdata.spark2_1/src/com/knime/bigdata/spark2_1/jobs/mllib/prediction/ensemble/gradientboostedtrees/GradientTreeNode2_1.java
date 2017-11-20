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
package com.knime.bigdata.spark2_1.jobs.mllib.prediction.ensemble.gradientboostedtrees;

import org.apache.spark.mllib.tree.model.Node;

import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;
import com.knime.bigdata.spark2_1.jobs.mllib.prediction.decisiontree.TreeNode2_1;

/**
 * TreeNodes have to be handled differently for Gradiend Boosted Trees since their prediction output needs to be interpreted differently
 * @author Ole Ostergaard
 */
public class GradientTreeNode2_1 extends TreeNode2_1 {

    private boolean m_isClassification;

    /**
     * @param rootNode
     * @param isClassification
     */
    public GradientTreeNode2_1(final Node rootNode, final boolean isClassification) {
        super(rootNode);
        m_isClassification = isClassification;

    }

    /**
     * @param node
     * @param rootNode
     * @param isClassification
     */
    public GradientTreeNode2_1(final Node node, final Node rootNode, final boolean isClassification) {
        super(node, rootNode);
        m_isClassification = isClassification;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getPrediction() {
        final double prediction = super.getPrediction();
        if(m_isClassification) {
            //see https://github.com/apache/spark/blob/branch-1.2/mllib/src/main/scala/org/apache/spark/mllib/tree/model/treeEnsembleModels.scala
            //line 119-120
            return prediction > 0 ? 1.0: 0.0;
        }
        return prediction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeNode getLeftNode() {
        if (getNode().leftNode().isDefined()) {
            return new GradientTreeNode2_1(getNode().leftNode().get(), getRootNode(), m_isClassification);
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeNode getRightNode() {
        if (getNode().rightNode().isDefined()) {
            return new GradientTreeNode2_1(getNode().rightNode().get(), getRootNode(), m_isClassification);
        } else {
            return null;
        }
    }
}
