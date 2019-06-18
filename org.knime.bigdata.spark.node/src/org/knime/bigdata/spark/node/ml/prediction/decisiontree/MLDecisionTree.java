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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTree {

    private final MLDecisionTreeNode m_rootNode;

    /**
     * Creates a new decision tree rooted at the given node.
     *
     * @param rootNode
     */
    public MLDecisionTree(final MLDecisionTreeNode rootNode) {
        super();
        m_rootNode = rootNode;
    }

    /**
     * @return the root node of the tree.
     */
    public MLDecisionTreeNode getRootNode() {
        return m_rootNode;
    }


    public void write(final DataOutput out) throws IOException {
        writeRecursively(m_rootNode, out);
    }

    private static void writeRecursively(final MLDecisionTreeNode node, final DataOutput out) throws IOException {
        node.write(out);
        if (node.getLeftNode() != null) {
            writeRecursively((MLDecisionTreeNode)node.getLeftNode(), out);
            writeRecursively((MLDecisionTreeNode)node.getRightNode(), out);
        }
    }

    public static MLDecisionTree read(final DataInput in) throws IOException {
        final MLDecisionTreeNode rootNode = new MLDecisionTreeNode(null);
        readRecursively(rootNode, in);
        return new MLDecisionTree(rootNode);
    }

    private static void readRecursively(final MLDecisionTreeNode node, final DataInput in) throws IOException {
        node.read(in);

        if (node.numDescendants() > 0) {
            MLDecisionTreeNode leftChild = new MLDecisionTreeNode(node);
            node.setLeftNode(leftChild);
            readRecursively(leftChild, in);

            MLDecisionTreeNode rightChild = new MLDecisionTreeNode(node);
            node.setRightNode(rightChild);
            readRecursively(rightChild, in);
        }
    }

}
