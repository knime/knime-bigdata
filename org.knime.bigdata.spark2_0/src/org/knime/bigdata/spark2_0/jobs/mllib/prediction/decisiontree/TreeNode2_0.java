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
package org.knime.bigdata.spark2_0.jobs.mllib.prediction.decisiontree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.tree.model.Node;

import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;

import scala.collection.Iterator;


/**
 *
 * @author Ole Ostergaard
 */
public class TreeNode2_0 implements TreeNode {

    private Node m_node;
    private Node m_rootNode;
    private Node m_parentNode; //must be found using the rootNode m_parentNode = Node.get(rootnode, node.getparentid())

    /**
     * @param rootNode the root node to be wrapped
     */
    public TreeNode2_0(final Node rootNode) {
        m_node = rootNode;
        m_rootNode = rootNode;
    }

    /**
     * @param node the node to be wrapped
     */
    public TreeNode2_0(final Node node, final Node rootNode) {
        m_node = node;
        m_rootNode = rootNode;
        m_parentNode = Node.getNode(Node.parentIndex(node.id()), m_rootNode);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeaf() {
        return m_node.isLeaf();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getPrediction() {
        return m_node.predict().predict();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getProbability() {
        return m_node.predict().prob();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasStats() {
        return m_node.stats().isDefined() ? true : false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getGain() {
        return m_node.stats().get().gain();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getImpurity() {
        return m_node.impurity();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getLeftImpurity() {
        return m_node.stats().get().leftImpurity();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getRightImpurity() {
        return m_node.stats().get().rightImpurity();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer numDescendants() {
        return m_node.numDescendants();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCategorical() {
        return m_parentNode.split().get().featureType().toString().equalsIgnoreCase("continuous") ? false : true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getThreshold() {
        return m_parentNode.split().get().threshold();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeftChild() {
        return Node.isLeftChild(m_node.id());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object> getCategories() {
            List<Object> javaCat = new ArrayList<>();
         scala.collection.immutable.List<Object> categories = m_parentNode.split().get().categories();
         Iterator<Object> iterator = categories.iterator();
         while (iterator.hasNext()) {
             javaCat.add(iterator.next());
         }

         return javaCat;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getParentSplitFeature() {
        return m_parentNode.split().get().feature();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getSplitFeature() {
        if (m_node.split().isDefined()) {
            return m_node.split().get().feature();
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeNode getLeftNode() {
        if (m_node.leftNode().isDefined()) {
            return new TreeNode2_0(m_node.leftNode().get(), m_rootNode);
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeNode getRightNode() {
        if (m_node.rightNode().isDefined()) {
            return new TreeNode2_0(m_node.rightNode().get(), m_rootNode);
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getParentId() {
        return Node.parentIndex(m_node.id());
    }

    /**
     * @return the actual node
     */
    public Node getNode() {
        return m_node;
    }

    /**
     * @return the tree's root node
     */
    public Node getRootNode() {
        return m_rootNode;
    }
}
