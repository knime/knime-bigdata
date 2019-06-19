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
 *   Created on Jun 3, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.view.TreeNode;

/**
 * Tree node of a Spark ML Decision Tree.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeNode implements TreeNode {

    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

    private double m_impurity;

    private double m_prediction;

    private double m_probability;

    private double m_gain;

    private int m_numDescendants;

    private boolean m_isCategorical;

    // for numeric nodes
    private double m_threshold;

    // for categorical nodes
    private List<Object> m_categories;

    private int m_splitFeature;

    private final MLDecisionTreeNode m_parent;

    private MLDecisionTreeNode m_leftChild;

    private MLDecisionTreeNode m_rightChild;

    private final int m_id;

    /**
     * Creates a new instance.
     *
     * @param parent The parent node.
     */
    public MLDecisionTreeNode(final MLDecisionTreeNode parent) {
        m_parent = parent;
        m_id = ID_GENERATOR.getAndIncrement();
        m_probability = -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeaf() {
        // if left child is null, right one must be too
        return m_leftChild == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getPrediction() {
        return m_prediction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getProbability() {
        return m_probability;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasStats() {
        // stats are gain, left impurity, right impurity
        // we have stats if this is an internal node
        return m_leftChild != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getGain() {
        return m_gain;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getImpurity() {
        return m_impurity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getLeftImpurity() {
        if (m_leftChild != null) {
            return m_leftChild.getImpurity();
        } else {
            return 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getRightImpurity() {
        if (m_rightChild != null) {
            return m_rightChild.getImpurity();
        } else {
            return 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer numDescendants() {
        return m_numDescendants;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCategorical() {
        return m_isCategorical;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getThreshold() {
        return m_threshold;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeftChild() {
        return m_parent.getLeftNode() == this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object> getCategories() {
        // an example of really horrible API design: getCategories() is assumed to always
        // return the categories of the left side of the split. This has historical reasons. The
        // MLlib class for categorical splits just provides the categories for the left side of the split.
        // Changing this requires revising how the decision tree view displays categorical splits
        if (isLeftChild()) {
            return m_categories;
        } else {
            return m_parent.getLeftNode().getCategories();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getParentSplitFeature() {
        return m_parent.getSplitFeature();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getSplitFeature() {
        return m_splitFeature;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeNode getLeftNode() {
        return m_leftChild;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeNode getRightNode() {
        return m_rightChild;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getParentId() {
        if (m_parent != null) {
            return m_parent.m_id;
        } else {
            return null;
        }
    }

    /**
     * @param impurity the impurity to set
     */
    public void setImpurity(final double impurity) {
        m_impurity = impurity;
    }

    /**
     * @param gain the gain to set
     */
    public void setGain(final double gain) {
        m_gain = gain;
    }

    /**
     * @param prediction the prediction to set
     */
    public void setPrediction(final double prediction) {
        m_prediction = prediction;
    }

    /**
     * @param probability the probability to set
     */
    public void setProbability(final double probability) {
        m_probability = probability;
    }

    /**
     * @param numDescendants the numDescendants to set
     */
    public void setNumDescendants(final int numDescendants) {
        m_numDescendants = numDescendants;
    }

    /**
     * @param isCategorical the isCategorical to set
     */
    public void setCategorical(final boolean isCategorical) {
        m_isCategorical = isCategorical;
    }

    /**
     * @param threshold the threshold to set
     */
    public void setThreshold(final double threshold) {
        m_threshold = threshold;
    }

    /**
     * @param categories the categories to set
     */
    public void setCategories(final List<Object> categories) {
        m_categories = categories;
    }

    /**
     * @param splitFeature the splitFeature to set
     */
    public void setSplitFeature(final int splitFeature) {
        m_splitFeature = splitFeature;
    }

    /**
     * @param leftNode the leftChild to set
     */
    public void setLeftNode(final MLDecisionTreeNode leftNode) {
        m_leftChild = leftNode;
    }

    /**
     * @param rightNode the rightChild to set
     */
    public void setRightNode(final MLDecisionTreeNode rightNode) {
        m_rightChild = rightNode;
    }

    /**
     * Writes this tree node (but not its children) to the given {@link DataOutput}.
     *
     * @param out
     * @throws IOException
     */
    public void write(final DataOutput out) throws IOException {
        out.writeDouble(m_impurity);
        out.writeDouble(m_prediction);
        out.writeDouble(m_probability);

        out.writeBoolean(m_leftChild != null);
        if (m_leftChild != null) {
            // internal node
            out.writeInt(m_numDescendants);
            out.writeDouble(m_gain);
            out.writeInt(m_splitFeature);
        }

        if (m_parent != null) {
            out.writeBoolean(m_isCategorical);
            if (m_isCategorical) {
                out.writeInt(m_categories.size());
                for (Object cat : m_categories) {
                    out.writeInt((Integer)cat);
                }
            } else {
                out.writeDouble(m_threshold);
            }
        }
    }

    /**
     * Fills the contents of this tree node (but not its children) using the given {@link DataInput}.
     *
     * @param in
     * @throws IOException
     */
    public void read(final DataInput in) throws IOException {
        m_impurity = in.readDouble();
        m_prediction = in.readDouble();
        m_probability = in.readDouble();

        if (in.readBoolean()) {
            // internal node
            m_numDescendants = in.readInt();
            m_gain = in.readDouble();
            m_splitFeature = in.readInt();
        } else {
            m_gain = -1;
            m_numDescendants = 0;
            m_splitFeature = -1;
        }

        if (m_parent != null) {
            m_isCategorical = in.readBoolean();
            if (m_isCategorical) {
                // is categorical
                m_categories = new LinkedList<>();
                final int toRead = in.readInt();
                for (int i = 0; i < toRead; i++) {
                    m_categories.add((double) in.readInt());
                }
                m_threshold = -1;
            } else {
                m_threshold = in.readDouble();
            }
        }
    }
}
