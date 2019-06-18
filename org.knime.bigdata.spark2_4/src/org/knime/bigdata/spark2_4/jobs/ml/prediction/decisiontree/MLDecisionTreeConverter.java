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
package org.knime.bigdata.spark2_4.jobs.ml.prediction.decisiontree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.tree.CategoricalSplit;
import org.apache.spark.ml.tree.ContinuousSplit;
import org.apache.spark.ml.tree.InternalNode;
import org.apache.spark.ml.tree.Node;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTree;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeEnsemble;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeNode;

/**
 * Utility class to convert Spark decision trees and its derivatives into KNIME's {@MLDecisionTree}s and its
 * derivatives.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeConverter {

    /**
     * Converts the given decision tree classification model into a {@link MLDecisionTree}.
     *
     * @param model The tree model to convert.
     * @return a new {@link MLDecisionTree} instance.
     */
    public static MLDecisionTree convert(final DecisionTreeClassificationModel model) {
        final MLDecisionTreeNode rootNode = new MLDecisionTreeNode(null);
        copyAttributesRecursively(model.rootNode(), null, rootNode);
        return new MLDecisionTree(rootNode);
    }

    /**
     * Converts the given decision tree regression model into a {@MLDecisionTree}.
     *
     * @param model The tree model to convert.
     * @return a new {@link MLDecisionTree} instance.
     */
    public static MLDecisionTree convert(final DecisionTreeRegressionModel model) {
        final MLDecisionTreeNode rootNode = new MLDecisionTreeNode(null);
        copyAttributesRecursively(model.rootNode(), null, rootNode);
        return new MLDecisionTree(rootNode);
    }

    /**
     * Converts the given GBT classification model into a {@link MLDecisionTreeEnsemble}.
     *
     * @param gbtModel The GBT classification model to convert.
     * @return a new {@link MLDecisionTreeEnsemble} instance.
     */
    public static MLDecisionTreeEnsemble convert(final GBTClassificationModel gbtModel) {
        final DecisionTreeRegressionModel[] trees = gbtModel.trees();
        final List<MLDecisionTree> knimeTrees = new ArrayList<>(gbtModel.getNumTrees());

        for (DecisionTreeRegressionModel tree : trees) {
            knimeTrees.add(convert(tree));
        }

        return new MLDecisionTreeEnsemble(knimeTrees);
    }

    /**
     * Converts the given GBT regression model into a {@link MLDecisionTreeEnsemble}.
     *
     * @param gbtModel The GBT regression model to convert.
     * @return a new {@link MLDecisionTreeEnsemble} instance.
     */
    public static MLDecisionTreeEnsemble convert(final GBTRegressionModel gbtModel) {
        final DecisionTreeRegressionModel[] trees = gbtModel.trees();
        final List<MLDecisionTree> knimeTrees = new ArrayList<>(gbtModel.getNumTrees());

        for (DecisionTreeRegressionModel tree : trees) {
            knimeTrees.add(convert(tree));
        }

        return new MLDecisionTreeEnsemble(knimeTrees);
    }

    /**
     * Converts the given random forest classification model into a {@link MLDecisionTreeEnsemble}.
     *
     * @param randomForestModel The random forest model to convert.
     * @return a new {@link MLDecisionTreeEnsemble} instance.
     */
    public static MLDecisionTreeEnsemble convert(final RandomForestClassificationModel randomForestModel) {
        final DecisionTreeClassificationModel[] trees = randomForestModel.trees();
        final List<MLDecisionTree> knimeTrees = new ArrayList<>(randomForestModel.getNumTrees());

        for (DecisionTreeClassificationModel tree : trees) {
            knimeTrees.add(convert(tree));
        }

        return new MLDecisionTreeEnsemble(knimeTrees);
    }

    /**
     * Converts the given random forest regression model into a {@link MLDecisionTreeEnsemble}.
     *
     * @param randomForestModel The random forest model to convert.
     * @return a new {@link MLDecisionTreeEnsemble} instance.
     */
    public static MLDecisionTreeEnsemble convert(final RandomForestRegressionModel randomForestModel) {
        final DecisionTreeRegressionModel[] trees = randomForestModel.trees();
        final List<MLDecisionTree> knimeTrees = new ArrayList<>(randomForestModel.getNumTrees());

        for (DecisionTreeRegressionModel tree : trees) {
            knimeTrees.add(convert(tree));
        }

        return new MLDecisionTreeEnsemble(knimeTrees);
    }

    private static void copyAttributesRecursively(final Node from, final InternalNode fromParent,
        final MLDecisionTreeNode to) {

        to.setImpurity(from.impurity());
        to.setPrediction(from.prediction());

        if (fromParent != null) {
            if (fromParent.split() instanceof CategoricalSplit) {
                final CategoricalSplit parentSplit = (CategoricalSplit)fromParent.split();
                to.setCategorical(true);
                transferCategoricalSplit(from, fromParent, to, parentSplit);
            } else {
                final ContinuousSplit parentSplit = (ContinuousSplit)fromParent.split();
                to.setCategorical(false);
                to.setThreshold(parentSplit.threshold());
            }
        }

        if (from instanceof InternalNode) {
            final InternalNode internalFrom = (InternalNode)from;
            to.setGain(internalFrom.gain());
            to.setNumDescendants(from.numDescendants());
            to.setSplitFeature(internalFrom.split().featureIndex());

            MLDecisionTreeNode toLeftNode = new MLDecisionTreeNode(to);
            to.setLeftNode(toLeftNode);
            copyAttributesRecursively(internalFrom.leftChild(), internalFrom, toLeftNode);

            MLDecisionTreeNode toRightNode = new MLDecisionTreeNode(to);
            to.setRightNode(toRightNode);
            copyAttributesRecursively(internalFrom.rightChild(), internalFrom, toRightNode);
        }
    }

    private static void transferCategoricalSplit(final Node from, final InternalNode fromParent,
        final MLDecisionTreeNode to, final CategoricalSplit parentSplit) {

        List<Object> categories = new LinkedList<>();

        if (from == fromParent.leftChild()) {
            for (double catIndex : parentSplit.leftCategories()) {
                categories.add((int)catIndex);
            }
        } else {
            for (double catIndex : parentSplit.rightCategories()) {
                categories.add((int)catIndex);
            }
        }
        to.setCategories(categories);
    }
}
