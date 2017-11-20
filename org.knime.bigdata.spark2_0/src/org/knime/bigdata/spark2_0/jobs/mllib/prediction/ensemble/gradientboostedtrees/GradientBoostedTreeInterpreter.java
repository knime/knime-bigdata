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
 *   Created on 21.07.2015 by koetter
 */
package org.knime.bigdata.spark2_0.jobs.mllib.prediction.ensemble.gradientboostedtrees;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;

import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.view.TreeNode;
import org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees.MLlibGradientBoostedTreeNodeModel;
import org.knime.bigdata.spark2_0.jobs.mllib.prediction.ensemble.MLlibTreeEnsembleModelInterpreter;


/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class GradientBoostedTreeInterpreter extends MLlibTreeEnsembleModelInterpreter<GradientBoostedTreesModel> {
    //implements SparkModelInterpreter<SparkModel<DecisionTreeModel>> {

    private static final long serialVersionUID = 1L;

    private static volatile GradientBoostedTreeInterpreter instance;

    private GradientBoostedTreeInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static GradientBoostedTreeInterpreter getInstance() {
        if (instance == null) {
            synchronized (GradientBoostedTreeInterpreter.class) {
                if (instance == null) {
                    instance = new GradientBoostedTreeInterpreter();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return MLlibGradientBoostedTreeNodeModel.MODEL_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TreeNode getRootNode(final DecisionTreeModel treeModel, final boolean isClassification) {
        return new GradientTreeNode2_0(treeModel.topNode(), isClassification);
    }

}
