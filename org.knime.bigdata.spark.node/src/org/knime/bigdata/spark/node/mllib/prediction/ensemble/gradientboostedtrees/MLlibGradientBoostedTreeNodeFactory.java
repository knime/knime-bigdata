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
 *   Created on 27.09.2015 by koetter
 */
package org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.gbt.GradientBoostedTreesLearnerComponents;
import org.knime.bigdata.spark.node.ml.prediction.gbt.GradientBoostedTreesLearnerNodeDialog;
import org.knime.bigdata.spark.node.ml.prediction.gbt.GradientBoostedTreesLearnerSettings;
import org.knime.core.node.NodeDialogPane;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibGradientBoostedTreeNodeFactory extends DefaultSparkNodeFactory<MLlibGradientBoostedTreeNodeModel> {

    /**
     * Constructor.
     */
    public MLlibGradientBoostedTreeNodeFactory() {
        super("mining/prediction");
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public MLlibGradientBoostedTreeNodeModel createNodeModel() {
        return new MLlibGradientBoostedTreeNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        final GradientBoostedTreesLearnerSettings settings =
            new GradientBoostedTreesLearnerSettings(DecisionTreeLearnerMode.DEPRECATED);
        final GradientBoostedTreesLearnerComponents components = new GradientBoostedTreesLearnerComponents(settings);

        return new GradientBoostedTreesLearnerNodeDialog(components);
    }
}
