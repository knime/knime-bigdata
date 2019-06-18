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
package org.knime.bigdata.spark.node.ml.prediction.randomforest.regression;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.randomforest.RandomForestLearnerComponents;
import org.knime.bigdata.spark.node.ml.prediction.randomforest.RandomForestLearnerNodeDialog;
import org.knime.bigdata.spark.node.ml.prediction.randomforest.RandomForestLearnerSettings;
import org.knime.core.node.NodeDialogPane;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLRandomForestRegressionLearnerNodeFactory
    extends DefaultSparkNodeFactory<MLRandomForestRegressionLearnerNodeModel> {

    /**
     * Constructor.
     */
    public MLRandomForestRegressionLearnerNodeFactory() {
        super("mining/prediction");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MLRandomForestRegressionLearnerNodeModel createNodeModel() {
        return new MLRandomForestRegressionLearnerNodeModel();
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
        final RandomForestLearnerSettings settings =
            new RandomForestLearnerSettings(DecisionTreeLearnerMode.REGRESSION);
        final RandomForestLearnerComponents components = new RandomForestLearnerComponents(settings);

        return new RandomForestLearnerNodeDialog(components);
    }

}
