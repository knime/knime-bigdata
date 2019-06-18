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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeComponents;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeNodeDialog;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeSettings;
import org.knime.core.node.NodeDialogPane;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLDecisionTreeClassificationLearnerNodeFactory extends DefaultSparkNodeFactory<MLDecisionTreeClassificationLearnerNodeModel> {

    /**
     * Constructor.
     */
    public MLDecisionTreeClassificationLearnerNodeFactory() {
        super("mining/prediction");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MLDecisionTreeClassificationLearnerNodeModel createNodeModel() {
        return new MLDecisionTreeClassificationLearnerNodeModel();
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
        final DecisionTreeSettings settings = new DecisionTreeSettings(DecisionTreeLearnerMode.CLASSIFICATION);

        final DecisionTreeComponents<DecisionTreeSettings> components =
            new DecisionTreeComponents<DecisionTreeSettings>(settings);

        return new DecisionTreeNodeDialog<>(components);
    }

}
