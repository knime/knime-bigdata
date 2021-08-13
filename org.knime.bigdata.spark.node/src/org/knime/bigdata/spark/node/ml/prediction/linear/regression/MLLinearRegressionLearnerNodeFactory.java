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
 */
package org.knime.bigdata.spark.node.ml.prediction.linear.regression;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerNodeDialog;
import org.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerSettings;
import org.knime.core.node.NodeDialogPane;

/**
 * Spark ml-based Linear Regression Learner node factory.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLLinearRegressionLearnerNodeFactory
    extends DefaultSparkNodeFactory<MLLinearRegressionLearnerNodeModel> {

    /**
     * Default constructor.
     */
    public MLLinearRegressionLearnerNodeFactory() {
        super("mining/prediction");
    }

    @Override
    public MLLinearRegressionLearnerNodeModel createNodeModel() {
        return new MLLinearRegressionLearnerNodeModel();
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new LinearLearnerNodeDialog(new LinearLearnerSettings(LinearLearnerMode.LINEAR_REGRESSION));
    }

}
