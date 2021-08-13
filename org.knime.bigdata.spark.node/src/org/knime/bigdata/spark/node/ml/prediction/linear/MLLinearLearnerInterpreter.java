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
package org.knime.bigdata.spark.node.ml.prediction.linear;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;

/**
 * ML-based linear learner models interpreter.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLLinearLearnerInterpreter implements ModelInterpreter<MLModel> {

    private static final long serialVersionUID = -3345033165871032959L;

    private final MLModelType m_modelType;

    /**
     * @param modelType The unique name of the model to interpret.
     */
    public MLLinearLearnerInterpreter(final MLModelType modelType) {
        m_modelType = modelType;
    }

    @Override
    public String getModelName() {
        return m_modelType.getUniqueName();
    }

    @Override
    public String getSummary(final MLModel linearLearnerModel) {
        return m_modelType.getUniqueName();
    }

    @Override
    public JComponent[] getViews(final MLModel linearLearnerModel) {
        return new JComponent[0];
    }

}
