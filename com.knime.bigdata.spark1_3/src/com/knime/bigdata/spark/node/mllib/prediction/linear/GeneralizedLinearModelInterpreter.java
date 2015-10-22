/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 02.10.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import java.util.List;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> the {@link GeneralizedLinearModel} implementation
 */
public class GeneralizedLinearModelInterpreter<M extends GeneralizedLinearModel> extends
    HTMLModelInterpreter<SparkModel<M>> {

    private static final long serialVersionUID = 1L;

    private final String m_modelName;

    /**
     * Constructor.
     * @param modelName the name of the model
     */
    protected GeneralizedLinearModelInterpreter(final String modelName) {
        m_modelName = modelName;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return m_modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel<M> sparkModel) {
        final GeneralizedLinearModel regressionModel = sparkModel.getModel();
        final Vector weightsVec = regressionModel.weights();
        final String weightString = LinearMethodsNodeModel.printWeights(weightsVec, NF);
        return "Model weights: " + weightString;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getHTMLDescription(final SparkModel<M> sparkModel) {
        final GeneralizedLinearModel model = sparkModel.getModel();
        final List<String> columnNames = sparkModel.getLearningColumnNames();
        final double[] weights = model.weights().toArray();
        return LinearMethodsNodeModel.printWeightedColumnHTMLList("Weight", columnNames, NF, weights);
    }

}
