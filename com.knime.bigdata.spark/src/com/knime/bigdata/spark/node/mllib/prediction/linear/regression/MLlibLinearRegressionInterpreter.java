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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.linear.regression;

import org.apache.spark.mllib.regression.LinearRegressionModel;

import com.knime.bigdata.spark.port.model.SparkModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibLinearRegressionInterpreter implements SparkModelInterpreter<LinearRegressionModel> {

    private static final long serialVersionUID = 1L;

    private static volatile MLlibLinearRegressionInterpreter instance;

    private MLlibLinearRegressionInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static MLlibLinearRegressionInterpreter getInstance() {
        if (instance == null) {
            synchronized (MLlibLinearRegressionInterpreter.class) {
                if (instance == null) {
                    instance = new MLlibLinearRegressionInterpreter();
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
        return "Linear Regression";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final LinearRegressionModel model) {
        return "Model weights: " + model.weights();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription(final LinearRegressionModel model) {
        return "Model: " + model.weights();
    }

}
