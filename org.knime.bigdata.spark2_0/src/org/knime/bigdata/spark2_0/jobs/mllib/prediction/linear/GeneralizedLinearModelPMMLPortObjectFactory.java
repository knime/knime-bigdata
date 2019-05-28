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
 *   Created on Oct 28, 2015 by ste
 */
package org.knime.bigdata.spark2_0.jobs.mllib.prediction.linear;

import java.util.List;

import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.knime.base.node.mine.regression.PMMLRegressionTranslator.NumericPredictor;
import org.knime.base.node.mine.regression.PMMLRegressionTranslator.RegressionTable;
import org.knime.bigdata.spark.core.port.model.MLlibModel;
import org.knime.bigdata.spark.node.pmml.converter.PMMLPortObjectFactory;
import org.knime.core.node.InvalidSettingsException;

/**
 *
 * @author Stefano Baghino <stefano.baghino@databiz.it>
 */
public abstract class GeneralizedLinearModelPMMLPortObjectFactory implements PMMLPortObjectFactory {

    /**
     * @param knimeModel
     * @return The regression table extrapolated from the model's intercept and weights
     * @throws InvalidSettingsException
     */
    protected RegressionTable regressionTableFromModel(final MLlibModel knimeModel) throws InvalidSettingsException {
        final GeneralizedLinearModel model = (GeneralizedLinearModel) knimeModel.getModel();
        final double intercept = model.intercept();
        final double[] weights = model.weights().toArray();

        final int numWeights = weights.length;
        NumericPredictor[] numericPredictors = new NumericPredictor[numWeights];
        List<String> columnNames = knimeModel.getLearningColumnNames();
        for (int i = 0; i < numWeights; i++) {
            numericPredictors[i] = new NumericPredictor(columnNames.get(i), 1, weights[i]);
        }

        return new RegressionTable(intercept, numericPredictors);

    }

}
