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

import org.knime.bigdata.spark.core.model.DefaultMLModelHelper;
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.node.ml.prediction.linear.MLLinearLearnerInterpreter;

/**
 * Model helper for {@link MLModel}-based linear regression models.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLLinearRegressionModelHelper extends DefaultMLModelHelper {

    /** Constructor. */
    public MLLinearRegressionModelHelper() {
        super(MLLinearRegressionLearnerNodeModel.MODEL_TYPE);
    }

    @Override
    public ModelInterpreter<MLModel> getModelInterpreter() {
        return new MLLinearLearnerInterpreter(MLLinearRegressionLearnerNodeModel.MODEL_TYPE);
    }
}
