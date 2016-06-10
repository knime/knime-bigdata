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
 *   Created on 06.05.2016 by koetter
 */
package com.knime.bigdata.spark1_3.jobs.mllib.prediction.linear.regression;

import com.knime.bigdata.spark.core.port.model.ModelInterpreter;
import com.knime.bigdata.spark.node.mllib.prediction.linear.regression.MLlibLinearRegressionNodeFactory;
import com.knime.bigdata.spark1_3.base.Spark_1_3_ModelHelper;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LinearRegressionModelHelper extends Spark_1_3_ModelHelper {

    /**
     * Constructor.
     */
    public LinearRegressionModelHelper() {
        super(MLlibLinearRegressionNodeFactory.MODEL_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelInterpreter getModelInterpreter() {
        return LinearRegressionInterpreter.getInstance();
    }

}
