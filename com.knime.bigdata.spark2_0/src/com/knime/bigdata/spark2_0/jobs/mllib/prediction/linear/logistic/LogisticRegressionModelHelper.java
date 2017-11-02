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
 *   Created on 06.05.2016 by koetter
 */
package com.knime.bigdata.spark2_0.jobs.mllib.prediction.linear.logistic;

import com.knime.bigdata.spark.core.port.model.ModelInterpreter;
import com.knime.bigdata.spark.node.mllib.prediction.linear.logisticregression.MLlibLogisticRegressionNodeFactory;
import com.knime.bigdata.spark2_0.api.Spark_2_0_ModelHelper;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LogisticRegressionModelHelper extends Spark_2_0_ModelHelper {

    /**
     * Constructor.
     */
    public LogisticRegressionModelHelper() {
        super(MLlibLogisticRegressionNodeFactory.MODEL_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelInterpreter getModelInterpreter() {
        return LogisticRegressionInterpreter.getInstance();
    }

}
