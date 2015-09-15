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
 *   Created on 30.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.linear.regression;

import org.apache.spark.mllib.regression.LinearRegressionModel;

import com.knime.bigdata.spark.jobserver.jobs.LinearRegressionWithSGDJob;
import com.knime.bigdata.spark.jobserver.jobs.SGDJob;
import com.knime.bigdata.spark.node.mllib.prediction.linear.AbstractLinearMethodsNodeFactory;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.SparkModelInterpreter;

/**
 *
 * @author koetter
 */
public class MLlibLinearRegressionNodeFactory extends AbstractLinearMethodsNodeFactory<LinearRegressionModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected SparkModelInterpreter<SparkModel<LinearRegressionModel>> getModelInterpreter() {
        return MLlibLinearRegressionInterpreter.getInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Class<? extends SGDJob> getJobClassPath() {
        return LinearRegressionWithSGDJob.class;
    }

}
