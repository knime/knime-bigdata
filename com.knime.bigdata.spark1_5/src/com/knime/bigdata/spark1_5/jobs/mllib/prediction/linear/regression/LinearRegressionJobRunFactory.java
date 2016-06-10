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
 *   Created on 29.01.2016 by koetter
 */
package com.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.regression;

import com.knime.bigdata.spark.node.mllib.prediction.linear.regression.MLlibLinearRegressionNodeFactory;
import com.knime.bigdata.spark1_5.jobs.mllib.prediction.linear.DefaultRegularizationJobRunFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LinearRegressionJobRunFactory
    extends DefaultRegularizationJobRunFactory {

    /**Constructor.*/
    public LinearRegressionJobRunFactory() {
        super(MLlibLinearRegressionNodeFactory.JOB_ID, LinearRegressionJob.class);
    }
}
