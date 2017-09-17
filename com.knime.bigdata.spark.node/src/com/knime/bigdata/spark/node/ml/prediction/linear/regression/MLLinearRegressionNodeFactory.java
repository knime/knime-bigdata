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
package com.knime.bigdata.spark.node.ml.prediction.linear.regression;

import com.knime.bigdata.spark.node.ml.prediction.linear.AbstractLinearMethodsNodeFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLLinearRegressionNodeFactory extends AbstractLinearMethodsNodeFactory {

    /**The unique model name.*/
    public static final String MODEL_NAME = "Linear Regression (ML)";

    /**The unique job id.*/
    public static final String JOB_ID = MLLinearRegressionNodeFactory.class.getCanonicalName();

    /**
     * Constructor.
     */
    public MLLinearRegressionNodeFactory() {
        super(MODEL_NAME, JOB_ID);
    }
}
