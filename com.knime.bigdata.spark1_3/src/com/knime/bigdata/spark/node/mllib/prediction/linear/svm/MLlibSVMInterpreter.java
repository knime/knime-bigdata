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
package com.knime.bigdata.spark.node.mllib.prediction.linear.svm;

import org.apache.spark.mllib.classification.SVMModel;

import com.knime.bigdata.spark.node.mllib.prediction.linear.GeneralizedLinearModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibSVMInterpreter extends GeneralizedLinearModelInterpreter<SVMModel> {

    private static final long serialVersionUID = 1L;

    private static final MLlibSVMInterpreter instance = new MLlibSVMInterpreter();

    private MLlibSVMInterpreter() {
        super("Linear SVM");
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static MLlibSVMInterpreter getInstance() {
        return instance;
    }
}