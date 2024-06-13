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
 *   Created on 21.07.2015 by koetter
 */
package org.knime.bigdata.spark3_5.jobs.mllib.prediction.linear.svm;

import org.knime.bigdata.spark.node.mllib.prediction.linear.svm.MLlibSVMNodeFactory;
import org.knime.bigdata.spark3_5.jobs.mllib.prediction.linear.GeneralizedLinearModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SVMInterpreter extends GeneralizedLinearModelInterpreter {

    private static final long serialVersionUID = 1L;

    private static final SVMInterpreter instance = new SVMInterpreter();

    private SVMInterpreter() {
        super(MLlibSVMNodeFactory.MODEL_NAME);
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static SVMInterpreter getInstance() {
        return instance;
    }
}
