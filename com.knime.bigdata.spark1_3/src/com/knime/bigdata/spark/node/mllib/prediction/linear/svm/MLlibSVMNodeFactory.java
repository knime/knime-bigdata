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
package com.knime.bigdata.spark.node.mllib.prediction.linear.svm;

import org.apache.spark.mllib.classification.SVMModel;

import com.knime.bigdata.spark.jobserver.jobs.SVMLearnerJob;
import com.knime.bigdata.spark.node.mllib.prediction.linear.AbstractLinearMethodsNodeFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibSVMNodeFactory extends AbstractLinearMethodsNodeFactory<SVMModel> {

    /**
     * Constructor.
     */
    public MLlibSVMNodeFactory() {
        super(MLlibSVMInterpreter.getInstance(), SVMLearnerJob.class, false);
    }
}