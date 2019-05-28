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
package org.knime.bigdata.spark2_3.jobs.mllib.prediction.linear.svm;

import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.node.mllib.prediction.linear.svm.MLlibSVMNodeFactory;
import org.knime.bigdata.spark2_3.api.Spark_2_3_MLlibModelHelper;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SVMModelHelper extends Spark_2_3_MLlibModelHelper {

    /**
     * Constructor.
     */
    public SVMModelHelper() {
        super(MLlibSVMNodeFactory.MODEL_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelInterpreter getModelInterpreter() {
        return SVMInterpreter.getInstance();
    }

}
