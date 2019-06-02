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
 *   Created on May 27, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.model;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public interface MLModelHelper extends ModelHelper<MLModel> {

    /**
     * Checks if the provided model is already registered as a named object in the given Spark context. If not, the
     * model will be uploaded and registered.
     *
     * @param sparkContext
     * @param model
     * @param exec
     * @throws KNIMESparkException
     * @throws CanceledExecutionException
     */
    void uploadModelToSparkIfNecessary(SparkContextID sparkContext, MLModel model, ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException;

}
