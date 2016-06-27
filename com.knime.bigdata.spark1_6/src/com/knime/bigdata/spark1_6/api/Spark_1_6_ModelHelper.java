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
 *   Created on Apr 13, 2016 by bjoern
 */
package com.knime.bigdata.spark1_6.api;

import com.knime.bigdata.spark.core.model.DefaultModelHelper;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public abstract class Spark_1_6_ModelHelper extends DefaultModelHelper {

    /**
     * @param modelName
     */
    public Spark_1_6_ModelHelper(final String modelName) {
        super(modelName);
    }
}