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
 *   Created on Apr 25, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.types;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public interface TypeConverter {

    /**
     * @return the name of the converter which is displayed to a user
     */
    String getName();

    /**
     * @return the description of the convert which might be displayed in user facing descriptions e.g. tool tips
     */
    String getDescription();

}