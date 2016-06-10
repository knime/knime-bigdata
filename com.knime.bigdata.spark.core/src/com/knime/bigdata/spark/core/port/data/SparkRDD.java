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
 *   Created on 29.05.2015 by koetter
 */
package com.knime.bigdata.spark.core.port.data;

import com.knime.bigdata.spark.core.context.SparkContextID;

/**
 * This class represents a data object within Spark. The object is identified by the Spark context
 * ({@link #getContextID()}) it lives in and its unique id ({@link #getID()})
 * @author Tobias Koetter, KNIME.com
 */
public interface SparkRDD {

    /**
     * @return the unique id of the Spark object
     */
    public abstract String getID();

    /**
     * @return the Spark context the object lives in
     */
    public abstract SparkContextID getContextID();

    /**
     * @param other the other {@link SparkRDD} to check for compatibility
     * @return <code>true</code> if both are compatible e.g. live in the same context
     */
    public boolean compatible(final SparkRDD other);

}