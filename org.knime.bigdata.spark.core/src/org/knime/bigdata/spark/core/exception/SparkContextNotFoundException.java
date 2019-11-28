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
 *   Created on May 3, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.exception;

import org.knime.bigdata.spark.core.context.SparkContextID;

/**
 * This exception indicates that the Job Server is still running fine but the Spark context no longer exists.
 * This might happen for example if the context has been destroyed or the Job Server has been restarted.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkContextNotFoundException extends KNIMESparkException {

    private static final long serialVersionUID = 1L;
    private final SparkContextID m_contextID;

    /**
     * Constructor
     */
    public SparkContextNotFoundException() {
        super("Spark context does not exist in the cluster. Please create a context first.");
        m_contextID = null;
    }

    /**
     * Constructor
     * @param contextID the {@link SparkContextID} that does not exist anymore
     */
    public SparkContextNotFoundException(final SparkContextID contextID) {
        super(String.format("Spark context '%s' does not exist in the cluster. Please create a context first.",
            contextID.toString()));
        m_contextID = contextID;
    }

    /**
     * @return the {@link SparkContextID} that no longer exists or {@code null}
     */
    public SparkContextID getSparkContextID() {
        return m_contextID;
    }
}
