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
 *   Created on Mar 23, 2018 by bjoern
 */
package org.knime.bigdata.spark.core.context;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public enum SparkContextIDScheme {

    /**
     * Scheme for Spark Jobserver-specific {@link SparkContextID}s.
     */
    SPARK_JOBSERVER("jobserver"),

    /**
     * Scheme for Spark Jobserver-specific {@link SparkContextID}s.
     */
    SPARK_LIVY("sparkLivy"),

    /**
     * Scheme for local Spark-specific {@link SparkContextID}s.
     */
    SPARK_LOCAL("sparkLocal");

    private final String m_scheme;

    SparkContextIDScheme(final String scheme) {
        m_scheme = scheme;
    }

    @Override
    public String toString() {
        return m_scheme;
    }
}
