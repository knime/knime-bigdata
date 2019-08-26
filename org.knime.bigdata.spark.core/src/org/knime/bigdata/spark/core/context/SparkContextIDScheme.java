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

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @since 2.3.0
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
     * Scheme for Spark Databricks-specific {@link SparkContextID}s.
     */
    SPARK_DATABRICKS("sparkDatabricks"),

    /**
     * Scheme for local Spark-specific {@link SparkContextID}s.
     */
    SPARK_LOCAL("sparkLocal");

    private static final Map<String,SparkContextIDScheme> SCHEMES = new HashMap<>();
    static {
        for(SparkContextIDScheme scheme : values()) {
            SCHEMES.put(scheme.toString(), scheme);
        }
    }


    private final String m_scheme;

    SparkContextIDScheme(final String scheme) {
        m_scheme = scheme;
    }

    @Override
    public String toString() {
        return m_scheme;
    }

    /**
     * Returns the enum constant for a given scheme string.
     *
     * @param schemeString The scheme string, e.g. extracted from a URL.
     * @return the respective enum constant.
     * @throws IllegalArgumentException if there was no enum constant for the given scheme string.
     */
    public static SparkContextIDScheme fromString(final String schemeString) {
        final  SparkContextIDScheme toReturn = SCHEMES.get(schemeString);

        if (toReturn == null) {
            throw new IllegalArgumentException("There is no Spark context ID scheme called " + schemeString);
        }

        return toReturn;
    }
}
