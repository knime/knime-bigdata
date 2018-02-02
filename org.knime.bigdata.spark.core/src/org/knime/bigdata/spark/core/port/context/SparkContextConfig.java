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
 *   Created on 26.01.2018 by Oleg Yasnev
 */
package org.knime.bigdata.spark.core.port.context;

import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Base interface for Spark context configuration classes.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @see SparkContext
 * @see SparkContextID
 */
public interface SparkContextConfig {

    /**
     * @return the Spark version.
     */
    SparkVersion getSparkVersion();

    /**
     * @return a human-readable name of the Spark context.
     */
    String getContextName();

    /**
     * @return <code>true</code> if Spark objects e.g. DataFrames/RDDs should be deleted when a KNIME workflow is
     *         closed.
     */
    boolean deleteObjectsOnDispose();

    /**
     * @return <code>true</code> for custom Spark settings
     * @see #getCustomSparkSettings()
     */
    boolean useCustomSparkSettings();

    /**
     * @return custom Spark settings as key/value pairs.
     * @see #useCustomSparkSettings()
     */
    Map<String, String> getCustomSparkSettings();

    /**
     * @return the ID of the Spark context that is configured with this config object.
     */
    SparkContextID getSparkContextID();
}
