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
 *   Created on Nov 1, 2016 by Sascha Wolke, KNIME.com
 */
package org.knime.bigdata.spark2_2.base;

import org.knime.bigdata.spark.core.jar.bundle.DefaultBundleGroupSparkJarProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.osgi.framework.Bundle;

import com.google.common.collect.ImmutableMap;

/**
 * Spark 2.0 driver bundle provider.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark_2_2_BundleGroupSparkJarProvider extends DefaultBundleGroupSparkJarProvider {

    /** Default constructor. */
    public Spark_2_2_BundleGroupSparkJarProvider() {
        super(SparkVersion.V_2_2, ImmutableMap.of(
            "com.databricks.spark.avro", new Bundle[] {
                    getBundle("com.databricks.spark-avro_2.11", "4.0.0") }));
    }
}
