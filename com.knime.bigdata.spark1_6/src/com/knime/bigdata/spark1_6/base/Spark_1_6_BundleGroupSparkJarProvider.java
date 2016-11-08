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
 *   Created on Nov 1, 2016 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark1_6.base;

import org.osgi.framework.Bundle;

import com.google.common.collect.ImmutableMap;
import com.knime.bigdata.spark.core.jar.bundle.DefaultBundleGroupSparkJarProvider;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Spark 1.6 driver bundle provider.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark_1_6_BundleGroupSparkJarProvider extends DefaultBundleGroupSparkJarProvider {

    /** Default constructor. */
    public Spark_1_6_BundleGroupSparkJarProvider() {
        super(SparkVersion.V_1_6, ImmutableMap.of(
            "com.databricks.spark.avro", new Bundle[] {
                    getBundle("databricks.spark-avro", "2.0.0", "3.0.0") },
            "com.databricks.spark.csv", new Bundle[] {
                    getBundle("databricks.spark-csv", "1.5.0"),
                    getBundle("org.apache.commons.csv", "1.1.0") }
        ));
    }
}