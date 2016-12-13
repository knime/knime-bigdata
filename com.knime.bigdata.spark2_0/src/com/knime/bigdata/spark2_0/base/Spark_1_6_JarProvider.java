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
 *   Created on 04.03.2016 by koetter
 */
package com.knime.bigdata.spark1_6.base;

import com.knime.bigdata.spark.core.jar.DefaultSparkJarProvider;
import com.knime.bigdata.spark.core.jar.JobsPluginJarProvider;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Implementation of the {@link DefaultSparkJarProvider} for Spark 1.6.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_6_JarProvider extends JobsPluginJarProvider {

    /**
     * Default constructor.
     */
    public Spark_1_6_JarProvider() {
        super(SparkVersion.V_1_6, JobserverSparkJob.class);
    }
}
