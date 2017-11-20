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
 *   Created on 04.03.2016 by koetter
 */
package org.knime.bigdata.spark1_5.base;

import org.knime.bigdata.spark.core.jar.DefaultSparkJarProvider;
import org.knime.bigdata.spark.core.jar.JobsPluginJarProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Implementation of the {@link DefaultSparkJarProvider} for Spark 1.5.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_5_JarProvider extends JobsPluginJarProvider {

    /**
     * Default constructor.
     */
    public Spark_1_5_JarProvider() {
        super(SparkVersion.V_1_5, JobserverSparkJob.class);
    }
}
