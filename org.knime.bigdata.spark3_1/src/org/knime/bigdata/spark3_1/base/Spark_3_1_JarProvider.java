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
package org.knime.bigdata.spark3_1.base;

import java.util.HashMap;
import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.jar.DefaultSparkJarProvider;
import org.knime.bigdata.spark.core.jar.JobsPluginJarProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Implementation of the {@link DefaultSparkJarProvider} for Spark 2.0.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_3_1_JarProvider extends JobsPluginJarProvider {

    private final static Map<SparkContextIDScheme, Class<?>> JOB_BINDING_CLASSES = new HashMap<>();

    static {
        JOB_BINDING_CLASSES.put(SparkContextIDScheme.SPARK_LIVY, LivySparkJob.class);
        JOB_BINDING_CLASSES.put(SparkContextIDScheme.SPARK_DATABRICKS, DatabricksSparkJob.class);
    }

    /**
     * Default constructor.
     */
    public Spark_3_1_JarProvider() {
        super(SparkVersion.V_3_1, JOB_BINDING_CLASSES);
    }
}
