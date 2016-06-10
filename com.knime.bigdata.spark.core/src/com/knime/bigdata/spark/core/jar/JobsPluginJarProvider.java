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
 *   Created on Jun 10, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.jar;

import java.io.File;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Convenience class that generalizes the way {@link SparkClass}es are collected from KNIME's own Spark job plugins.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JobsPluginJarProvider extends DefaultSparkJarProvider {

    private final String m_jobserverSparkJobClass;

    /**
     * Default constructor.
     */
    public JobsPluginJarProvider(final SparkVersion sparkVersion, final Class<?> jobserverSparkJobClass) {
        super(sparkVersion, KNIMEPluginScanPredicates.KNIME_JOBS_PLUGIN_PREDICATE,
            KNIMEPluginScanPredicates.KNIME_JAR_PREDICATE,
            KNIMEPluginScanPredicates.KNIME_JOBSERVER_UTILS_JAR_PREDICATE);

        m_jobserverSparkJobClass = jobserverSparkJobClass.getName();
    }

    @Override
    public void collect(final JarCollector collector) {
        super.collect(collector);
        collector.setJobserverJobClass(m_jobserverSparkJobClass);
    }

    /**
     * The default implementation loops through all classes and checks if the {@link SparkClass} annotation is present.
     *
     * @param collector the {@link JarCollector}
     * @param file the jar {@link File} to add
     */
    @Override
    public void scanJar(final JarCollector collector, final File file) {
        if (KNIMEPluginScanPredicates.KNIME_JOBSERVER_UTILS_JAR_PREDICATE.test(file.getName())) {
            // directly add all classes of the job server utils jar
            collector.addJar(file);
        } else {
            super.scanJar(collector, file);
        }
    }
}
