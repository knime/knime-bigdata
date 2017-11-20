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
package org.knime.bigdata.spark.core.jar.bundle;

import java.util.ArrayList;

import org.knime.bigdata.spark.core.jar.FileBasedJarCollector;
import org.knime.bigdata.spark.core.jar.JarCollector;
import org.knime.bigdata.spark.core.jar.JobJar;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.Bundle;

/**
 * Combines OSGi bundles into one driver jar file.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class BundleGroupSparkJarProvider {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BundleGroupSparkJarProvider.class);

    private final SparkVersion m_version;

    private final String m_name;

    private final ArrayList<BundleSparkJarProvider> m_provider;

    /** Cached jar file */
    private JobJar m_jobJar = null;

    /**
     * Creates a jar file based on given bundles.
     *
     * @param version - Supported Spark version
     * @param name - Name of this driver
     * @param bundles - Bundles to include in JAR
     */
    public BundleGroupSparkJarProvider(final SparkVersion version, final String name, final Bundle... bundles) {
        final CompatibilityChecker checker = new FixedVersionCompatibilityChecker(version);
        m_version = version;
        m_name = name;
        m_provider = new ArrayList<>();
        for (Bundle bundle : bundles) {
            m_provider.add(new BundleSparkJarProvider(checker, bundle));
        }
    }

    /** @return Name of driver */
    public String getName() { return m_name; }

    /** @return Generates and returns the final (cached) job jar */
    public JobJar getJobJar() {
        final long startTime = System.currentTimeMillis();

        if (m_jobJar == null || !m_jobJar.getJarFile().exists()) {
            JarCollector collector = new FileBasedJarCollector(m_version);

            for (BundleSparkJarProvider provider : m_provider) {
                collector.addProviderID(provider.getProviderID());
                provider.collect(collector);
                LOGGER.debug("Provider " + provider.getProviderID() + " added to jar.");
            }

            m_jobJar = collector.getJobJar();

            final long durationTime = System.currentTimeMillis() - startTime;
            LOGGER.debug(
                "Time to collect all bundles for Spark version " + m_version.toString() + ": " + durationTime + " ms");
        }

        return m_jobJar;
    }

}
