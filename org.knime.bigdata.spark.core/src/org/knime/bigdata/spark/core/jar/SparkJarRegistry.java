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
 *   Created on 05.07.2015 by koetter
 */
package org.knime.bigdata.spark.core.jar;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.knime.core.node.NodeLogger;

import org.knime.bigdata.spark.core.version.SparkProviderRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Registry for {@link JarCollector} that collect the java classes that should be send to a Spark job server.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJarRegistry extends SparkProviderRegistry<SparkJarProvider> {

    private class LazyCollector {
        private final SparkVersion m_version;

        private final LinkedList<SparkJarProvider> m_registeredJarProviders = new LinkedList<>();

        private JobJar m_jobJar = null;

        private LazyCollector(final SparkVersion version) {
            m_version = version;
        }

        private void addProvider(final SparkJarProvider provider) {
            m_registeredJarProviders.add(provider);
        }

        private JobJar getJobJar() {
            final long startTime = System.currentTimeMillis();

            if (m_jobJar == null || !m_jobJar.getJarFile().exists()) {
                JarCollector collector = new FileBasedJarCollector(m_version);

                for (final BaseSparkJarProvider baseProvider : m_baseProviders) {
                    collector.addProviderID(baseProvider.getProviderID());
                    baseProvider.collect(collector);
                }

                for (final SparkJarProvider provider : m_registeredJarProviders) {
                    collector.addProviderID(provider.getProviderID());
                    provider.collect(collector);
                }

                m_jobJar = collector.getJobJar();

                final long durationTime = System.currentTimeMillis() - startTime;
                LOGGER.debug(
                    "Time to collect all jars for Spark version " + m_version.toString() + ": " + durationTime + " ms");
            }

            return m_jobJar;
        }
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkJarRegistry.class);

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "org.knime.bigdata.spark.core.SparkJarProvider";

    private static SparkJarRegistry instance;

    private final Map<SparkVersion, LazyCollector> m_jar = new HashMap<>();

    private final Set<BaseSparkJarProvider> m_baseProviders = new HashSet<>();

    private SparkJarRegistry() {
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static SparkJarRegistry getInstance() {
        if (instance == null) {
            instance = new SparkJarRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProvider(final SparkJarProvider provider) {

        // do not create a lazy collector for base jar providers
        if (provider instanceof BaseSparkJarProvider) {
            m_baseProviders.add((BaseSparkJarProvider)provider);
            return;
        }

        final Set<SparkVersion> sparkVersions = provider.getSupportedSparkVersions();
        for (SparkVersion version : sparkVersions) {
            LazyCollector collector = m_jar.get(version);
            if (collector == null) {
                collector = new LazyCollector(version);
                m_jar.put(version, collector);
            }
            collector.addProvider(provider);
        }
    }

    /**
     * Returns the jar file for the given Spark version
     *
     * @param sparkVersion the Spark version to get the jar files for e.g. 1.2, 1.3, etc
     * @return the corresponding jar file or <code>null</code> if none exists
     */
    public synchronized static JobJar getJobJar(final SparkVersion sparkVersion) {
        LazyCollector collector = getInstance().m_jar.get(sparkVersion);
        if (collector != null) {
            return collector.getJobJar();
        }
        return null;
    }
}
