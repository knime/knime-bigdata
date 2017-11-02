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
package com.knime.bigdata.spark.core.node;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.core.version.SparkProviderRegistry;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Registry that stores all {@link SparkNodeFactory}.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkNodeFactoryRegistry extends SparkProviderRegistry<SparkNodeFactoryProvider> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkNodeFactoryRegistry.class);

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "com.knime.bigdata.spark.core.SparkNodeFactoryProvider";

    private static SparkNodeFactoryRegistry instance;

    private final Map<String, SparkNodeFactory<?>> m_factory = new LinkedHashMap<>();

    private static final Map<SparkVersion, Set<String>> m_idsPerVersion = new LinkedHashMap<>();

    private SparkNodeFactoryRegistry() {}

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static SparkNodeFactoryRegistry getInstance() {
        if (instance == null) {
            instance = new SparkNodeFactoryRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    @Override
    protected void addProvider(final SparkNodeFactoryProvider provider) {
        final Collection<SparkNodeFactory<?>> factories = provider.get();
        final Set<String> nodeIds = new LinkedHashSet<>(factories.size());
        for (final SparkNodeFactory<?> factory : factories) {
            final String id = factory.getId();
            final SparkNodeFactory<?> old = m_factory.get(id);
            if (old != null) {
                final String msg = "Duplicate Spark node factory provider detected for id: " + id;
                LOGGER.warn(msg + "Class name 1: " + old.getClass().getName() + " Class name 2: "
                    + factory.getClass().getName());
                throw new IllegalStateException(msg);
            }
            m_factory.put(id, factory);
            nodeIds.add(id);
        }
        final Set<SparkVersion> sparkVersions = provider.getSupportedSparkVersions();
        for (SparkVersion sparkVersion : sparkVersions) {
            m_idsPerVersion.put(sparkVersion, nodeIds);
        }
    }

    /**
     * @param nodeId the unique node id
     * @return the corresponding {@link SparkNodeFactory} or <code>null</code> if none compatible exists
     */
    @SuppressWarnings("unchecked")
    public synchronized static <T extends SparkNodeModel> SparkNodeFactory<T> get(final String nodeId) {
        final SparkNodeFactory<T> provider = (SparkNodeFactory<T>)getInstance().m_factory.get(nodeId);
        return provider;
    }

    /**
     * @param sparkVersion Spark version
     * @return the ids of all compatible {@link SparkNodeFactory}
     * @see #get(String) to get the provider for the given id or an empty {@link Set} if no node is compatible with the
     *      given SparkVersion
     */
    public synchronized static Collection<String> getNodeIds(final SparkVersion sparkVersion) {
        final Set<String> idSet = m_idsPerVersion.get(sparkVersion);
        return idSet != null ? idSet : Collections.<String> emptyList();
    }

    /**
     * @return all available {@link SparkNodeFactory} ids
     */
    public synchronized static Collection<String> getNodeIds() {
        return Collections.unmodifiableCollection(getInstance().m_factory.keySet());
    }
}
