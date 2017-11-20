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
 *   Created on Nov 16, 2017 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.sql_function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.knime.bigdata.spark.core.version.SparkProviderRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.NodeLogger;

/**
 * Spark aggregation function registry.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkSQLFunctionDialogProviderRegistry extends SparkProviderRegistry<SparkSQLFunctionDialogProvider> {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkSQLFunctionDialogProviderRegistry.class);

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "org.knime.bigdata.spark.node.SparkSQLFunctionDialogProvider";

    private static SparkSQLFunctionDialogProviderRegistry instance;

    private final Map<SparkVersion, Set<SparkSQLFunctionDialogProvider>> m_provider = new LinkedHashMap<>();

    private SparkSQLFunctionDialogProviderRegistry() {}

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static SparkSQLFunctionDialogProviderRegistry getInstance() {
        if (instance == null) {
            instance = new SparkSQLFunctionDialogProviderRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    @Override
    protected void addProvider(final SparkSQLFunctionDialogProvider provider) {
        final Set<SparkVersion> sparkVersions = provider.getSupportedSparkVersions();
        for (SparkVersion sparkVersion : sparkVersions) {
            if (!m_provider.containsKey(sparkVersion)) {
                m_provider.put(sparkVersion, new LinkedHashSet<SparkSQLFunctionDialogProvider>());
            }
            m_provider.get(sparkVersion).add(provider);
        }
    }

    /**
     * Returns all aggregation function factories by spark version.
     * @param version Spark version to support
     * @return List of supported function factories
     */
    public List<SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction>> getAggregationFunctionFactories(final SparkVersion version) {
        if (m_provider.containsKey(version)) {
            final ArrayList<SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction>> result = new ArrayList<>();
            for (SparkSQLFunctionDialogProvider provider : m_provider.get(version)) {
                for (SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> factory : provider.getAggregationFunctionFactories()) {
                    if (result.contains(factory)) {
                        String msg = "Duplicated Spark Function Dialog Factory Provider detected! (function: " + factory.getId() + ")";
                        LOGGER.warn(msg);
                        throw new IllegalStateException(msg);
                    } else {
                        result.add(factory);
                    }
                }
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }
}
