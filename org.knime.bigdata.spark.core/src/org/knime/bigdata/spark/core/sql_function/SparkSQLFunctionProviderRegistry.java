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
package org.knime.bigdata.spark.core.sql_function;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.version.SparkProviderRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.NodeLogger;

/**
 * Registry with spark functions providers.
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkSQLFunctionProviderRegistry extends SparkProviderRegistry<SparkSQLFunctionProvider> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkSQLFunctionProviderRegistry.class);

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "org.knime.bigdata.spark.core.SparkSQLFunctionProvider";

    private static SparkSQLFunctionProviderRegistry instance;

    /** function name per factory per spark version */
    private static final Map<SparkVersion, HashMap<String, String>> m_functionsPerVersion = new LinkedHashMap<>();

    /** factory provider per factory name per spark version */
    private static final Map<SparkVersion, HashMap<String, SparkSQLFunctionProvider>> m_factoryProvider = new LinkedHashMap<>();

    private SparkSQLFunctionProviderRegistry() {}

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static SparkSQLFunctionProviderRegistry getInstance() {
        if (instance == null) {
            instance = new SparkSQLFunctionProviderRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    @Override
    protected void addProvider(final SparkSQLFunctionProvider provider) {
        final String factoryClassName = provider.getFunctionFactoryClassName();
        final Set<SparkVersion> sparkVersions = provider.getSupportedSparkVersions();

        for (SparkVersion sparkVersion : sparkVersions) {
            if (!m_functionsPerVersion.containsKey(sparkVersion)) {
                m_functionsPerVersion.put(sparkVersion, new HashMap<String, String>());
                m_factoryProvider.put(sparkVersion, new HashMap<String, SparkSQLFunctionProvider>());
            }

            final HashMap<String, String> functionsPerVersion = m_functionsPerVersion.get(sparkVersion);
            for (String function : provider.get()) {
                if (functionsPerVersion.containsKey(function)) {
                    String msg = "Duplicated Spark Function Provider detected on function " + function
                            + " (Factories: " + functionsPerVersion.get(function) + " vs. " + factoryClassName + ")";
                    LOGGER.warn(msg);
                    throw new IllegalStateException(msg);
                }

                functionsPerVersion.put(function, factoryClassName);
            }

            final HashMap<String, SparkSQLFunctionProvider> factoryProvider = m_factoryProvider.get(sparkVersion);
            if (factoryProvider.containsKey(factoryClassName)) {
                String msg = "Duplicated Spark Function Factory detected (Spark version: " + sparkVersion
                    + ", factory: " + factoryClassName + ")";
                LOGGER.warn(msg);
                throw new IllegalStateException(msg);
            }
            factoryProvider.put(factoryClassName, provider);
        }
    }

    /**
     * @param sparkVersion version to search for
     * @return map with supported functions (key) and factories (value) or empty map
     */
    public Map<String, String> getSupportedFunctions(final SparkVersion sparkVersion) {
        if (m_functionsPerVersion.containsKey(sparkVersion)) {
            return m_functionsPerVersion.get(sparkVersion);
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Returns result field of a given input table spec and job config.
     * @param sparkVersion spark version to work on
     * @param inputSpec input table spec containing referenced columns from job input
     * @param input function input
     * @return result field of function or null if unable to compute
     */
    public IntermediateField getFunctionResultField(final SparkVersion sparkVersion, final IntermediateSpec inputSpec,
        final SparkSQLFunctionJobInput input) {

        return m_factoryProvider.get(sparkVersion).get(input.getFactoryName()).getFunctionResultField(sparkVersion, inputSpec, input);
    }
}
