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
 *   Created on 22.01.2018 by Oleg Yasnev
 */
package org.knime.bigdata.spark.core.context;

import java.util.LinkedHashMap;
import java.util.Map;

import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkProviderRegistry;

/**
 * Registry class for the Eclipse extension point where {@link SparkContextProvider}s can register themselves.
 *
 * @author Oleg Yasnev, KNIME GmbH
 */
public class SparkContextProviderRegistry extends SparkProviderRegistry<SparkContextProvider<?>> {

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "org.knime.bigdata.spark.core.SparkContextProvider";

    private static SparkContextProviderRegistry instance;

    @SuppressWarnings("rawtypes")
    private final Map<String, SparkContextProvider> m_providers = new LinkedHashMap<>();

    private SparkContextProviderRegistry() {
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public synchronized static SparkContextProviderRegistry getInstance() {
        if (instance == null) {
            instance = new SparkContextProviderRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    /**
     * Looks up the {@link SparkContextProvider} for the given URL scheme (see {@link SparkContextID}).
     *
     * @param scheme The URL scheme as a String.
     * @return A matching provider for the URL scheme, or null, if none was found.
     */
    @SuppressWarnings("unchecked")
    public static <T extends SparkContextConfig> SparkContextProvider<T> getSparkContextProvider(final String scheme) {
        return getInstance().m_providers.get(scheme);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProvider(final SparkContextProvider<?> provider) throws IllegalArgumentException {
        String scheme = provider.getSupportedScheme();
        if (m_providers.containsKey(scheme)) {
            throw new IllegalArgumentException(
                String.format("Could not register provider %s for the scheme %s ",
                    provider.getClass().getName(), scheme));
        }
        m_providers.put(scheme, provider);
    }
}
