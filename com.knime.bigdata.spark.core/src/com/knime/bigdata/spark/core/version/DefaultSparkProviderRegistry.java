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
 *   Created on 28.04.2016 by koetter
 */
package com.knime.bigdata.spark.core.version;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.knime.core.node.NodeLogger;
import org.knime.core.util.Pair;

import com.knime.bigdata.spark.core.exception.DuplicateElementException;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <I> The type of the ID of element E
 * @param <E> the element type
 * @param <P> the {@link DefaultSparkProvider} implementation
 */
public abstract class DefaultSparkProviderRegistry<I, E, P extends SparkProviderWithElements<E>>
extends SparkProviderRegistry<P> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DefaultSparkProviderRegistry.class);

    private final Map<Pair<SparkVersion, I>, E> m_elementsByKey = new LinkedHashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProvider(final P provider) {
        for(final SparkVersion sparkVersion : provider.getSupportedSparkVersions()) {
            final Collection<E> elements = provider.get();
            for (final E e : elements) {
                try {
                    addElement(sparkVersion, e);
                } catch (DuplicateElementException ex) {
                    LOGGER.warn(ex.getMessage(), ex);
                }
            }
        }
    }

    /**
     * @param sparkVersion
     * @param e
     * @throws DuplicateElementException if the element id is not unique
     */
    protected synchronized void addElement(final SparkVersion sparkVersion, final E e) throws DuplicateElementException {
        final I id = getElementID(e);
        final Pair<SparkVersion, I> providerKey = new Pair<>(sparkVersion, id);
        if (m_elementsByKey.containsKey(providerKey)) {
            throw new DuplicateElementException(id, String.format(
                "There are two providers for id  %s and Spark version %s. Ignoring %s.",
                id, sparkVersion.getLabel(), e.getClass().getName()));
        } else {
            m_elementsByKey.put(providerKey, e);
        }
    }

    /**
     * @param e the element E
     * @return the id of the element used in the map key together with the {@link SparkVersion}
     */
    protected abstract I getElementID(E e);

    /**
     * @param id the unique id
     * @param sparkVersion Spark version
     * @return the corresponding element or <code>null</code> if none exists
     */
    protected synchronized E get(final I id, final SparkVersion sparkVersion) {
        return m_elementsByKey.get(new Pair<>(sparkVersion, id));
    }
}
