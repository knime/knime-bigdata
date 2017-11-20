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
 *   Created on 27.04.2016 by koetter
 */
package com.knime.bigdata.spark.core.version;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <E> the actual elements the {@link SparkProvider} provides
 */
public class DefaultSparkProvider<E> implements SparkProviderWithElements<E> {

    private final CompatibilityChecker m_checker;
    private final Collection<E> m_elements = new LinkedList<>();

    /**
     * @param checker the {@link CompatibilityChecker}
     * @param elements of type T
     */
    protected DefaultSparkProvider(final CompatibilityChecker checker,
        final E[] elements) {
        this(checker, Arrays.asList(elements));
    }


    /**
     * @param checker the {@link CompatibilityChecker}
     * @param elements of type T
     */
    protected DefaultSparkProvider(final CompatibilityChecker checker,
        final Collection<E> elements) {
        m_checker = checker;
        m_elements.addAll(elements);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompatibilityChecker getChecker() {
        return m_checker;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return m_checker.supportSpark(sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return m_checker.getSupportedSparkVersions();
    }

    /**
     * Adds another element to the list this provider should provide.
     * @param element the element to add
     */
    public void add(final E element) {
        m_elements.add(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<E> get() {
        return m_elements;
    }

}