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
 *   Created on May 13, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util.template;

import java.util.Set;

import com.knime.bigdata.spark.core.version.CompatibilityChecker;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public abstract class AbstractJavaSnippetTemplateRepositoryProvider implements JavaSnippetTemplateRepositoryProvider {

    private final CompatibilityChecker m_checker;

    /**
     * @param checker
     */
    public AbstractJavaSnippetTemplateRepositoryProvider(final CompatibilityChecker checker) {
        super();
        m_checker = checker;
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
}
