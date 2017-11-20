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
 *   Created on 29.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.version;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Compatibility checker that checks for compatibility with one or multiple Spark versions.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class FixedVersionCompatibilityChecker implements CompatibilityChecker {

    private Set<SparkVersion> m_versions;

    /**
     * Constructor.
     *
     * @param version The supported Spark versions
     *
     */
    public FixedVersionCompatibilityChecker(final SparkVersion... version) {
        m_versions = Collections.unmodifiableSet(new HashSet<SparkVersion>(Arrays.asList(version)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return m_versions.contains(sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return m_versions;
    }
}
