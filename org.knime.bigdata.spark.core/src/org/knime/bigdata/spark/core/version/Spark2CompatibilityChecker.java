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
 *   Created on Jun 3, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.version;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Compatibility checker that checks for Spark 2.0 and higher.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class Spark2CompatibilityChecker implements CompatibilityChecker {

    /**
     * Singleton instance.
     */
    public static final CompatibilityChecker INSTANCE = new Spark2CompatibilityChecker();

    private Spark2CompatibilityChecker() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return sparkVersion.getMajor() >= 2;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return Arrays.asList(SparkVersion.ALL).stream().filter(new Predicate<SparkVersion>() {
            @Override
            public boolean test(final SparkVersion t) {
                return t.getMajor() >= 2;
            }
        }).collect(Collectors.<SparkVersion> toSet());
    }

}
