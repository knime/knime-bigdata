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

import java.util.Set;

import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Provides spark aggregation function details that can be used in KNIME dialogs.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public abstract class SparkSQLFunctionDialogProvider implements SparkProvider {
    private final CompatibilityChecker m_checker;
    private final SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> m_aggFuncFactories[];

    /**
     * @param checker compatible spark version checker
     * @param aggFunctionFactories list of aggregation functions factories
     */
    @SafeVarargs
    public SparkSQLFunctionDialogProvider(final CompatibilityChecker checker,
        final SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction>... aggFunctionFactories) {

        m_checker = checker;
        m_aggFuncFactories = aggFunctionFactories;
    }

    @Override
    public CompatibilityChecker getChecker() {
        return m_checker;
    }

    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return m_checker.supportSpark(sparkVersion);
    }

    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return m_checker.getSupportedSparkVersions();
    }

    /** @return Supported SQL Aggregation Functions Factories */
    public SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction>[] getAggregationFunctionFactories() {
        return m_aggFuncFactories;
    }
}
