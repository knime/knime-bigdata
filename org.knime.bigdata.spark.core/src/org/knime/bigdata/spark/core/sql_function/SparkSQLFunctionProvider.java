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

import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.DefaultSparkProvider;

/**
 * Provides spark function and a factory name to use them in spark jobs.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public abstract class SparkSQLFunctionProvider extends DefaultSparkProvider<String> {
    private final Class<? extends SparkSQLFunctionFactory<?>> m_factoryClass;

    /**
     * @param checker supported spark version checker
     * @param factoryClass spark side factory class
     * @param functions supported aggregation function identifiers
     */
    public SparkSQLFunctionProvider(final CompatibilityChecker checker,
        final Class<? extends SparkSQLFunctionFactory<?>> factoryClass, final String functions[]) {

        super(checker, functions);
        m_factoryClass = factoryClass;
    }

    /** @return class name of function factory */
    public String getFunctionFactoryClassName() {
        return m_factoryClass.getCanonicalName();
    }
}
