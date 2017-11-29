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

import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.DefaultSparkProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Provides spark function and a factory name to use them in spark jobs.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public abstract class SparkSQLFunctionProvider extends DefaultSparkProvider<String> {
    private final SparkSQLFunctionFactory<?> m_factory;

    /**
     * @param checker supported spark version checker
     * @param factory spark side factory
     * @param functions supported aggregation function identifiers
     */
    public SparkSQLFunctionProvider(final CompatibilityChecker checker,
        final SparkSQLFunctionFactory<?> factory, final String functions[]) {

        super(checker, functions);
        m_factory = factory;
    }

    /** @return class name of function factory */
    public String getFunctionFactoryClassName() {
        return m_factory.getClass().getCanonicalName();
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

        return m_factory.getFunctionResultField(sparkVersion, inputSpec, input);
    }
}
