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
package org.knime.bigdata.spark2_4.base;

import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionProvider;
import org.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Provides spark function and a factory name to use them in spark jobs.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class Spark_2_4_SQLFunctionFactoryProvider extends SparkSQLFunctionProvider {

    /** Default constructor */
    public Spark_2_4_SQLFunctionFactoryProvider() {
        super(new FixedVersionCompatibilityChecker(SparkVersion.V_2_4),
            new Spark_2_4_SQLFunctionFactory(),
            Spark_2_4_SQLFunctionFactory.SUPPORTED_FUNCTIONS);
    }
}
