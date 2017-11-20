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

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Factory of spark functions.
 *
 * @param <T> Type of aggregation column
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public interface SparkSQLFunctionFactory<T> {

    /**
     * @param input function configuration
     * @return spark column expression with requested function
     */
    public T getFunctionColumn(final SparkSQLFunctionJobInput input);
}
