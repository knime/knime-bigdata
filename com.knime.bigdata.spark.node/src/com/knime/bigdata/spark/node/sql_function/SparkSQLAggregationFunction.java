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
package com.knime.bigdata.spark.node.sql_function;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.port.database.aggregation.AggregationFunction;

/**
 * Spark SQL aggregation function.
 * @author Sascha Wolke, KNIME GmbH
 */
public interface SparkSQLAggregationFunction extends SparkSQLFunction, AggregationFunction  {

    /**
     * Return type of this function.
     * @param originalType Type of the column that will be aggregated
     * @return The type of the aggregated column or null if unknown
     */
    public DataType getType(final DataType originalType);

    /**
     * Set column spec to aggregate on. This method might never be called if this function does not get associated with
     * a specific data column spec.
     *
     * @param colSpec column to aggregate on
     */
    public void setColumnSpec(final DataColumnSpec colSpec);
}
