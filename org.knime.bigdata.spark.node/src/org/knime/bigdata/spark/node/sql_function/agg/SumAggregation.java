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
package org.knime.bigdata.spark.node.sql_function.agg;

import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;

import org.knime.bigdata.spark.node.sql_function.NoSettingsFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionDialogFactory;

/**
 * Returns the sum calculated from values of a group aggregation function.
 * @author Sascha Wolke, KNIME GmbH
 */
public class SumAggregation extends NoSettingsFunction implements SparkSQLAggregationFunction {

    /** Function factory */
    public static class Factory implements SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> {
        private final SparkSQLAggregationFunction m_instance;

        /**
         * @param id the name of the function
         * @param description the description
         */
        public Factory(final String id, final String description) {
            m_instance = new SumAggregation(id, description);
        }

        @Override
        public String getId() { return m_instance.getId(); }

        @Override
        public SparkSQLAggregationFunction getInstance() { return m_instance; }
    }

    /**
     * @param id the name of the function
     * @param description the description
     */
    public SumAggregation(final String id, final String description) {
        super(id, description);
    }

    @Override
    public DataType getType(final DataType originalType) {
        return originalType;
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return type.isCompatible(DoubleValue.class);
    }
}
