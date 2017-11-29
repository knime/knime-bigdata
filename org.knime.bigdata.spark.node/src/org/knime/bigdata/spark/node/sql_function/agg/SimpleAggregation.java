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

import org.knime.bigdata.spark.node.sql_function.NoSettingsFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionDialogFactory;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;

/**
 * Simple aggregation function without any settings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SimpleAggregation extends NoSettingsFunction implements SparkSQLAggregationFunction {
    private final Class<DataValue> m_compatibleClass;


    /** Simple Factory of no {@link SimpleAggregation} */
    public static class Factory implements SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> {
        private final SparkSQLAggregationFunction m_instance;

        /**
         * @param id the name of the function
         * @param description the description
         * @param compatibleClasses the compatible {@link DataValue} class or <code>null</code>
         */
        public Factory(final String id, final String description, final Class<? extends DataValue> compatibleClasses) {
            m_instance = new SimpleAggregation(id, description, compatibleClasses);
        }

        /**
         * Aggregation function that is compatible to every data type and returns input data type as result.
         * @param id the name of the function
         * @param description the description
         */
        public Factory(final String id, final String description) {
            m_instance = new SimpleAggregation(id, description);
        }

        @Override
        public String getId() { return m_instance.getId(); }

        @Override
        public SparkSQLAggregationFunction getInstance() { return m_instance; }
    }

    /**
     * @param name the name of the function
     * @param description the description
     * @param compatibleClass the compatible {@link DataValue} class or <code>null</code>
     *
     */
    @SuppressWarnings("unchecked")
    public SimpleAggregation(final String name, final String description, final Class<? extends DataValue> compatibleClass) {

        super(name, description);
        m_compatibleClass = (Class<DataValue>)compatibleClass;
    }

    /**
     * Aggregation function that is compatible to every data type and returns input data type as result.
     * @param name the name of the function
     * @param description the description
     */
    public SimpleAggregation(final String name, final String description) {
        this(name, description, null);
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return m_compatibleClass == null || type.isCompatible(m_compatibleClass);
    }

    @Override
    public String getColumnName() {
        return getLabel();
    }
}
