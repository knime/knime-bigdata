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
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataType;

/**
 * Returns the sum calculated from values of a group aggregation function. This version is only compatible to boolean
 * values. It has an ID and a label to present it in the dialog like the normal {@link SumAggregation} function for
 * other numerics.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SumBooleanAggregation extends NoSettingsFunction implements SparkSQLAggregationFunction {
    private final String m_label;

    /** Function factory */
    public static class Factory implements SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> {
        private final SparkSQLAggregationFunction m_instance;

        /**
         * @param id the name of the function
         * @param label the label of the function shown in the dialog
         * @param description the description
         */
        public Factory(final String id, final String label, final String description) {
            m_instance = new SumBooleanAggregation(id, label, description);
        }

        @Override
        public String getId() { return m_instance.getId(); }

        @Override
        public SparkSQLAggregationFunction getInstance() { return m_instance; }
    }

    /**
     * @param id the name of the function
     * @param label the name of the function shown in the dialog
     * @param description the description
     */
    public SumBooleanAggregation(final String id, final String label, final String description) {
        super(id, description);
        m_label = label;
    }

    @Override
    public String getLabel() {
        return m_label;
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return type.isCompatible(BooleanValue.class);
    }
}
