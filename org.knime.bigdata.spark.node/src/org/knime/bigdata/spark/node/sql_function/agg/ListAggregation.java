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

/**
 * Aggregates all values of a given column into a list.
 * @author Sascha Wolke, KNIME GmbH
 */
public class ListAggregation extends NoSettingsFunction implements SparkSQLAggregationFunction {
    private final static String ID = "collect_list";
    private final static String DESC = "Collects and returns a list of non-unique elements";

    /** Function factory */
    public static class Factory implements SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> {
        @Override
        public String getId() { return ID; }

        @Override
        public SparkSQLAggregationFunction getInstance() { return new ListAggregation(); }
    }

    /** Default constructor */
    public ListAggregation() {
        super(ID, DESC);
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return true;
    }
}
