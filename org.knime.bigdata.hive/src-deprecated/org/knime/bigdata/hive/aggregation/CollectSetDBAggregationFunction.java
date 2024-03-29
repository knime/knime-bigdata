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
 *   Created on 01.08.2014 by koetter
 */
package org.knime.bigdata.hive.aggregation;

import org.knime.core.data.DataValue;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.aggregation.DBAggregationFunctionFactory;
import org.knime.core.node.port.database.aggregation.SimpleDBAggregationFunction;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
@Deprecated
public final class CollectSetDBAggregationFunction extends SimpleDBAggregationFunction {
    private static final String ID = "COLLECT_SET";

    /**Factory for the parent class.*/
    public static final class Factory implements DBAggregationFunctionFactory {
        private static final CollectSetDBAggregationFunction INSTANCE = new CollectSetDBAggregationFunction();

        /**
         * {@inheritDoc}
         */
        @Override
        public String getId() {
            return ID;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DBAggregationFunction createInstance() {
            return INSTANCE;
        }
    }

    private CollectSetDBAggregationFunction() {
        super(ID, "Returns a set of objects with duplicate elements eliminated.", StringCell.TYPE,
            DataValue.class);
    }
}
