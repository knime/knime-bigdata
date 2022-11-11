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
package org.knime.bigdata.impala.aggregation;

import org.knime.core.data.DataValue;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.aggregation.DBAggregationFunctionFactory;
import org.knime.core.node.port.database.aggregation.SimpleDBAggregationFunction;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
@Deprecated
public final class NDVDBAggregationFunction extends SimpleDBAggregationFunction {
    private static final String ID = "NDV";

    /**Factory for the parent class.*/
    public static final class Factory implements DBAggregationFunctionFactory {
        private static final NDVDBAggregationFunction INSTANCE = new NDVDBAggregationFunction();

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

    private NDVDBAggregationFunction() {
        super(ID, "An aggregate function that returns an approximate value similar to the result of COUNT(DISTINCT col), "
                + "the 'number of distinct values'. It is much faster than the combination of COUNT and DISTINCT, "
                + "and uses a constant amount of memory and thus is less memory-intensive for columns with high "
                + "cardinality.", LongCell.TYPE, DataValue.class);
    }
}
