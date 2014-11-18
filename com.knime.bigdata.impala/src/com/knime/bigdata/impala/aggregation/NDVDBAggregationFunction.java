/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.impala.aggregation;

import org.knime.core.data.DataValue;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.aggregation.DBAggregationFunctionFactory;
import org.knime.core.node.port.database.aggregation.SimpleDBAggregationFunction;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public final class NDVDBAggregationFunction extends SimpleDBAggregationFunction {

    private static volatile NDVDBAggregationFunction instance;

    private static final String ID = "NDV";
    /**Factory for the parent class.*/
    public static final class Factory implements DBAggregationFunctionFactory {
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
            if (instance == null) {
                synchronized (NDVDBAggregationFunction.class) {
                    if (instance == null) {
                        instance = new NDVDBAggregationFunction();
                    }
                }
            }
            return instance;
        }
    }

    private NDVDBAggregationFunction() {
        super(ID, "An aggregate function that returns an approximate value similar to the result of COUNT(DISTINCT col), "
                + "the 'number of distinct values'. It is much faster than the combination of COUNT and DISTINCT, "
                + "and uses a constant amount of memory and thus is less memory-intensive for columns with high "
                + "cardinality.", LongCell.TYPE, DataValue.class);
    }
}
