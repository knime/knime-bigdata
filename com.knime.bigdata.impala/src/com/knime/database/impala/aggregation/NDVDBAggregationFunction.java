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
package com.knime.database.impala.aggregation;

import org.knime.core.data.DataType;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public final class NDVDBAggregationFunction implements DBAggregationFunction {

    private static volatile NDVDBAggregationFunction instance;

    private NDVDBAggregationFunction() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static NDVDBAggregationFunction getInstance() {
        if (instance == null) {
            synchronized (NDVDBAggregationFunction.class) {
                if (instance == null) {
                    instance = new NDVDBAggregationFunction();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "NDV";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getType(final DataType originalType) {
        return LongCell.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "An aggregate function that returns an approximate value similar to the result of COUNT(DISTINCT col), "
                + "the 'number of distinct values'. It is much faster than the combination of COUNT and DISTINCT, "
                + "and uses a constant amount of memory and thus is less memory-intensive for columns with high "
                + "cardinality.";
    }
}
