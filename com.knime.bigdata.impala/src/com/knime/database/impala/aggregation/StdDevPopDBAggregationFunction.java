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
import org.knime.core.data.def.StringCell;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public final class StdDevPopDBAggregationFunction implements DBAggregationFunction {

    private static volatile StdDevPopDBAggregationFunction instance;

    private StdDevPopDBAggregationFunction() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static StdDevPopDBAggregationFunction getInstance() {
        if (instance == null) {
            synchronized (StdDevPopDBAggregationFunction.class) {
                if (instance == null) {
                    instance = new StdDevPopDBAggregationFunction();
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
        return "STDDEV_POP";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getType(final DataType originalType) {
        return StringCell.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "The function computes the population standard deviation, respectively, of the input values."
                + "The function evaluates all input rows matched by the query and is scaled by 1/N";
    }

}
