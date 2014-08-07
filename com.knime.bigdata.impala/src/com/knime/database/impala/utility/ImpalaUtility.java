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
 *   Created on 08.05.2014 by thor
 */
package com.knime.database.impala.utility;

import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.AverageDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.CountDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.MaxDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.MinDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.SumDBAggregationFunction;

import com.knime.database.impala.aggregation.GroupConcatDBAggregationFunction;
import com.knime.database.impala.aggregation.NDVDBAggregationFunction;
import com.knime.database.impala.aggregation.StdDevPopDBAggregationFunction;
import com.knime.database.impala.aggregation.StdDevSampDBAggregationFunction;
import com.knime.database.impala.aggregation.VariancePopDBAggregationFunction;
import com.knime.database.impala.aggregation.VarianceSampDBAggregationFunction;

/**
 * Database utility for Impala.
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class ImpalaUtility extends DatabaseUtility {
    /**The unique database identifier.*/
    public static final String DATABASE_IDENTIFIER = "impala";

    private static final StatementManipulator MANIPULATOR = new ImpalaStatementManipulator();

    /**
     *
     */
    public ImpalaUtility() {
        super(DATABASE_IDENTIFIER, MANIPULATOR,
            AverageDBAggregationFunction.getInstance(), CountDBAggregationFunction.getInstance(),
            MaxDBAggregationFunction.getInstance(), MinDBAggregationFunction.getInstance(),
            NDVDBAggregationFunction.getInstance(), StdDevSampDBAggregationFunction.getInstance(),
            StdDevPopDBAggregationFunction.getInstance(), SumDBAggregationFunction.getInstance(),
            VarianceSampDBAggregationFunction.getInstance(), VariancePopDBAggregationFunction.getInstance(),
            GroupConcatDBAggregationFunction.getInstance());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsDelete() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsUpdate() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsInsert() {
        return false;
    }
}
