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
package com.knime.bigdata.hive.utility;

import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.aggregation.AverageDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.CountDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.MaxDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.MinDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.SumDBAggregationFunction;

import com.knime.bigdata.hive.aggregation.CollectSetDBAggregationFunction;
import com.knime.bigdata.hive.aggregation.StdDevPopDBAggregationFunction;
import com.knime.bigdata.hive.aggregation.StdDevSampDBAggregationFunction;
import com.knime.bigdata.hive.aggregation.VarPopDBAggregationFunction;
import com.knime.bigdata.hive.aggregation.VarSampDBAggregationFunction;

/**
 * Database utility for Hive.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
public class HiveUtility extends DatabaseUtility {
    /**The unique database identifier.*/
    static final String DATABASE_IDENTIFIER = "hive2";

    /**
     * Constructor.
     */
    public HiveUtility() {
        super(DATABASE_IDENTIFIER, new HiveStatementManipulator(), CountDBAggregationFunction.getInstance(),
            SumDBAggregationFunction.getInstance(), AverageDBAggregationFunction.getInstance(),
            MinDBAggregationFunction.getInstance(), MaxDBAggregationFunction.getInstance(),
            VarPopDBAggregationFunction.getInstance(), VarSampDBAggregationFunction.getInstance(),
            StdDevPopDBAggregationFunction.getInstance(), StdDevSampDBAggregationFunction.getInstance(),
            CollectSetDBAggregationFunction.getInstance());
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
