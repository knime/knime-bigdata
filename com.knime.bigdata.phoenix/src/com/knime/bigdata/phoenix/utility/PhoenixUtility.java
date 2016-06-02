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
package com.knime.bigdata.phoenix.utility;

import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.function.AvgDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CorrDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CountDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CovarPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CovarSampDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MaxDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MinDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrAvgXDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrAvgYDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrCountDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrInterceptDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrR2DBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrSXXDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrSXYDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrSYYDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.RegrSlopeDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevSampDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.SumDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarSampDBAggregationFunction;
import org.knime.core.node.port.database.reader.DBReader;
import org.knime.core.node.port.database.writer.DBWriter;


/**
 * Database utility for Impala.
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class PhoenixUtility extends DatabaseUtility {
    /**The unique database identifier.*/
    public static final String DATABASE_IDENTIFIER = "phoenix";

    /** The driver's class name. */
    public static final String DRIVER = PhoenixDriverFactory.DRIVER;

    private static final StatementManipulator MANIPULATOR = new PhoenixStatementManipulator();

    /**
     * Constructor.
     */
    public PhoenixUtility() {
        super(DATABASE_IDENTIFIER, MANIPULATOR, new PhoenixDriverFactory(),
            new AvgDistinctDBAggregationFunction.Factory(), new CorrDBAggregationFunction.Factory(),
            new  CountDistinctDBAggregationFunction.Factory(), new CovarPopDBAggregationFunction.Factory(),
            new CovarSampDBAggregationFunction.Factory(), new MaxDBAggregationFunction.Factory(),
            new MinDBAggregationFunction.Factory(), new RegrAvgXDBAggregationFunction.Factory(),
            new RegrAvgYDBAggregationFunction.Factory(), new RegrCountDBAggregationFunction.Factory(),
            new RegrInterceptDBAggregationFunction.Factory(), new RegrR2DBAggregationFunction.Factory(),
            new RegrSlopeDBAggregationFunction.Factory(), new RegrSXXDBAggregationFunction.Factory(),
            new RegrSXYDBAggregationFunction.Factory(), new RegrSYYDBAggregationFunction.Factory(),
            new StdDevPopDBAggregationFunction.Factory(), new StdDevSampDBAggregationFunction.Factory(),
            new SumDistinctDBAggregationFunction.Factory(), new VarPopDBAggregationFunction.Factory(),
            new VarSampDBAggregationFunction.Factory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsDelete() {
        return true;
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
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBReader getReader(final DatabaseQueryConnectionSettings querySettings) {
        return new PhoenixReader(querySettings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBWriter getWriter(final DatabaseConnectionSettings connSettings) {
        return new PhoenixWriter(connSettings);
    }
}
