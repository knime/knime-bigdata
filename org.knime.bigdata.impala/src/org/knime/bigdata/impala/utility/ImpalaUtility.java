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
 *   Created on 08.05.2014 by thor
 */
package org.knime.bigdata.impala.utility;

import java.io.IOException;

import org.knime.bigdata.commons.security.kerberos.KerberosConnectionFactory;
import org.knime.bigdata.impala.aggregation.NDVDBAggregationFunction;
import org.knime.core.data.StringValue;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.aggregation.function.AvgDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CountDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.GroupConcatDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MaxDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MinDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevSampDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.SumDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VariancePopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarianceSampDBAggregationFunction;
import org.knime.core.node.port.database.connection.DBConnectionFactory;
import org.knime.core.node.port.database.connection.DBDriverFactory;
import org.knime.core.node.port.database.tablecreator.DBTableCreator;

/**
 * Database utility for Impala.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class ImpalaUtility extends DatabaseUtility {
    /**The unique database identifier.*/
    public static final String DATABASE_IDENTIFIER = "impala";

    /** The driver's class name. */
    public static final String DRIVER = ImpalaDriverFactory.DRIVER;

    /**
     * Constructor.
     * @throws IOException
     */
    public ImpalaUtility() throws IOException {
        super(DATABASE_IDENTIFIER, new ImpalaStatementManipulator(), new ImpalaDriverFactory(),
            new AvgDistinctDBAggregationFunction.Factory(), new CountDistinctDBAggregationFunction.Factory(),
            new GroupConcatDBAggregationFunction.Factory(StringValue.class), new MaxDBAggregationFunction.Factory(),
            new MinDBAggregationFunction.Factory(), new NDVDBAggregationFunction.Factory(),
            new StdDevSampDBAggregationFunction.Factory(), new StdDevPopDBAggregationFunction.Factory(),
            new SumDistinctDBAggregationFunction.Factory(), new VarianceSampDBAggregationFunction.Factory(),
            new VariancePopDBAggregationFunction.Factory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DBConnectionFactory createConnectionFactory(final DBDriverFactory df) {
        return new KerberosConnectionFactory(df);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public DBTableCreator getTableCreator(final String schema, final String tableName, final boolean isTempTable) {
        return new ImpalaTableCreator(getStatementManipulator(), schema, tableName, isTempTable);
    }

}
