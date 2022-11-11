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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.knime.bigdata.hive.utility.KerberosConnectionFactory;
import org.knime.bigdata.impala.aggregation.NDVDBAggregationFunction;
import org.knime.core.data.StringValue;
import org.knime.core.node.NodeLogger;
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
@Deprecated
public class ImpalaUtility extends DatabaseUtility {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ImpalaUtility.class);
    /**The unique database identifier.*/
    public static final String DATABASE_IDENTIFIER = "impala";

    /** The driver's class name. */
    public static final String DRIVER = ImpalaDriverFactory.DRIVER;

    private static final String VALIDATION_QUERY;

    static {
         VALIDATION_QUERY = System.getProperty("knime.bigdata.impala.validation.query");
         if (VALIDATION_QUERY != null && !VALIDATION_QUERY.isEmpty()) {
             LOGGER.info("Validate Impala connection using query: " + VALIDATION_QUERY);
         }
    }

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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(final Connection conn) throws SQLException {
        if (VALIDATION_QUERY != null && !VALIDATION_QUERY.isEmpty()) {
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery(VALIDATION_QUERY)) {
                    rs.next();
                    return true;
                }
            } catch (SQLException e) {
                return false;
            }
        }
        return super.isValid(conn);
    }

}
