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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.AvgDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CorrDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CountDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CovarPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CovarSampDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MaxDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MinDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevSampDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.SumDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarSampDBAggregationFunction;
import org.knime.core.node.port.database.connection.DBConnectionFactory;
import org.knime.core.node.port.database.connection.DBDriverFactory;
import org.knime.core.node.port.database.tablecreator.DBTableCreator;

import com.knime.bigdata.commons.security.kerberos.KerberosConnectionFactory;
import com.knime.bigdata.hive.aggregation.CollectSetDBAggregationFunction;
import com.knime.bigdata.hive.aggregation.percentile.PercentileApproxDBAggregationFunction;
import com.knime.bigdata.hive.aggregation.percentile.PercentileDBAggregationFunction;
import com.knime.licenses.LicenseChecker;
import com.knime.licenses.LicenseException;
import com.knime.licenses.LicenseFeatures;
import com.knime.licenses.LicenseUtil;

/**
 * Database utility for Hive.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class HiveUtility extends DatabaseUtility {
    /**The unique database identifier.*/
    public static final String DATABASE_IDENTIFIER = "hive2";

    /**
     * The driver's class name.
     */
    public static final String DRIVER = HiveDriverFactory.DRIVER;


    /**
     * {@link LicenseChecker} to use.
     */
    public static final LicenseChecker LICENSE_CHECKER = new LicenseUtil(LicenseFeatures.HiveConnector);

    /**
     * Constructor.
     * @throws LicenseException
     */
    public HiveUtility() throws LicenseException {
        super(DATABASE_IDENTIFIER, new HiveStatementManipulator(), new HiveDriverFactory(),
            new CountDistinctDBAggregationFunction.Factory(), new SumDistinctDBAggregationFunction.Factory(),
            new AvgDistinctDBAggregationFunction.Factory(), new MinDBAggregationFunction.Factory(),
            new MaxDBAggregationFunction.Factory(), new VarPopDBAggregationFunction.Factory(),
            new VarSampDBAggregationFunction.Factory(), new StdDevPopDBAggregationFunction.Factory(),
            new StdDevSampDBAggregationFunction.Factory(), new CovarPopDBAggregationFunction.Factory(),
            new CovarSampDBAggregationFunction.Factory(), new CorrDBAggregationFunction.Factory(),
            new PercentileDBAggregationFunction.Factory(), new PercentileApproxDBAggregationFunction.Factory(),
            new CollectSetDBAggregationFunction.Factory());
            //CollectListDBAggregationFunction.getInstance() supported by Hive 0.13.0
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
    public StatementManipulator getStatementManipulator() {
        try {
            LICENSE_CHECKER.checkLicense();
        } catch (LicenseException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return super.getStatementManipulator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBAggregationFunction getAggregationFunction(final String id) {
        try {
            LICENSE_CHECKER.checkLicense();
        } catch (LicenseException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return super.getAggregationFunction(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<DBAggregationFunction> getAggregationFunctions() {
        try {
            LICENSE_CHECKER.checkLicense();
        } catch (LicenseException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return super.getAggregationFunctions();
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
        return new HiveTableCreator(getStatementManipulator(), schema, tableName, isTempTable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCase() {
        return true;
    }

    @Override
    public boolean supportsRandomSampling() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tableExists(final Connection conn, final String tableName) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null)) {
            return rs.next();
        }
    }
}
