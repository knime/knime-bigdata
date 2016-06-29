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
package com.knime.bigdata.impala.utility;

import java.util.Collection;

import org.knime.core.data.StringValue;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
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

import com.knime.bigdata.commons.security.kerberos.KerberosConnectionFactory;
import com.knime.bigdata.impala.aggregation.NDVDBAggregationFunction;
import com.knime.licenses.LicenseChecker;
import com.knime.licenses.LicenseException;
import com.knime.licenses.LicenseFeatures;
import com.knime.licenses.LicenseUtil;

/**
 * Database utility for Impala.
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class ImpalaUtility extends DatabaseUtility {
    /**The unique database identifier.*/
    public static final String DATABASE_IDENTIFIER = "impala";

    /** The driver's class name. */
    public static final String DRIVER = ImpalaDriverFactory.DRIVER;

    /**
     * {@link LicenseChecker} to use.
     */
    public static final LicenseChecker LICENSE_CHECKER = new LicenseUtil(LicenseFeatures.ImpalaConnector);

    /**
     * Constructor.
     */
    public ImpalaUtility() {
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
    public StatementManipulator getStatementManipulator() {
        try {
            LICENSE_CHECKER.checkLicense();
        } catch (LicenseException e) {
            new RuntimeException(e.getMessage(), e);
        }
        return super.getStatementManipulator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<DBAggregationFunction> getAggregationFunctions() {
        try {
            LICENSE_CHECKER.checkLicense();
        } catch (LicenseException e) {
            new RuntimeException(e.getMessage(), e);
        }
        return super.getAggregationFunctions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBAggregationFunction getAggregationFunction(final String id) {
        try {
            LICENSE_CHECKER.checkLicense();
        } catch (LicenseException e) {
            new RuntimeException(e.getMessage(), e);
        }
        return super.getAggregationFunction(id);
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
