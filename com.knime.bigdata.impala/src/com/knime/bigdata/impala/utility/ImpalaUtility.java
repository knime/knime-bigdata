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

import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.AverageDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CountDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.GroupConcatDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MaxDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MinDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevSampDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.SumDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VariancePopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarianceSampDBAggregationFunction;

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

    /**
     * {@link LicenseChecker} to use.
     */
    public static final LicenseChecker LICENSE_CHECKER = new LicenseUtil(LicenseFeatures.ImpalaConnector);

    private static final StatementManipulator MANIPULATOR = new ImpalaStatementManipulator();

    /**
     * Constructor.
     */
    public ImpalaUtility() {
        super(DATABASE_IDENTIFIER, MANIPULATOR,
            AverageDBAggregationFunction.getInstance(), CountDBAggregationFunction.getInstance(),
            MaxDBAggregationFunction.getInstance(), MinDBAggregationFunction.getInstance(),
            NDVDBAggregationFunction.getInstance(), StdDevSampDBAggregationFunction.getInstance(),
            StdDevPopDBAggregationFunction.getInstance(), SumDBAggregationFunction.getInstance(),
            VarianceSampDBAggregationFunction.getInstance(), VariancePopDBAggregationFunction.getInstance(),
            new GroupConcatDBAggregationFunction());
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
    public DBAggregationFunction getAggregationFunction(String id) {
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
