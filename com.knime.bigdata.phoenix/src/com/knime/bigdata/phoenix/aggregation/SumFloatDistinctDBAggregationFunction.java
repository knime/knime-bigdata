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
package com.knime.bigdata.phoenix.aggregation;

import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.aggregation.DBAggregationFunctionFactory;
import org.knime.core.node.port.database.aggregation.function.AbstractDistinctDBAggregationFunction;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public final class SumFloatDistinctDBAggregationFunction extends AbstractDistinctDBAggregationFunction {

    private static final String LABEL = "SUM_FLOAT";
    /**Factory for parent class.*/
    public static final class Factory implements DBAggregationFunctionFactory {
        /**
         * {@inheritDoc}
         */
        @Override
        public String getId() {
            return LABEL + AbstractDistinctDBAggregationFunction.LABEL_POSTIX;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DBAggregationFunction createInstance() {
            return new SumFloatDistinctDBAggregationFunction();
        }
    }

    /**
     * Constructor.
     */
    public SumFloatDistinctDBAggregationFunction() {
        this(false);
    }

    private SumFloatDistinctDBAggregationFunction(final boolean distinct) {
        super(distinct);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getType(final DataType originalType) {
        return DoubleCell.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSQLFragment(final StatementManipulator manipulator, final String tableName, final String columnName) {
        return  buildSQLFragment("SUM_FLOAT", manipulator, tableName, columnName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLabel() {
        return LABEL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCompatible(final DataType type) {
        return type.isCompatible(DoubleValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "Computes the sum of an expression over a group of rows. "
                + "It returns a DOUBLE PRECISION value for the expression, regardless of the expression type.";
    }
}
