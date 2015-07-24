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
 *   Created on 05.07.2015 by koetter
 */
package com.knime.bigdata.spark.util.types;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DoubleCell;

import com.knime.bigdata.spark.util.converter.SparkTypeConverter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class DoubleType implements SparkTypeConverter<DoubleCell, Double> {

    private static final org.apache.spark.sql.api.java.DataType[] SPARK = new org.apache.spark.sql.api.java.DataType[] {
        org.apache.spark.sql.api.java.DataType.DoubleType, org.apache.spark.sql.api.java.DataType.FloatType};

    private static final DataType[] KNIME = new DataType[] {DoubleCell.TYPE};

    private static volatile DoubleType instance;

    private DoubleType() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static DoubleType getInstance() {
        if (instance == null) {
            synchronized (DoubleType.class) {
                if (instance == null) {
                    instance = new DoubleType();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public org.apache.spark.sql.api.java.DataType getSparkSqlType() {
        return getSparkSqlTypes()[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getKNIMEType() {
        return getKNIMETypes()[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<Double> getPrimitiveType() {
        return Double.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public org.apache.spark.sql.api.java.DataType[] getSparkSqlTypes() {
        return SPARK;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType[] getKNIMETypes() {
        return KNIME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataCell convert(final Object sparkObject) {
        if (sparkObject instanceof Double) {
            Double val = (Double) sparkObject;
            return new DoubleCell(val);
        }
        return DataType.getMissingCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double convert(final DataCell cell) {
        if (cell instanceof DoubleValue) {
            return ((DoubleValue)cell).getDoubleValue();
        }
        return null;
    }

}
