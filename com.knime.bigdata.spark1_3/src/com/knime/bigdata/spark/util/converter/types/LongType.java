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
package com.knime.bigdata.spark.util.converter.types;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.LongValue;
import org.knime.core.data.def.LongCell;

import com.knime.bigdata.spark.util.converter.SparkTypeConverter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LongType implements SparkTypeConverter<LongCell, Long> {

    private static final org.apache.spark.sql.types.DataType[] SPARK = new org.apache.spark.sql.types.DataType[] {
        org.apache.spark.sql.types.DataTypes.LongType};

    private static final DataType[] KNIME = new DataType[] {LongCell.TYPE};

    private static volatile LongType instance;

    private LongType() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static LongType getInstance() {
        if (instance == null) {
            synchronized (LongType.class) {
                if (instance == null) {
                    instance = new LongType();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public org.apache.spark.sql.types.DataType getSparkSqlType() {
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
    public Class<Long> getPrimitiveType() {
        return Long.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public org.apache.spark.sql.types.DataType[] getSparkSqlTypes() {
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
        if (sparkObject instanceof Long) {
            Long val = (Long) sparkObject;
            return new LongCell(val);
        }
        return DataType.getMissingCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long convert(final DataCell cell) {
        if (cell instanceof LongValue) {
            return ((LongValue)cell).getLongValue();
        }
        return null;
    }
}
