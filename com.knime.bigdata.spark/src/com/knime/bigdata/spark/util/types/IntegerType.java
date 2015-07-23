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
import org.knime.core.data.IntValue;
import org.knime.core.data.def.IntCell;

import com.knime.bigdata.spark.util.converter.SparkTypeConverter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class IntegerType implements SparkTypeConverter<IntCell, Integer> {

    private static final org.apache.spark.sql.api.java.DataType[] SPARK = new org.apache.spark.sql.api.java.DataType[] {
        org.apache.spark.sql.api.java.DataType.IntegerType, org.apache.spark.sql.api.java.DataType.ByteType,
        org.apache.spark.sql.api.java.DataType.ShortType};

    private static final DataType[] KNIME = new DataType[] {IntCell.TYPE};

    private static volatile IntegerType instance;

    private IntegerType() {
        //avoid object creation
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
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static IntegerType getInstance() {
        if (instance == null) {
            synchronized (IntegerType.class) {
                if (instance == null) {
                    instance = new IntegerType();
                }
            }
        }
        return instance;
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
        if (sparkObject instanceof Integer) {
            Integer val = (Integer) sparkObject;
            return new IntCell(val);
        }
        return DataType.getMissingCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer convert(final DataCell cell) {
        if (cell instanceof IntValue) {
            return ((IntValue)cell).getIntValue();
        }
        return null;
    }

}
