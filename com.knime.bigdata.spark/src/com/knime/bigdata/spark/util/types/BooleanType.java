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

import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;

import com.knime.bigdata.spark.util.converter.SparkTypeConverter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class BooleanType implements SparkTypeConverter<BooleanCell, Boolean> {

    private static final org.apache.spark.sql.api.java.DataType[] SPARK = new org.apache.spark.sql.api.java.DataType[] {
            org.apache.spark.sql.api.java.DataType.BooleanType};

    private static final DataType[] KNIME = new DataType[] {BooleanCell.TYPE};

    private static volatile BooleanType instance;

    private BooleanType() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static BooleanType getInstance() {
        if (instance == null) {
            synchronized (BooleanType.class) {
                if (instance == null) {
                    instance = new BooleanType();
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
    public Class<Boolean> getPrimitiveType() {
        return Boolean.class;
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
        if (sparkObject instanceof Boolean) {
            Boolean b = (Boolean) sparkObject;
            return BooleanCell.get(b);
        }
        return DataType.getMissingCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean convert(final DataCell cell) {
        if (cell instanceof BooleanValue) {
            return Boolean.valueOf(((BooleanValue)cell).getBooleanValue());
        }
        return null;
    }

}
