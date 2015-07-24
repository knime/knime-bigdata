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

import java.sql.Date;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.date.DateAndTimeValue;

import com.knime.bigdata.spark.util.converter.SparkTypeConverter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class DateAndTimeType implements SparkTypeConverter<DateAndTimeCell, Date> {

    private static final org.apache.spark.sql.api.java.DataType[] SPARK = new org.apache.spark.sql.api.java.DataType[] {
            org.apache.spark.sql.api.java.DataType.DateType};

    private static final DataType[] KNIME = new DataType[] {DateAndTimeCell.TYPE};

    private static volatile DateAndTimeType instance;

    private DateAndTimeType() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static DateAndTimeType getInstance() {
        if (instance == null) {
            synchronized (DateAndTimeType.class) {
                if (instance == null) {
                    instance = new DateAndTimeType();
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
    public Class<Date> getPrimitiveType() {
        return Date.class;
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
        if (sparkObject instanceof Date) {
            Date val = (Date) sparkObject;
            return new DateAndTimeCell(val.getTime(), true, true, true);
        }
        return DataType.getMissingCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date convert(final DataCell cell) {
        if (cell instanceof DateAndTimeValue) {
            return new Date(((DateAndTimeValue)cell).getUTCTimeInMillis());
        }
        return null;
    }

}
