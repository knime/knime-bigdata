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
import org.knime.core.data.StringValue;
import org.knime.core.data.def.StringCell;

import com.knime.bigdata.spark.util.converter.SparkTypeConverter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class StringType implements SparkTypeConverter<StringCell, String> {

    private static final org.apache.spark.sql.api.java.DataType[] SPARK = new org.apache.spark.sql.api.java.DataType[] {
            org.apache.spark.sql.api.java.DataType.StringType};

    private static final DataType[] KNIME = new DataType[] {StringCell.TYPE};

    private static volatile StringType instance;

    private StringType() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static StringType getInstance() {
        if (instance == null) {
            synchronized (StringType.class) {
                if (instance == null) {
                    instance = new StringType();
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
    public Class<String> getPrimitiveType() {
        return String.class;
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
        if (sparkObject != null) {
            return new StringCell(sparkObject.toString());
        }
        return DataType.getMissingCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String convert(final DataCell cell) {
        if (cell instanceof StringValue) {
            return ((StringValue)cell).getStringValue();
        }
        return null;
    }

}
