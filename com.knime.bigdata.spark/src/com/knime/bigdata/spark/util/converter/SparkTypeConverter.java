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
package com.knime.bigdata.spark.util.converter;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <K> KNIME {@link DataCell} implementation
 * @param <S> Spark {@link Object} implementation
 */
public interface SparkTypeConverter<K extends DataCell, S extends Object> {

    /**
     * @return the preferred Spark {@link org.apache.spark.sql.api.java.DataType} type this converter converts
     * the supported KNIME types to
     */
    public org.apache.spark.sql.api.java.DataType getSparkSqlType();

    /**
     * @return the Spark {@link org.apache.spark.sql.api.java.DataType}s that are supported by this converter
     */
    public org.apache.spark.sql.api.java.DataType[] getSparkSqlTypes();

    /**
     * @return the preferred KNIME {@link DataType} this converter converts the supported Spark types to
     */
    public DataType getKNIMEType();

    /**
     * @return the KNIME {@link DataType}s that are supported by this converter
     */
    public DataType[] getKNIMETypes();

    /**
     * @return the preferred primitive {@link Class} type
     */
    public Class<S> getPrimitiveType();

    /**
     * @param sparkObject the Spark data object to convert into a KNIME {@link DataCell}
     * @return corresponding KNIME {@link DataCell} or {@link DataType#getMissingCell()} if the object is
     * <code>null</code>
     */
    public DataCell convert(Object sparkObject);

    /**
     * @param cell the {@link DataCell} to convert
     * @return the corresponding Spark object
     */
    public S convert(DataCell cell);

}