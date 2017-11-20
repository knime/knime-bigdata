/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
 *   Created on 24.04.2016 by koetter
 */
package com.knime.bigdata.spark.core.types.converter.spark;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;

/**
 * Default converter between {@link IntermediateDataType} and a Spark type given by the generic type parameter T.
 * Invoking {@link #convert(Object)} and {@link #convert(Serializable)} performs no operation and returns just the input
 * value.
 *
 * <p>
 * NOTE:As required by {@link IntermediateToSparkConverter}, instances of this class must be serializable, including the
 * members. Since an instance of the Spark data type class is a member, the serializability requirements applies to it,
 * which is a problem e.g. in Spark 1.2, where instances of Spark's own DataType class are not serializable. To work
 * around this limitation, you can use the
 * {@link #DefaultIntermediateToSparkConverter(IntermediateDataType, SerializableProxyType)} constructor that accepts a
 * {@link SerializableProxyType}.
 * </p>
 *
 *
 * @author Tobias Koetter, KNIME.com
 * @param <T> The Spark data type this converter converts to
 * @see SerializableProxyType
 */
@SparkClass
public class DefaultIntermediateToSparkConverter<T> implements IntermediateToSparkConverter<T> {

    private static final long serialVersionUID = 1L;

    private final IntermediateDataType m_intermediateDataType;

    private final Serializable m_sparkDataType;

    /**
     * Use this constructor when the type T is serializable.
     *
     * @param intermediateDataType the {@link IntermediateDataType}
     * @param sparkType the actual Spark data type which might be Spark version dependent.
     * @throws ClassCastException if T is not {@link Serializable}
     *
     */
    public DefaultIntermediateToSparkConverter(final IntermediateDataType intermediateDataType, final T sparkType) {
        m_intermediateDataType = intermediateDataType;
        m_sparkDataType = (Serializable) sparkType;
    }

    /**
     * Use this constructor when the type T is not serializable.
     *
     * @param intermediateDataType the {@link IntermediateDataType}
     * @param sparkTypeProxy a serializable proxy for the spark type
     */
    public DefaultIntermediateToSparkConverter(final IntermediateDataType intermediateDataType,
        final SerializableProxyType<T> sparkTypeProxy) {
        m_intermediateDataType = intermediateDataType;
        m_sparkDataType = sparkTypeProxy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntermediateDataType getIntermediateDataType() {
        return m_intermediateDataType;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T getSparkDataType() {
        if (m_sparkDataType instanceof SerializableProxyType) {
            return ((SerializableProxyType<T>)m_sparkDataType).getProxiedType();
        } else {
            return (T)m_sparkDataType;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable getSerializableDataType() {
        return m_sparkDataType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object convert(final Serializable intermediateTypeValue) {
        return intermediateTypeValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable convert(final Object sparkObject) {
        return (Serializable)sparkObject;
    }

    @SuppressWarnings("unchecked")
    private String getSparkTypeName() {
        if (m_sparkDataType instanceof SerializableProxyType) {
            return ((SerializableProxyType<T>)m_sparkDataType).getProxiedType().getClass().getCanonicalName();
        } else {
            return ((T)m_sparkDataType).getClass().getCanonicalName();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return String.format("%s <->  %s", getSparkTypeName(), m_intermediateDataType.getTypeId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return String.format("Converter between Spark's %s and intermediate type %s", getSparkTypeName(),
            m_intermediateDataType.getTypeId());
    }
}
