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
 *   Created on 30.05.2016 by koetter
 */
package com.knime.bigdata.spark.core.types.converter.spark;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateArrayDataType;

import scala.collection.mutable.WrappedArray;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <T> The Spark data type this converter converts to
 */
@SparkClass
public class IntermediateArrayToSparkConverter<T> extends DefaultIntermediateToSparkConverter<T> {

    private static final long serialVersionUID = 1L;
    private final IntermediateToSparkConverter<?> m_elementConverter;

    /**
     * @param elementConverter the {@link IntermediateToSparkConverter} for the array element type
     * @param sparkTypeProxy {@link SerializableProxyType}
     */
    public IntermediateArrayToSparkConverter(final IntermediateToSparkConverter<?> elementConverter,
        final SerializableProxyType<T> sparkTypeProxy) {
        super(new IntermediateArrayDataType(elementConverter.getIntermediateDataType()), sparkTypeProxy);
        m_elementConverter = elementConverter;
    }

    /**
     * @param elementConverter the {@link IntermediateToSparkConverter} for the array element type
     * @param sparkType the actual Spark data type which might be Spark version dependent.
     * @throws ClassCastException if T is not {@link Serializable}
     */
    public IntermediateArrayToSparkConverter(final IntermediateToSparkConverter<?> elementConverter,
        final T sparkType) {
        super(new IntermediateArrayDataType(elementConverter.getIntermediateDataType()), sparkType);
        m_elementConverter = elementConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable convert(final Object sparkObject) {
        if (sparkObject instanceof Object[]) {
            final Object[] objectArray = (Object[])sparkObject;
            final Serializable[] result = new Serializable[objectArray.length];
            for (int i = 0, length = objectArray.length; i < length; i++) {
                result[i] = m_elementConverter.convert(objectArray[i]);
            }
            return result;

        } else if (sparkObject instanceof WrappedArray) {
            return convert(((WrappedArray<?>) sparkObject).array());

        } else {
            return super.convert(sparkObject);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object convert(final Serializable intermediateTypeValue) {
        if (intermediateTypeValue instanceof Object[]) {
            final Object[] objectArray = (Object[])intermediateTypeValue;
            final Serializable[] result = new Serializable[objectArray.length];
            for (int i = 0, length = objectArray.length; i < length; i++) {
                result[i] = m_elementConverter.convert(objectArray[i]);
            }

          return WrappedArray.make(result);

        } else {
            return super.convert(intermediateTypeValue);
        }
    }

}
