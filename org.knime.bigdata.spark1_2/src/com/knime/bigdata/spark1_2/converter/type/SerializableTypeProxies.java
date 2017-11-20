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
 *   Created on Apr 29, 2016 by bjoern
 */
package com.knime.bigdata.spark1_2.converter.type;

import java.io.Serializable;

import org.apache.spark.sql.api.java.ArrayType;
import org.apache.spark.sql.api.java.BinaryType;
import org.apache.spark.sql.api.java.BooleanType;
import org.apache.spark.sql.api.java.ByteType;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.DateType;
import org.apache.spark.sql.api.java.DoubleType;
import org.apache.spark.sql.api.java.FloatType;
import org.apache.spark.sql.api.java.IntegerType;
import org.apache.spark.sql.api.java.LongType;
import org.apache.spark.sql.api.java.NullType;
import org.apache.spark.sql.api.java.ShortType;
import org.apache.spark.sql.api.java.StringType;
import org.apache.spark.sql.api.java.TimestampType;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.converter.spark.SerializableProxyType;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class SerializableTypeProxies {

    public static class ArrayTypeProxy implements SerializableProxyType<ArrayType> {

        private static final long serialVersionUID = 1L;

        private final Serializable m_elementType;

        /**
         * @param elementType {@link SerializableProxyType} of the element
         */
        public ArrayTypeProxy(final Serializable elementType) {
            m_elementType = elementType;
        }

        private DataType getSparkDataType() {
            if (m_elementType instanceof SerializableProxyType) {
                return ((SerializableProxyType<DataType>)m_elementType).getProxiedType();
            } else {
                return (DataType) m_elementType;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ArrayType getProxiedType() { return DataType.createArrayType(getSparkDataType()); }
    }

    static class BinaryTypeProxy implements SerializableProxyType<BinaryType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public BinaryType getProxiedType() { return DataType.BinaryType; }
    }

    static class BooleanTypeProxy implements SerializableProxyType<BooleanType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public BooleanType getProxiedType() { return DataType.BooleanType; }
    }

    static class ByteTypeProxy implements SerializableProxyType<ByteType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public ByteType getProxiedType() { return DataType.ByteType; }
    }

    static class StringTypeProxy implements SerializableProxyType<StringType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public StringType getProxiedType() { return DataType.StringType; }
    }

    static class DateTypeProxy implements SerializableProxyType<DateType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public DateType getProxiedType() { return DataType.DateType; }
    }

    static class DoubleTypeProxy implements SerializableProxyType<DoubleType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public DoubleType getProxiedType() { return DataType.DoubleType; }
    }

    static class FloatTypeProxy implements SerializableProxyType<FloatType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public FloatType getProxiedType() { return DataType.FloatType; }
    }

    static class IntegerTypeProxy implements SerializableProxyType<IntegerType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public IntegerType getProxiedType() { return DataType.IntegerType; }
    }

    static class LongTypeProxy implements SerializableProxyType<LongType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public LongType getProxiedType() { return DataType.LongType; }
    }

    static class NullTypeProxy implements SerializableProxyType<NullType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public NullType getProxiedType() { return DataType.NullType; }
    }

    static class ShortTypeProxy implements SerializableProxyType<ShortType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public ShortType getProxiedType() { return DataType.ShortType; }
    }

    static class TimestampTypeProxy implements SerializableProxyType<TimestampType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public TimestampType getProxiedType() { return DataType.TimestampType; }
    }

}
