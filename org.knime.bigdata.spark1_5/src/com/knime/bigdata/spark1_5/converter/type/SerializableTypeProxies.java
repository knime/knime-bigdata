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
package com.knime.bigdata.spark1_5.converter.type;

import java.io.Serializable;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;

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
        public ArrayType getProxiedType() { return DataTypes.createArrayType(getSparkDataType()); }
    }

    static class BinaryTypeProxy implements SerializableProxyType<BinaryType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public BinaryType getProxiedType() { return (BinaryType) DataTypes.BinaryType; }
    }

    static class BooleanTypeProxy implements SerializableProxyType<BooleanType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public BooleanType getProxiedType() { return (BooleanType) DataTypes.BooleanType; }
    }

    static class ByteTypeProxy implements SerializableProxyType<ByteType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public ByteType getProxiedType() { return (ByteType) DataTypes.ByteType; }
    }

    static class StringTypeProxy implements SerializableProxyType<StringType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public StringType getProxiedType() { return (StringType) DataTypes.StringType; }
    }

    static class DateTypeProxy implements SerializableProxyType<DateType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public DateType getProxiedType() { return (DateType) DataTypes.DateType; }
    }

    static class DoubleTypeProxy implements SerializableProxyType<DoubleType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public DoubleType getProxiedType() { return (DoubleType) DataTypes.DoubleType; }
    }

    static class FloatTypeProxy implements SerializableProxyType<FloatType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public FloatType getProxiedType() { return (FloatType) DataTypes.FloatType; }
    }

    static class IntegerTypeProxy implements SerializableProxyType<IntegerType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public IntegerType getProxiedType() { return (IntegerType) DataTypes.IntegerType; }
    }

    static class LongTypeProxy implements SerializableProxyType<LongType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public LongType getProxiedType() { return (LongType) DataTypes.LongType; }
    }

    static class NullTypeProxy implements SerializableProxyType<NullType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public NullType getProxiedType() { return (NullType) DataTypes.NullType; }
    }

    static class ShortTypeProxy implements SerializableProxyType<ShortType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public ShortType getProxiedType() { return (ShortType) DataTypes.ShortType; }
    }

    static class TimestampTypeProxy implements SerializableProxyType<TimestampType> {

        private static final long serialVersionUID = 1L;

        /**
         * {@inheritDoc}
         */
        @Override
        public TimestampType getProxiedType() { return (TimestampType) DataTypes.TimestampType; }
    }

}
