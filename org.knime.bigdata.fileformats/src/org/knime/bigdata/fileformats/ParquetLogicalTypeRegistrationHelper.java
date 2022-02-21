/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */

package org.knime.bigdata.fileformats;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.JulianFields;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.knime.bigdata.fileformats.ParquetProducers.BinaryByteArrayProducer;
import org.knime.bigdata.fileformats.ParquetProducers.BooleanbooleanProducer;
import org.knime.bigdata.fileformats.ParquetProducers.BytesBytArrayProducer;
import org.knime.bigdata.fileformats.ParquetProducers.BytesInputStreamProducer;
import org.knime.bigdata.fileformats.ParquetProducers.DoubleDoubleProducer;
import org.knime.bigdata.fileformats.ParquetProducers.FloatDoubleProducer;
import org.knime.bigdata.fileformats.ParquetProducers.Int96LocalDateProducer;
import org.knime.bigdata.fileformats.ParquetProducers.Int96LocalDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.Int96ZonedDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.IntMillisLocalTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.IntegerIntegerProducer;
import org.knime.bigdata.fileformats.ParquetProducers.IntegerLocalDateProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongLongProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongMicrosLocalDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongMicrosLocalTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongMicrosZonedDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongMillisLocalDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongMillisZonedDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongNanosLocalDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongNanosLocalTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongNanosZonedDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.StringStringProducer;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetCellValueConsumerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetCellValueProducerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetDestination;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetListCellValueConsumerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetListCellValueProducerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeDestination;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeSource;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetSource;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.core.data.convert.map.ConsumerRegistry;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;

/**
 * Helper class for type mapping registration
 *
 * @author Sascha Wolke, KNIME GmbH
 */
final class ParquetLogicalTypeRegistrationHelper {

    /**
     * Registers the Parquet consumers
     */
    static void registerParquetConsumers() { // NOSONAR ignore method length

        final List<ParquetCellValueConsumerFactory<?>> primitiveConsumers = new ArrayList<>();

        final ParquetCellValueConsumerFactory<InputStream> binaryConsumer =
            new ParquetCellValueConsumerFactory<>(InputStream.class, new ParquetType(PrimitiveTypeName.BINARY),
                (c, v) -> c.addBinary(Binary.fromConstantByteArray(IOUtils.toByteArray(v))));
        primitiveConsumers.add(binaryConsumer);

        final ParquetCellValueConsumerFactory<String> stringToBinaryEnumConsumer = new ParquetCellValueConsumerFactory<>(
            String.class, new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.enumType()),
            (c, v) -> c.addBinary(Binary.fromString(v)));
        primitiveConsumers.add(stringToBinaryEnumConsumer);

        final ParquetCellValueConsumerFactory<String> stringToBinaryJSONConsumer = new ParquetCellValueConsumerFactory<>(
            String.class, new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.jsonType()),
            (c, v) -> c.addBinary(Binary.fromString(v)));
        primitiveConsumers.add(stringToBinaryJSONConsumer);

        final ParquetCellValueConsumerFactory<String> stringToBinaryUTF8Consumer = new ParquetCellValueConsumerFactory<>(
            String.class, new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),
            (c, v) -> c.addBinary(Binary.fromString(v)));
        primitiveConsumers.add(stringToBinaryUTF8Consumer);

        final ParquetCellValueConsumerFactory<String> stringToBinaryUUIDConsumer =
            new ParquetCellValueConsumerFactory<>(String.class,
                new ParquetType(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType()),
                new ParquetConsumers.UUIDasFixedLenByteArray());
        primitiveConsumers.add(stringToBinaryUUIDConsumer);

        final ParquetCellValueConsumerFactory<Boolean> booleanConsumer = new ParquetCellValueConsumerFactory<>(
            Boolean.class, new ParquetType(PrimitiveTypeName.BOOLEAN), (c, v) -> c.addBoolean(v));
        primitiveConsumers.add(booleanConsumer);

        final ParquetCellValueConsumerFactory<Double> doubleConsumer = new ParquetCellValueConsumerFactory<>(
            Double.class, new ParquetType(PrimitiveTypeName.DOUBLE), (c, v) -> c.addDouble(v));
        primitiveConsumers.add(doubleConsumer);

        final ParquetCellValueConsumerFactory<Double> floatConsumer = new ParquetCellValueConsumerFactory<>(Double.class,
            new ParquetType(PrimitiveTypeName.FLOAT), (c, v) -> c.addFloat(v.floatValue()));
        primitiveConsumers.add(floatConsumer);

        ////////////////////////////////// Integer (signed) //////////////////////////////////
        final ParquetCellValueConsumerFactory<Integer> integerConsumer = new ParquetCellValueConsumerFactory<>(
            Integer.class, new ParquetType(PrimitiveTypeName.INT32), (c, v) -> c.addInteger(v));
        primitiveConsumers.add(integerConsumer);

        final ParquetCellValueConsumerFactory<Integer> integerToIn32SignedInt8Consumer = new ParquetCellValueConsumerFactory<>(
                Integer.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)),
                new ParquetConsumers.IntToSignedIntConsumer(8, Byte.MIN_VALUE, Byte.MAX_VALUE));
        primitiveConsumers.add(integerToIn32SignedInt8Consumer);

        final ParquetCellValueConsumerFactory<Integer> integerToIn32SignedInt16Consumer = new ParquetCellValueConsumerFactory<>(
                Integer.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true)),
                new ParquetConsumers.IntToSignedIntConsumer(16, Short.MIN_VALUE, Short.MAX_VALUE));
        primitiveConsumers.add(integerToIn32SignedInt16Consumer);

        final ParquetCellValueConsumerFactory<Integer> integerToIn32SignedInt32Consumer =
            new ParquetCellValueConsumerFactory<>(Integer.class,
                new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)),
                (c, v) -> c.addInteger(v));
        primitiveConsumers.add(integerToIn32SignedInt32Consumer);

        final ParquetCellValueConsumerFactory<Integer> integerToIn64SignedInt64Consumer = new ParquetCellValueConsumerFactory<>(
                Integer.class, new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, true)),
                (c, v) -> c.addLong(v));
        primitiveConsumers.add(integerToIn64SignedInt64Consumer);

        ////////////////////////////////// Long (signed) //////////////////////////////////
        final ParquetCellValueConsumerFactory<Long> longToInt64Consumer = new ParquetCellValueConsumerFactory<>(
            Long.class, new ParquetType(PrimitiveTypeName.INT64), (c, v) -> c.addLong(v));
        primitiveConsumers.add(longToInt64Consumer);

        final ParquetCellValueConsumerFactory<Long> longToIn32SignedInt8Consumer = new ParquetCellValueConsumerFactory<>(
                Long.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)),
                new ParquetConsumers.LongToSignedIntConsumer(8, Byte.MIN_VALUE, Byte.MAX_VALUE));
        primitiveConsumers.add(longToIn32SignedInt8Consumer);

        final ParquetCellValueConsumerFactory<Long> longToIn32SignedInt16Consumer = new ParquetCellValueConsumerFactory<>(
                Long.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true)),
                new ParquetConsumers.LongToSignedIntConsumer(16, Short.MIN_VALUE, Short.MAX_VALUE));
        primitiveConsumers.add(longToIn32SignedInt16Consumer);

        final ParquetCellValueConsumerFactory<Long> longToIn32SignedInt32Consumer = new ParquetCellValueConsumerFactory<>(
                Long.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)),
                new ParquetConsumers.LongToSignedIntConsumer(32, Integer.MIN_VALUE, Integer.MAX_VALUE));
        primitiveConsumers.add(longToIn32SignedInt32Consumer);

        final ParquetCellValueConsumerFactory<Long> longToInt64Int64Consumer = new ParquetCellValueConsumerFactory<>(
            Long.class, new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, true)), //
            (c, v) -> c.addLong(v));
        primitiveConsumers.add(longToInt64Int64Consumer);

        ////////////////////////////////// Integer (unsigned) //////////////////////////////////
        final ParquetCellValueConsumerFactory<Integer> integerToInt32UnsignedInt8Consumer = new ParquetCellValueConsumerFactory<>(
                Integer.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, false)),
                new ParquetConsumers.IntToUnsignedIntOrLongConsumer(8, 2l*Byte.MAX_VALUE + 1));
        primitiveConsumers.add(integerToInt32UnsignedInt8Consumer);

        final ParquetCellValueConsumerFactory<Integer> integerToInt32UnsignedInt16Consumer = new ParquetCellValueConsumerFactory<>(
                Integer.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, false)),
                new ParquetConsumers.IntToUnsignedIntOrLongConsumer(16, 2l*Short.MAX_VALUE + 1));
        primitiveConsumers.add(integerToInt32UnsignedInt16Consumer);

        final ParquetCellValueConsumerFactory<Integer> integerToInt32UnsignedInt32Consumer = new ParquetCellValueConsumerFactory<>(
                Integer.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false)),
                new ParquetConsumers.IntToUnsignedIntOrLongConsumer(32));
        primitiveConsumers.add(integerToInt32UnsignedInt32Consumer);

        final ParquetCellValueConsumerFactory<Integer> integerToInt32UnsignedInt64Consumer = new ParquetCellValueConsumerFactory<>(
                Integer.class, new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false)),
                new ParquetConsumers.IntToUnsignedIntOrLongConsumer(64));
        primitiveConsumers.add(integerToInt32UnsignedInt64Consumer);

        ////////////////////////////////// Long (unsigned) //////////////////////////////////
        final ParquetCellValueConsumerFactory<Long> longToInt32UnsignedInt8Consumer = new ParquetCellValueConsumerFactory<>(
                Long.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, false)),
                new ParquetConsumers.LongToUnsignedIntConsumer(8, 2l*Byte.MAX_VALUE + 1));
        primitiveConsumers.add(longToInt32UnsignedInt8Consumer);

        final ParquetCellValueConsumerFactory<Long> longToInt32UnsignedInt16Consumer = new ParquetCellValueConsumerFactory<>(
                Long.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, false)),
                new ParquetConsumers.LongToUnsignedIntConsumer(16, 2l*Short.MAX_VALUE + 1));
        primitiveConsumers.add(longToInt32UnsignedInt16Consumer);

        final ParquetCellValueConsumerFactory<Long> longToInt32UnsignedInt32Consumer = new ParquetCellValueConsumerFactory<>(
                Long.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false)),
                new ParquetConsumers.LongToUnsignedIntConsumer(32, 2l*Integer.MAX_VALUE + 1));
        primitiveConsumers.add(longToInt32UnsignedInt32Consumer);

        final ParquetCellValueConsumerFactory<Long> longToInt64UnsignedInt64Consumer = new ParquetCellValueConsumerFactory<>(
                Long.class, new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false)),
                new ParquetConsumers.LongToUnsignedLongConsumer());
        primitiveConsumers.add(longToInt64UnsignedInt64Consumer);

        ////////////////////////////////// LocalDate //////////////////////////////////
        final ParquetCellValueConsumerFactory<LocalDate> localDateConsumer = new ParquetCellValueConsumerFactory<>(
            LocalDate.class, new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()),
            (c, v) -> c.addInteger((int)v.toEpochDay()));
        primitiveConsumers.add(localDateConsumer);

        // Special case for Impala on CDH => 6.2 (Date as Timestamp on int64)
        final ParquetCellValueConsumerFactory<LocalDate> localDateAsInt64Consumer =
            new ParquetCellValueConsumerFactory<>(LocalDate.class,
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                (c, v) -> c.addLong(v.atTime(0, 0, 0).atZone(ZoneOffset.UTC).toInstant().toEpochMilli()));
        primitiveConsumers.add(localDateAsInt64Consumer);

        // Special case for Impala on CDH < 6.2 (Date as Timestamp on int96)
        final ParquetCellValueConsumerFactory<LocalDate> localDateAsInt96Consumer =
            new ParquetCellValueConsumerFactory<>(LocalDate.class,
                new ParquetType(PrimitiveTypeName.INT96), (c, v) -> {
                    final var buffer = new byte[12];
                    final var byteBuffer = ByteBuffer.wrap(buffer);
                    final ZonedDateTime dateTime = v.atTime(0, 0, 0).atZone(ZoneOffset.UTC);
                    final long timeOfDayNanos = 0;
                    final int julianDay = (int)JulianFields.JULIAN_DAY.getFrom(dateTime.toLocalDate());
                    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay);
                    c.addBinary(Binary.fromConstantByteArray(buffer));
                });
        primitiveConsumers.add(localDateAsInt96Consumer);

        ////////////////////////////////// LocalTime (adjusted and not adjusted to UTC) //////////////////////////////////
        for (final boolean adjustedToUTC : new boolean[] { true, false }) {
            final ParquetCellValueConsumerFactory<LocalTime> timeToMillisConsumer =
                new ParquetCellValueConsumerFactory<>(LocalTime.class,
                    new ParquetType(PrimitiveTypeName.INT32,
                        LogicalTypeAnnotation.timeType(adjustedToUTC, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                    (c, v) -> c.addInteger((int)TimeUnit.MILLISECONDS.convert(v.toNanoOfDay(), TimeUnit.NANOSECONDS)));
            primitiveConsumers.add(timeToMillisConsumer);

            final ParquetCellValueConsumerFactory<LocalTime> timeToMicrosConsumer =
                new ParquetCellValueConsumerFactory<>(LocalTime.class,
                    new ParquetType(PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.timeType(adjustedToUTC, LogicalTypeAnnotation.TimeUnit.MICROS)),
                    (c, v) -> c.addLong(TimeUnit.MICROSECONDS.convert(v.toNanoOfDay(), TimeUnit.NANOSECONDS)));
            primitiveConsumers.add(timeToMicrosConsumer);

            final ParquetCellValueConsumerFactory<LocalTime> timeToNanosConsumer =
                new ParquetCellValueConsumerFactory<>(LocalTime.class,
                    new ParquetType(PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.timeType(adjustedToUTC, LogicalTypeAnnotation.TimeUnit.NANOS)),
                    (c, v) -> c.addLong(TimeUnit.NANOSECONDS.convert(v.toNanoOfDay(), TimeUnit.NANOSECONDS)));
            primitiveConsumers.add(timeToNanosConsumer);
        }

        ////////////////////////////////// LocalDateTime (adjusted and not adjusted to UTC) //////////////////////////////////
        for (final boolean adjustedToUTC : new boolean[] { true, false }) {
            final ParquetCellValueConsumerFactory<LocalDateTime> localDateTimeAsInt64MillisConsumer =
                new ParquetCellValueConsumerFactory<>(LocalDateTime.class,
                    new ParquetType(PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.timestampType(adjustedToUTC, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                    new ParquetConsumers.LocalDateTimeAsInt64MillisConsumer());
            primitiveConsumers.add(localDateTimeAsInt64MillisConsumer);

            final ParquetCellValueConsumerFactory<LocalDateTime> localDateTimeAsInt64MicrosConsumer =
                new ParquetCellValueConsumerFactory<>(LocalDateTime.class,
                    new ParquetType(PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.timestampType(adjustedToUTC, LogicalTypeAnnotation.TimeUnit.MICROS)),
                    new ParquetConsumers.LocalDateTimeAsInt64MicrosConsumer());
            primitiveConsumers.add(localDateTimeAsInt64MicrosConsumer);

            final ParquetCellValueConsumerFactory<LocalDateTime> localDateTimeAsInt64NanosConsumer =
                new ParquetCellValueConsumerFactory<>(LocalDateTime.class,
                    new ParquetType(PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.timestampType(adjustedToUTC, LogicalTypeAnnotation.TimeUnit.NANOS)),
                    new ParquetConsumers.LocalDateTimeAsInt64NanosConsumer());
            primitiveConsumers.add(localDateTimeAsInt64NanosConsumer);
        }

        // Impala on CDH < 6.2 and Hive
        final ParquetCellValueConsumerFactory<LocalDateTime> localDateTimeAsInt96Consumer =
            new ParquetCellValueConsumerFactory<>(LocalDateTime.class, new ParquetType(PrimitiveTypeName.INT96),
                new ParquetConsumers.LocalDateTimeAsInt96Consumer());
        primitiveConsumers.add(localDateTimeAsInt96Consumer);

        ////////////////////////////////// ZonedDateTime (adjusted to UTC) //////////////////////////////////
        final ParquetCellValueConsumerFactory<ZonedDateTime> zonedDateTimeAsInt64MillisConsumerUTC =
            new ParquetCellValueConsumerFactory<>(ZonedDateTime.class,
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                new ParquetConsumers.ZonedDateTimeAsInt64MillisConsumerUTC());
        primitiveConsumers.add(zonedDateTimeAsInt64MillisConsumerUTC);

        final ParquetCellValueConsumerFactory<ZonedDateTime> zonedDateTimeAsInt64MicrosConsumerUTC =
            new ParquetCellValueConsumerFactory<>(ZonedDateTime.class,
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)),
                new ParquetConsumers.ZonedDateTimeAsInt64MicrosConsumerUTC());
        primitiveConsumers.add(zonedDateTimeAsInt64MicrosConsumerUTC);

        final ParquetCellValueConsumerFactory<ZonedDateTime> zonedDateTimeAsInt64NanosConsumerUTC =
            new ParquetCellValueConsumerFactory<>(ZonedDateTime.class,
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)),
                new ParquetConsumers.ZonedDateTimeAsInt64NanosConsumerUTC());
        primitiveConsumers.add(zonedDateTimeAsInt64NanosConsumerUTC);

        ////////////////////////////////// ZonedDateTime (not adjusted to UTC) //////////////////////////////////
        final ParquetCellValueConsumerFactory<ZonedDateTime> zonedDateTimeAsInt64MillisConsumerNoUTC =
            new ParquetCellValueConsumerFactory<>(ZonedDateTime.class,
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                new ParquetConsumers.ZonedDateTimeAsInt64MillisConsumerNoUTC());
        primitiveConsumers.add(zonedDateTimeAsInt64MillisConsumerNoUTC);

        final ParquetCellValueConsumerFactory<ZonedDateTime> zonedDateTimeAsInt64MicrosConsumerNoUTC =
            new ParquetCellValueConsumerFactory<>(ZonedDateTime.class,
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)),
                new ParquetConsumers.ZonedDateTimeAsInt64MicrosConsumerNoUTC());
        primitiveConsumers.add(zonedDateTimeAsInt64MicrosConsumerNoUTC);

        final ParquetCellValueConsumerFactory<ZonedDateTime> zonedDateTimeAsInt64NanosConsumerNoUTC =
            new ParquetCellValueConsumerFactory<>(ZonedDateTime.class,
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS)),
                new ParquetConsumers.ZonedDateTimeAsInt64NanosConsumerNoUTC());
        primitiveConsumers.add(zonedDateTimeAsInt64NanosConsumerNoUTC);

        ////////////////////////////////// add to registry //////////////////////////////////
        final ConsumerRegistry<ParquetType, ParquetDestination> consumerRegisty =
            MappingFramework.forDestinationType(ParquetLogicalTypeDestination.class);

        for (final ParquetCellValueConsumerFactory<?> elementFactory : primitiveConsumers) {

            consumerRegisty.register(elementFactory);
            final ParquetListCellValueConsumerFactory<Array, ?> arrayConsumer =
                new ParquetListCellValueConsumerFactory<>(elementFactory);
            consumerRegisty.register(arrayConsumer);
        }

    }

    /**
     * Registers the producer for the parquet types
     */
    static void registerParquetProducers() {
        final List<ParquetCellValueProducerFactory<?>> primitiveProducers = new ArrayList<>();

        final ParquetCellValueProducerFactory<Boolean> booleanBooleanProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.BOOLEAN), Boolean.class, new BooleanbooleanProducer());
        primitiveProducers.add(booleanBooleanProducer);

        final ParquetCellValueProducerFactory<InputStream> binaryProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.BINARY), InputStream.class, new BytesInputStreamProducer());
        primitiveProducers.add(binaryProducer);

        final ParquetCellValueProducerFactory<String> binaryEnumToStringProducer =
            new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.enumType()),
                String.class, new StringStringProducer());
        primitiveProducers.add(binaryEnumToStringProducer);

        final ParquetCellValueProducerFactory<String> binaryJsonToStringProducer =
            new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.jsonType()),
                String.class, new StringStringProducer());
        primitiveProducers.add(binaryJsonToStringProducer);

        final ParquetCellValueProducerFactory<String> binaryUtf8ToStringProducer =
            new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),
                String.class, new StringStringProducer());
        primitiveProducers.add(binaryUtf8ToStringProducer);

        final ParquetCellValueProducerFactory<byte[]> int96Producer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT96), byte[].class, new BytesBytArrayProducer());
        primitiveProducers.add(int96Producer);

        final ParquetCellValueProducerFactory<Double> doubleProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.DOUBLE), Double.class, new DoubleDoubleProducer());
        primitiveProducers.add(doubleProducer);

        final ParquetCellValueProducerFactory<Double> floatProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.FLOAT), Double.class, new FloatDoubleProducer());
        primitiveProducers.add(floatProducer);

        final ParquetCellValueProducerFactory<Long> int64ToLongProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT64), Long.class, new LongLongProducer());
        primitiveProducers.add(int64ToLongProducer);

        final ParquetCellValueProducerFactory<Long> int64Int64ToLongProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, true)), Long.class, new LongLongProducer());
        primitiveProducers.add(int64Int64ToLongProducer);

        final ParquetCellValueProducerFactory<LocalTime> int32MilliToTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT32,
                    LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                LocalTime.class, new IntMillisLocalTimeProducer());
        primitiveProducers.add(int32MilliToTimeProducer);

        final ParquetCellValueProducerFactory<LocalTime> int64MicroToTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS)),
                LocalTime.class, new LongMicrosLocalTimeProducer());
        primitiveProducers.add(int64MicroToTimeProducer);

        final ParquetCellValueProducerFactory<LocalTime> int64NanoToTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.NANOS)),
                LocalTime.class, new LongNanosLocalTimeProducer());
        primitiveProducers.add(int64NanoToTimeProducer);

        final ParquetCellValueProducerFactory<ZonedDateTime> int64MillisAsZonedDateTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                ZonedDateTime.class, new LongMillisZonedDateTimeProducer());
        primitiveProducers.add(int64MillisAsZonedDateTimeProducer);

        final ParquetCellValueProducerFactory<ZonedDateTime> int64MicrosAsZonedDateTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)),
                ZonedDateTime.class, new LongMicrosZonedDateTimeProducer());
        primitiveProducers.add(int64MicrosAsZonedDateTimeProducer);

        final ParquetCellValueProducerFactory<ZonedDateTime> int64NanosAsZonedDateTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)),
                ZonedDateTime.class, new LongNanosZonedDateTimeProducer());
        primitiveProducers.add(int64NanosAsZonedDateTimeProducer);

        final ParquetCellValueProducerFactory<ZonedDateTime> int96asZonedDateTimeProducer =
                new ParquetCellValueProducerFactory<>(
                    new ParquetType(PrimitiveTypeName.INT96), ZonedDateTime.class,
                    new Int96ZonedDateTimeProducer());
        primitiveProducers.add(int96asZonedDateTimeProducer);

        final ParquetCellValueProducerFactory<Integer> int32ToIntProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT32), Integer.class, new IntegerIntegerProducer());
        primitiveProducers.add(int32ToIntProducer);

        final ParquetCellValueProducerFactory<Integer> int32Int8ToIntProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)), Integer.class, new IntegerIntegerProducer());
        primitiveProducers.add(int32Int8ToIntProducer);

        final ParquetCellValueProducerFactory<Integer> int32Int16ToIntProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true)), Integer.class, new IntegerIntegerProducer());
        primitiveProducers.add(int32Int16ToIntProducer);

        final ParquetCellValueProducerFactory<Integer> int32Int32ToIntProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)), Integer.class, new IntegerIntegerProducer());
        primitiveProducers.add(int32Int32ToIntProducer);

        final ParquetCellValueProducerFactory<LocalDate> int32dateProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()), LocalDate.class,
            new IntegerLocalDateProducer());
        primitiveProducers.add(int32dateProducer);

        final ParquetCellValueProducerFactory<LocalDate> int96dateProducer =
                new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.INT96),
                    LocalDate.class, new Int96LocalDateProducer());
        primitiveProducers.add(int96dateProducer);

        final ParquetCellValueProducerFactory<LocalDateTime> int64MillisDateTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                LocalDateTime.class, new LongMillisLocalDateTimeProducer());
        primitiveProducers.add(int64MillisDateTimeProducer);

        final ParquetCellValueProducerFactory<LocalDateTime> int64MicrosDateTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)),
                LocalDateTime.class, new LongMicrosLocalDateTimeProducer());
        primitiveProducers.add(int64MicrosDateTimeProducer);

        final ParquetCellValueProducerFactory<LocalDateTime> int64NanosDateTimeProducer =
                new ParquetCellValueProducerFactory<>(
                    new ParquetType(PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS)),
                    LocalDateTime.class, new LongNanosLocalDateTimeProducer());
            primitiveProducers.add(int64NanosDateTimeProducer);

        final ParquetCellValueProducerFactory<LocalDateTime> int96dateTimeProducer = new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT96), LocalDateTime.class,
                new Int96LocalDateTimeProducer());
        primitiveProducers.add(int96dateTimeProducer);

        final ParquetCellValueProducerFactory<byte[]> byteArrayProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY), byte[].class, new BinaryByteArrayProducer());
        primitiveProducers.add(byteArrayProducer);

        // Register the specific producers
        final ProducerRegistry<ParquetType, ParquetSource> producerRegistry =
            MappingFramework.forSourceType(ParquetLogicalTypeSource.class);
        for (final ParquetCellValueProducerFactory<?> elementFactory : primitiveProducers) {
            producerRegistry.register(elementFactory);
            ParquetListCellValueProducerFactory<Array, ?> listFactory = new ParquetListCellValueProducerFactory<>(
                elementFactory.getSourceType(), elementFactory.getDestinationType(), elementFactory);
            producerRegistry.register(listFactory);
        }
    }

    private ParquetLogicalTypeRegistrationHelper() {
        throw new IllegalStateException("Utility class");
    }

}
