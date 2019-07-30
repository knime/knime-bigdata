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
 *
 * History
 *   09.10.2018 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
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
import org.apache.parquet.schema.OriginalType;
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
import org.knime.bigdata.fileformats.ParquetProducers.IntLocalTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.IntegerIntegerProducer;
import org.knime.bigdata.fileformats.ParquetProducers.IntegerLocalDateProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongLocalDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongLongProducer;
import org.knime.bigdata.fileformats.ParquetProducers.LongZonedDateTimeProducer;
import org.knime.bigdata.fileformats.ParquetProducers.StringStringProducer;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetCellValueConsumerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetCellValueProducerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetDestination;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetListCellValueConsumerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetListCellValueProducerFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetSource;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.core.data.convert.map.ConsumerRegistry;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;

/**
 * Helper class for type mapping registration
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class ParquetRegistrationHelper {
    /**
     * Registers the Parquet consumers
     */
    public static void registerParquetConsumers() {

        final List<ParquetCellValueConsumerFactory<?>> primitiveConsumers = new ArrayList<>();

        final ParquetCellValueConsumerFactory<InputStream> binaryConsumer =
            new ParquetCellValueConsumerFactory<>(InputStream.class, new ParquetType(PrimitiveTypeName.BINARY),
                (c, v) -> c.addBinary(Binary.fromConstantByteArray(IOUtils.toByteArray(v))));
        primitiveConsumers.add(binaryConsumer);

        final ParquetCellValueConsumerFactory<String> stringStringKindConsumer = new ParquetCellValueConsumerFactory<>(
            String.class, new ParquetType(PrimitiveTypeName.BINARY, OriginalType.UTF8),
            (c, v) -> c.addBinary(Binary.fromString(v)));
        primitiveConsumers.add(stringStringKindConsumer);

        final ParquetCellValueConsumerFactory<Boolean> booleanConsumer = new ParquetCellValueConsumerFactory<>(
            Boolean.class, new ParquetType(PrimitiveTypeName.BOOLEAN), (c, v) -> c.addBoolean(v));
        primitiveConsumers.add(booleanConsumer);

        final ParquetCellValueConsumerFactory<Double> doubleConsumer = new ParquetCellValueConsumerFactory<>(
            Double.class, new ParquetType(PrimitiveTypeName.DOUBLE), (c, v) -> c.addDouble(v));
        primitiveConsumers.add(doubleConsumer);

        final ParquetCellValueConsumerFactory<Float> floatConsumer = new ParquetCellValueConsumerFactory<>(Float.class,
            new ParquetType(PrimitiveTypeName.FLOAT), (c, v) -> c.addFloat(v));
        primitiveConsumers.add(floatConsumer);

        final ParquetCellValueConsumerFactory<Long> longConsumer = new ParquetCellValueConsumerFactory<>(Long.class,
            new ParquetType(PrimitiveTypeName.INT64), (c, v) -> c.addLong(v));
        primitiveConsumers.add(longConsumer);

        final ParquetCellValueConsumerFactory<LocalTime> timeConsumer = new ParquetCellValueConsumerFactory<>(
            LocalTime.class, new ParquetType(PrimitiveTypeName.INT32, OriginalType.TIME_MILLIS),
            (c, v) -> c.addInteger((int)TimeUnit.MILLISECONDS.convert(v.toNanoOfDay(), TimeUnit.NANOSECONDS)));
        primitiveConsumers.add(timeConsumer);

        final ParquetCellValueConsumerFactory<Integer> integerConsumer = new ParquetCellValueConsumerFactory<>(
            Integer.class, new ParquetType(PrimitiveTypeName.INT32), (c, v) -> c.addInteger(v));
        primitiveConsumers.add(integerConsumer);

        final ParquetCellValueConsumerFactory<LocalDate> localDateConsumer = new ParquetCellValueConsumerFactory<>(
            LocalDate.class, new ParquetType(PrimitiveTypeName.INT32, OriginalType.DATE),
            (c, v) -> c.addInteger((int)v.toEpochDay()));
        primitiveConsumers.add(localDateConsumer);

        // Special case for Impala on CDH => 6.2 (Date as Timestamp)
        final ParquetCellValueConsumerFactory<LocalDate> localDateAsInt64Consumer = new ParquetCellValueConsumerFactory<>(
                LocalDate.class, new ParquetType(PrimitiveTypeName.INT64, OriginalType.TIMESTAMP_MILLIS),
                (c, v) -> c.addLong(v.atTime(0, 0, 0).atZone(ZoneOffset.UTC).toInstant().toEpochMilli()));
        primitiveConsumers.add(localDateAsInt64Consumer);

        // Special case for Impala on CDH < 6.2 (Date as Timestamp)
        final ParquetCellValueConsumerFactory<LocalDate> localDateAsInt96Consumer =
            new ParquetCellValueConsumerFactory<>(LocalDate.class,
                new ParquetType(PrimitiveTypeName.INT96), (c, v) -> {
                    final byte[] buffer = new byte[12];
                    final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
                    final ZonedDateTime dateTime = v.atTime(0, 0, 0).atZone(ZoneOffset.UTC);
                    final long timeOfDayNanos = 0;
                    final int julianDay = (int)JulianFields.JULIAN_DAY.getFrom(dateTime.toLocalDate());
                    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay);
                    c.addBinary(Binary.fromConstantByteArray(buffer));
                });
        primitiveConsumers.add(localDateAsInt96Consumer);

        // Impala on CDH => 6.2
        final ParquetCellValueConsumerFactory<LocalDateTime> localDateTimeAsInt64Consumer =
            new ParquetCellValueConsumerFactory<>(LocalDateTime.class,
                new ParquetType(PrimitiveTypeName.INT64, OriginalType.TIMESTAMP_MILLIS),
                (c, v) -> c.addLong(v.atZone(ZoneOffset.UTC).toInstant().toEpochMilli()));
        primitiveConsumers.add(localDateTimeAsInt64Consumer);

        // Impala on CDH < 6.2 and Hive
        final ParquetCellValueConsumerFactory<LocalDateTime> localDateTimeAsInt96Consumer =
            new ParquetCellValueConsumerFactory<>(LocalDateTime.class,
                new ParquetType(PrimitiveTypeName.INT96), (c, v) -> {
                    final byte[] buffer = new byte[12];
                    final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
                    final ZonedDateTime dateTime = v.atZone(ZoneOffset.UTC);
                    final long timeOfDayNanos = dateTime.toLocalTime().toNanoOfDay();
                    final int julianDay = (int)JulianFields.JULIAN_DAY.getFrom(dateTime.toLocalDate());
                    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay);
                    c.addBinary(Binary.fromConstantByteArray(buffer));
                });
        primitiveConsumers.add(localDateTimeAsInt96Consumer);

        final ParquetCellValueConsumerFactory<InputStream> bytearrayConsumer = new ParquetCellValueConsumerFactory<>(
            InputStream.class, new ParquetType(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY),
            (c, v) -> c.addBinary(Binary.fromConstantByteArray(IOUtils.toByteArray(v))));
        primitiveConsumers.add(bytearrayConsumer);

        final ConsumerRegistry<ParquetType, ParquetDestination> consumerRegisty =
            MappingFramework.forDestinationType(ParquetDestination.class);

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

    public static void registerParquetProducers() {

        final List<ParquetCellValueProducerFactory<?>> primitiveProducers = new ArrayList<>();

        final ParquetCellValueProducerFactory<Boolean> booleanBooleanProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.BOOLEAN), Boolean.class, new BooleanbooleanProducer());
        primitiveProducers.add(booleanBooleanProducer);

        final ParquetCellValueProducerFactory<InputStream> binaryProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.BINARY), InputStream.class, new BytesInputStreamProducer());
        primitiveProducers.add(binaryProducer);

        final ParquetCellValueProducerFactory<String> stringProducer =
            new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.BINARY, OriginalType.UTF8),
                String.class, new StringStringProducer());
        primitiveProducers.add(stringProducer);

        final ParquetCellValueProducerFactory<byte[]> int96Producer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT96), byte[].class, new BytesBytArrayProducer());
        primitiveProducers.add(int96Producer);

        final ParquetCellValueProducerFactory<Double> doubleProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.DOUBLE), Double.class, new DoubleDoubleProducer());
        primitiveProducers.add(doubleProducer);

        final ParquetCellValueProducerFactory<Double> floatProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.FLOAT), Double.class, new FloatDoubleProducer());
        primitiveProducers.add(floatProducer);

        final ParquetCellValueProducerFactory<Long> longProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT64), Long.class, new LongLongProducer());
        primitiveProducers.add(longProducer);

        final ParquetCellValueProducerFactory<LocalTime> timeProducer =
            new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.INT32, OriginalType.TIME_MILLIS),
                LocalTime.class, new IntLocalTimeProducer());
        primitiveProducers.add(timeProducer);

        final ParquetCellValueProducerFactory<ZonedDateTime> int64asZonedDateTimeProducer =
            new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT64, OriginalType.TIMESTAMP_MILLIS), ZonedDateTime.class,
                new LongZonedDateTimeProducer());
        primitiveProducers.add(int64asZonedDateTimeProducer);

        final ParquetCellValueProducerFactory<ZonedDateTime> int96asZonedDateTimeProducer =
                new ParquetCellValueProducerFactory<>(
                    new ParquetType(PrimitiveTypeName.INT96), ZonedDateTime.class,
                    new Int96ZonedDateTimeProducer());
        primitiveProducers.add(int96asZonedDateTimeProducer);

        final ParquetCellValueProducerFactory<Integer> intProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT32), Integer.class, new IntegerIntegerProducer());
        primitiveProducers.add(intProducer);

        final ParquetCellValueProducerFactory<LocalDate> int32dateProducer =
                new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.INT32, OriginalType.DATE),
                    LocalDate.class, new IntegerLocalDateProducer());
        primitiveProducers.add(int32dateProducer);

        final ParquetCellValueProducerFactory<LocalDate> int96dateProducer =
                new ParquetCellValueProducerFactory<>(new ParquetType(PrimitiveTypeName.INT96),
                    LocalDate.class, new Int96LocalDateProducer());
        primitiveProducers.add(int96dateProducer);

        final ParquetCellValueProducerFactory<LocalDateTime> int64dateTimeProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.INT64, OriginalType.TIMESTAMP_MILLIS), LocalDateTime.class,
            new LongLocalDateTimeProducer());
        primitiveProducers.add(int64dateTimeProducer);

        final ParquetCellValueProducerFactory<LocalDateTime> int96dateTimeProducer = new ParquetCellValueProducerFactory<>(
                new ParquetType(PrimitiveTypeName.INT96), LocalDateTime.class,
                new Int96LocalDateTimeProducer());
        primitiveProducers.add(int96dateTimeProducer);

        final ParquetCellValueProducerFactory<byte[]> byteArrayProducer = new ParquetCellValueProducerFactory<>(
            new ParquetType(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY), byte[].class, new BinaryByteArrayProducer());
        primitiveProducers.add(byteArrayProducer);

        // Register the specific producers
        final ProducerRegistry<ParquetType, ParquetSource> producerRegistry =
            MappingFramework.forSourceType(ParquetSource.class);
        for (final ParquetCellValueProducerFactory<?> elementFactory : primitiveProducers) {
            producerRegistry.register(elementFactory);
            ParquetListCellValueProducerFactory<Array, ?> listFactory = new ParquetListCellValueProducerFactory<>(
                elementFactory.getSourceType(), elementFactory.getDestinationType(), elementFactory);
            producerRegistry.register(listFactory);
        }
    }

    private ParquetRegistrationHelper() {
        throw new IllegalStateException("Utility class");
    }

}
