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
 *   13.06.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.JulianFields;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.io.api.Binary;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetCellValueProducer;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetConverter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetPrimitiveConverter;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetProducers {

    /**
     * Producer for Boolean values
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class BooleanbooleanProducer extends ParquetCellValueProducer<Boolean> {

        /**
         * {@inheritDoc}
         */
        @Override
        protected ParquetConverter<Boolean> createConverter() {
            return new ParquetPrimitiveConverter<Boolean>() {

                @Override
                public void addBoolean(final boolean value) {
                    m_value = value;
                }
            };
        }

        @Override
        public BooleanbooleanProducer cloneProducer() {
            return new BooleanbooleanProducer();
        }

    }

    /**
     * Producer for String values
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class StringStringProducer extends ParquetCellValueProducer<String> {
        @Override
        public ParquetPrimitiveConverter<String> createConverter() {
            return new ParquetPrimitiveConverter<String>() {

                @Override
                public void addBinary(final Binary value) {
                    m_value = value.toStringUsingUTF8();
                }
            };
        }

        @Override
        public StringStringProducer cloneProducer() {
            return new StringStringProducer();
        }
    }

    /**
     * Producer for InputStream
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class BytesInputStreamProducer extends ParquetCellValueProducer<InputStream> {
        @Override
        public ParquetPrimitiveConverter<InputStream> createConverter() {
            return new ParquetPrimitiveConverter<InputStream>() {

                @Override
                public void addBinary(final Binary value) {
                    m_value = new ByteArrayInputStream(value.getBytes());
                }
            };
        }

        @Override
        public BytesInputStreamProducer cloneProducer() {
            return new BytesInputStreamProducer();
        }

    }

    /**
     * Producer for byte array
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class BytesBytArrayProducer extends ParquetCellValueProducer<byte[]> {
        @Override
        public ParquetPrimitiveConverter<byte[]> createConverter() {
            return new ParquetPrimitiveConverter<byte[]>() {

                @Override
                public void addBinary(final Binary value) {
                    m_value = value.getBytes();
                }
            };
        }

        @Override
        public BytesBytArrayProducer cloneProducer() {
            return new BytesBytArrayProducer();
        }
    }

    /**
     * Producer for Double values
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class DoubleDoubleProducer extends ParquetCellValueProducer<Double> {
        @Override
        public ParquetPrimitiveConverter<Double> createConverter() {
            return new ParquetPrimitiveConverter<Double>() {

                @Override
                public void addDouble(final double value) {
                    m_value = value;
                }
            };
        }

        @Override
        public DoubleDoubleProducer cloneProducer() {
            return new DoubleDoubleProducer();
        }
    }

    /**
     * Producer for Double value from Float
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class FloatDoubleProducer extends ParquetCellValueProducer<Double> {
        @Override
        public ParquetPrimitiveConverter<Double> createConverter() {
            return new ParquetPrimitiveConverter<Double>() {

                @Override
                public void addFloat(final float value) {
                    m_value = (double)value;
                }
            };
        }

        @Override
        public FloatDoubleProducer cloneProducer() {
            return new FloatDoubleProducer();
        }
    }

    /**
     * Producer for Long values
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class LongLongProducer extends ParquetCellValueProducer<Long> {
        @Override
        public ParquetPrimitiveConverter<Long> createConverter() {
            return new ParquetPrimitiveConverter<Long>() {

                @Override
                public void addLong(final long value) {
                    m_value = value;
                }
            };
        }

        @Override
        public LongLongProducer cloneProducer() {
            return new LongLongProducer();
        }
    }

    /**
     * Producer for LocalTime from Integer (Milliseconds)
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class IntMillisLocalTimeProducer extends ParquetCellValueProducer<LocalTime> {
        @Override
        public ParquetPrimitiveConverter<LocalTime> createConverter() {
            return new ParquetPrimitiveConverter<LocalTime>() {

                @Override
                public void addInt(final int value) {
                    m_value = LocalTime.ofNanoOfDay(TimeUnit.NANOSECONDS.convert(value, TimeUnit.MILLISECONDS));
                }
            };
        }

        @Override
        public IntMillisLocalTimeProducer cloneProducer() {
            return new IntMillisLocalTimeProducer();
        }
    }

    /**
     * Producer for LocalTime from Long (Microseconds)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class LongMicrosLocalTimeProducer extends ParquetCellValueProducer<LocalTime> {
        @Override
        public ParquetPrimitiveConverter<LocalTime> createConverter() {
            return new ParquetPrimitiveConverter<LocalTime>() {

                @Override
                public void addLong(final long value) {
                    m_value = LocalTime.ofNanoOfDay(TimeUnit.NANOSECONDS.convert(value, TimeUnit.MICROSECONDS));
                }
            };
        }

        @Override
        public LongMicrosLocalTimeProducer cloneProducer() {
            return new LongMicrosLocalTimeProducer();
        }
    }

    /**
     * Producer for LocalTime from Long (Nanoseconds)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class LongNanosLocalTimeProducer extends ParquetCellValueProducer<LocalTime> {
        @Override
        public ParquetPrimitiveConverter<LocalTime> createConverter() {
            return new ParquetPrimitiveConverter<LocalTime>() {

                @Override
                public void addLong(final long value) {
                    m_value = LocalTime.ofNanoOfDay(value);
                }
            };
        }

        @Override
        public ParquetCellValueProducer<LocalTime> cloneProducer() {
            return new LongNanosLocalTimeProducer();
        }
    }

    /**
     * Producer for Zoned Date Time from Long (Milliseconds)
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class LongMillisZonedDateTimeProducer extends ParquetCellValueProducer<ZonedDateTime> {
        @Override
        public ParquetPrimitiveConverter<ZonedDateTime> createConverter() {
            return new ParquetPrimitiveConverter<ZonedDateTime>() {

                @Override
                public void addLong(final long value) {
                    m_value = ZonedDateTime.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC),
                        ZoneId.of("Etc/UTC"));
                }
            };
        }

        @Override
        public LongMillisZonedDateTimeProducer cloneProducer() {
            return new LongMillisZonedDateTimeProducer();
        }
    }

    /**
     * Producer for Zoned Date Time from Long (Microseconds)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class LongMicrosZonedDateTimeProducer extends ParquetCellValueProducer<ZonedDateTime> {
        @Override
        public ParquetPrimitiveConverter<ZonedDateTime> createConverter() {
            return new ParquetPrimitiveConverter<ZonedDateTime>() {

                @Override
                public void addLong(final long value) {
                    final long seconds = TimeUnit.SECONDS.convert(value, TimeUnit.MICROSECONDS);
                    final long nanos = (value % (1000*1000l)) * 1000l;
                    final Instant instant = Instant.ofEpochSecond(seconds, nanos);
                    m_value = ZonedDateTime.of(LocalDateTime.ofInstant(instant, ZoneOffset.UTC),
                        ZoneId.of("Etc/UTC"));
                }
            };
        }

        @Override
        public LongMicrosZonedDateTimeProducer cloneProducer() {
            return new LongMicrosZonedDateTimeProducer();
        }
    }

    /**
     * Producer for Zoned Date Time from Long (Nanoseconds)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class LongNanosZonedDateTimeProducer extends ParquetCellValueProducer<ZonedDateTime> {
        @Override
        public ParquetPrimitiveConverter<ZonedDateTime> createConverter() {
            return new ParquetPrimitiveConverter<ZonedDateTime>() {

                @Override
                public void addLong(final long value) {
                    final long seconds = TimeUnit.SECONDS.convert(value, TimeUnit.NANOSECONDS);
                    final long nanos = (value % (1000*1000*1000l));
                    final Instant instant = Instant.ofEpochSecond(seconds, nanos);
                    m_value = ZonedDateTime.of(LocalDateTime.ofInstant(instant, ZoneOffset.UTC),
                        ZoneId.of("Etc/UTC"));
                }
            };
        }

        @Override
        public ParquetCellValueProducer<ZonedDateTime> cloneProducer() {
            return new LongNanosZonedDateTimeProducer();
        }
    }

    /**
     * Producer for Zoned Date Time from Binary (INT96)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class Int96ZonedDateTimeProducer extends ParquetCellValueProducer<ZonedDateTime> {
        @Override
        public ParquetPrimitiveConverter<ZonedDateTime> createConverter() {
            return new ParquetPrimitiveConverter<ZonedDateTime>() {

                @Override
                public void addBinary(final Binary value) {
                    final ByteBuffer buf = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                    final LocalTime localTime = LocalTime.ofNanoOfDay(buf.getLong());
                    final LocalDate localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, buf.getInt());
                    m_value = ZonedDateTime.of(LocalDateTime.of(localDate, localTime), ZoneId.of("Etc/UTC"));
                }
            };
        }

        @Override
        public Int96ZonedDateTimeProducer cloneProducer() {
            return new Int96ZonedDateTimeProducer();
        }
    }

    /**
     * Producer for Integer values
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class IntegerIntegerProducer extends ParquetCellValueProducer<Integer> {
        @Override
        public ParquetPrimitiveConverter<Integer> createConverter() {
            return new ParquetPrimitiveConverter<Integer>() {

                @Override
                public void addInt(final int value) {
                    m_value = value;
                }
            };
        }

        @Override
        public IntegerIntegerProducer cloneProducer() {
            return new IntegerIntegerProducer();
        }
    }

    /**
     * Producer for Local Date from Integer
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class IntegerLocalDateProducer extends ParquetCellValueProducer<LocalDate> {
        @Override
        public ParquetPrimitiveConverter<LocalDate> createConverter() {
            return new ParquetPrimitiveConverter<LocalDate>() {

                @Override
                public void addInt(final int value) {
                    m_value = LocalDate.ofEpochDay(value);
                }
            };
        }

        @Override
        public IntegerLocalDateProducer cloneProducer() {
            return new IntegerLocalDateProducer();
        }
    }

    /**
     * Producer for Local Date from Binary (INT96)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class Int96LocalDateProducer extends ParquetCellValueProducer<LocalDate> {
        @Override
        public ParquetPrimitiveConverter<LocalDate> createConverter() {
            return new ParquetPrimitiveConverter<LocalDate>() {

                @Override
                public void addBinary(final Binary value) {
                    final ByteBuffer buf = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                    buf.getLong(); // skip time (first 8 bytes)
                    m_value = LocalDate.MIN.with(JulianFields.JULIAN_DAY, buf.getInt());
                }
            };
        }

        @Override
        public Int96LocalDateProducer cloneProducer() {
            return new Int96LocalDateProducer();
        }
    }

    /**
     * Producer for Local Date Time from Long (Milliseconds)
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class LongMillisLocalDateTimeProducer extends ParquetCellValueProducer<LocalDateTime> {
        @Override
        public ParquetPrimitiveConverter<LocalDateTime> createConverter() {
            return new ParquetPrimitiveConverter<LocalDateTime>() {

                @Override
                public void addLong(final long value) {
                    m_value = LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
                }
            };
        }

        @Override
        public LongMillisLocalDateTimeProducer cloneProducer() {
            return new LongMillisLocalDateTimeProducer();
        }
    }

    /**
     * Producer for Local Date Time from Long (Microseconds)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class LongMicrosLocalDateTimeProducer extends ParquetCellValueProducer<LocalDateTime> {
        @Override
        public ParquetPrimitiveConverter<LocalDateTime> createConverter() {
            return new ParquetPrimitiveConverter<LocalDateTime>() {

                @Override
                public void addLong(final long value) {
                    final long seconds = TimeUnit.SECONDS.convert(value, TimeUnit.MICROSECONDS);
                    final long nanos = (value % (1000*1000l)) * 1000l;
                    final Instant instant = Instant.ofEpochSecond(seconds, nanos);
                    m_value = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                }
            };
        }

        @Override
        public LongMicrosLocalDateTimeProducer cloneProducer() {
            return new LongMicrosLocalDateTimeProducer();
        }
    }

    /**
     * Producer for Local Date Time from Long (Nanoseconds)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class LongNanosLocalDateTimeProducer extends ParquetCellValueProducer<LocalDateTime> {
        @Override
        public ParquetPrimitiveConverter<LocalDateTime> createConverter() {
            return new ParquetPrimitiveConverter<LocalDateTime>() {

                @Override
                public void addLong(final long value) {
                    final long seconds = TimeUnit.SECONDS.convert(value, TimeUnit.NANOSECONDS);
                    final long nanos = (value % (1000*1000*1000l));
                    final Instant instant = Instant.ofEpochSecond(seconds, nanos);
                    m_value = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                }
            };
        }

        @Override
        public ParquetCellValueProducer<LocalDateTime> cloneProducer() {
            return new LongNanosLocalDateTimeProducer();
        }
    }

    /**
     * Producer for Local Date Time from Binary (INT96)
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class Int96LocalDateTimeProducer extends ParquetCellValueProducer<LocalDateTime> {
        @Override
        public ParquetPrimitiveConverter<LocalDateTime> createConverter() {
            return new ParquetPrimitiveConverter<LocalDateTime>() {

                @Override
                public void addBinary(final Binary value) {
                    final ByteBuffer buf = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                    final LocalTime localTime = LocalTime.ofNanoOfDay(buf.getLong());
                    final LocalDate localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, buf.getInt());
                    m_value = LocalDateTime.of(localDate, localTime);
                }
            };
        }

        @Override
        public Int96LocalDateTimeProducer cloneProducer() {
            return new Int96LocalDateTimeProducer();
        }
    }

    /**
     * Producer for byte array from binary
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    public static class BinaryByteArrayProducer extends ParquetCellValueProducer<byte[]> {
        @Override
        public ParquetPrimitiveConverter<byte[]> createConverter() {
            return new ParquetPrimitiveConverter<byte[]>() {

                @Override
                public void addBinary(final Binary value) {
                    m_value = value.getBytes();
                }
            };
        }

        @Override
        public BinaryByteArrayProducer cloneProducer() {
            return new BinaryByteArrayProducer();
        }
    }
}
