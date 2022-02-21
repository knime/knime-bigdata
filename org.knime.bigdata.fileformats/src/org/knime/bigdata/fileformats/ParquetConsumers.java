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
 *   Jan 31, 2022 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.fileformats;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.JulianFields;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetCellValueConsumer;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;

/**
 * Collection of {@link ParquetCellValueConsumer} implementations.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
final class ParquetConsumers {

    private ParquetConsumers() {
    }

    ////////////////////////////////// Integer (signed) //////////////////////////////////
    static class IntToSignedIntConsumer implements ParquetCellValueConsumer<Integer> {
        private final int m_bits;
        private final int m_min;
        private final int m_max;

        IntToSignedIntConsumer(final int bits, final int min, final int max) {
            m_bits = bits;
            m_min = min;
            m_max = max;
        }

        @Override
        public void writeNonNullValue(final RecordConsumer c, final Integer v) throws Exception {
            if (v < m_min || v > m_max) {
                throw new BigDataFileFormatException(String.format( //
                    "Cannot write %d as signed %d bit integer, only values between %d and %d are supported.", //
                    (int)v, m_bits, m_min, m_max));
            } else {
                c.addInteger(v);
            }
        }
    }

    static class LongToSignedIntConsumer implements ParquetCellValueConsumer<Long> {
        private final int m_bits;
        private final int m_min;
        private final int m_max;

        LongToSignedIntConsumer(final int bits, final int min, final int max) {
            m_bits = bits;
            m_min = min;
            m_max = max;
        }

        @Override
        public void writeNonNullValue(final RecordConsumer c, final Long v) throws Exception {
            if (v < m_min || v > m_max) {
                throw new BigDataFileFormatException(String.format( //
                    "Cannot write %d as signed %d bit integer, only values between %d and %d are supported.", //
                    (long)v, m_bits, m_min, m_max));
            } else {
                c.addInteger(Math.toIntExact(v));
            }
        }
    }

    ////////////////////////////////// Integer (unsigned) //////////////////////////////////
    static class IntToUnsignedIntOrLongConsumer implements ParquetCellValueConsumer<Integer> {
        private final int m_bits;
        private final long m_max;

        IntToUnsignedIntOrLongConsumer(final int bits, final long max) {
            m_bits = bits;
            m_max = max;
        }

        IntToUnsignedIntOrLongConsumer(final int bits) {
            m_bits = bits;
            m_max = -1;
        }

        @Override
        public void writeNonNullValue(final RecordConsumer c, final Integer v) throws Exception {
            if (v < 0 && m_max == -1) {
                throw new BigDataFileFormatException(String.format( //
                    "Cannot write %d as unsigned %d bit integer, only values >= 0 are supported.", //
                    (int)v, m_bits));
            } else if (v < 0 || (m_max != -1 && v > m_max)) {
                throw new BigDataFileFormatException(String.format( //
                    "Cannot write %d as unsigned %d bit integer, only values between 0 and %d are supported.", //
                    (int)v, m_bits, m_max));
            } else if (m_bits <= 32) {
                c.addInteger((int)Integer.toUnsignedLong(v));
            } else {
                c.addLong(Integer.toUnsignedLong(v));
            }
        }
    }

    static class LongToUnsignedIntConsumer implements ParquetCellValueConsumer<Long> {
        private final int m_bits;
        private final long m_max;

        LongToUnsignedIntConsumer(final int bits, final long max) {
            m_bits = bits;
            m_max = max;
        }

        @Override
        public void writeNonNullValue(final RecordConsumer c, final Long v) throws Exception {
            if (v < 0 || v > m_max) {
                throw new BigDataFileFormatException(String.format( //
                    "Cannot write %d as unsigned %d bit integer, only values between 0 and %d are supported.", //
                    (long)v, m_bits, m_max));
            } else {
                c.addInteger((int)(long)(v));
            }
        }
    }

    static class LongToUnsignedLongConsumer implements ParquetCellValueConsumer<Long> {

        @Override
        public void writeNonNullValue(final RecordConsumer c, final Long v) throws Exception {
            if (v < 0) {
                throw new BigDataFileFormatException(String.format( //
                    "Cannot write %d as unsigned %d bit integer, only values >= 0 are supported.", //
                    (long)v, 64));
            } else {
                c.addLong(v);
            }
        }
    }

    ////////////////////////////////// LocalDateTime //////////////////////////////////

    static class LocalDateTimeAsInt64MillisConsumer implements ParquetCellValueConsumer<LocalDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final LocalDateTime v) throws Exception {
            c.addLong(v.atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
        }
    }

    static class LocalDateTimeAsInt64MicrosConsumer implements ParquetCellValueConsumer<LocalDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final LocalDateTime v) throws Exception {
            final var instant = v.atZone(ZoneOffset.UTC).toInstant();
            final long seconds = TimeUnit.MICROSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS);
            final long microSeconds = TimeUnit.MICROSECONDS.convert(instant.getNano(), TimeUnit.NANOSECONDS);

            if (seconds == Long.MIN_VALUE || seconds == Long.MAX_VALUE) {
                throw new BigDataFileFormatException(String.format(
                    "Cannot write %s as timestamp with microsecond precision, given time to small or to large.",
                    v.toString()));
            } else {
                c.addLong(seconds + microSeconds);
            }
        }
    }

    static class LocalDateTimeAsInt64NanosConsumer implements ParquetCellValueConsumer<LocalDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final LocalDateTime v) throws Exception {
            final var instant = v.atZone(ZoneOffset.UTC).toInstant();
            final long seconds = TimeUnit.NANOSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS);
            final long nanoSeconds = instant.getNano();

            if (seconds == Long.MIN_VALUE || seconds == Long.MAX_VALUE) {
                throw new BigDataFileFormatException(String.format(
                    "Cannot write %s as timestamp with nanosecond precision, given time to small or to large.",
                    v.toString()));
            } else {
                c.addLong(seconds + nanoSeconds);
            }
        }
    }

    static class LocalDateTimeAsInt96Consumer implements ParquetCellValueConsumer<LocalDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final LocalDateTime v) throws Exception {
            final var buffer = new byte[12];
            final var byteBuffer = ByteBuffer.wrap(buffer);
            final ZonedDateTime dateTime = v.atZone(ZoneOffset.UTC);
            final long timeOfDayNanos = dateTime.toLocalTime().toNanoOfDay();
            final int julianDay = (int)JulianFields.JULIAN_DAY.getFrom(dateTime.toLocalDate());
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay);
            c.addBinary(Binary.fromConstantByteArray(buffer));
        }
    }

    ////////////////////////////////// ZonedDateTime (adjusted to UTC) //////////////////////////////////
    static class ZonedDateTimeAsInt64MillisConsumerUTC implements ParquetCellValueConsumer<ZonedDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final ZonedDateTime v) throws Exception {
            c.addLong(v.toInstant().toEpochMilli());
        }
    }

    static class ZonedDateTimeAsInt64MicrosConsumerUTC implements ParquetCellValueConsumer<ZonedDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final ZonedDateTime v) throws Exception {
            final var instant = v.toInstant();
            final long seconds = TimeUnit.MICROSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS);
            final long microSeconds = TimeUnit.MICROSECONDS.convert(instant.getNano(), TimeUnit.NANOSECONDS);

            if (seconds == Long.MIN_VALUE || seconds == Long.MAX_VALUE) {
                throw new BigDataFileFormatException(String.format(
                    "Cannot write %s as timestamp with microsecond precision, given time to small or to large.",
                    v.toString()));
            } else {
                c.addLong(seconds + microSeconds);
            }
        }
    }

    static class ZonedDateTimeAsInt64NanosConsumerUTC implements ParquetCellValueConsumer<ZonedDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final ZonedDateTime v) throws Exception {
            final var instant = v.toInstant();
            final long seconds = TimeUnit.NANOSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS);
            final long nanoSeconds = instant.getNano();

            if (seconds == Long.MIN_VALUE || seconds == Long.MAX_VALUE) {
                throw new BigDataFileFormatException(String.format(
                    "Cannot write %s as timestamp with nanosecond precision, given time to small or to large.",
                    v.toString()));
            } else {
                c.addLong(seconds + nanoSeconds);
            }
        }
    }

    ////////////////////////////////// ZonedDateTime (not adjusted to UTC) //////////////////////////////////
    static class ZonedDateTimeAsInt64MillisConsumerNoUTC implements ParquetCellValueConsumer<ZonedDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final ZonedDateTime v) throws Exception {
            final var instant = v.toLocalDateTime().atZone(ZoneOffset.UTC).toInstant();
            c.addLong(instant.toEpochMilli());
        }
    }

    static class ZonedDateTimeAsInt64MicrosConsumerNoUTC implements ParquetCellValueConsumer<ZonedDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final ZonedDateTime v) throws Exception {
            final var instant = v.toLocalDateTime().atZone(ZoneOffset.UTC).toInstant();
            final long seconds = TimeUnit.MICROSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS);
            final long microSeconds = TimeUnit.MICROSECONDS.convert(instant.getNano(), TimeUnit.NANOSECONDS);

            if (seconds == Long.MIN_VALUE || seconds == Long.MAX_VALUE) {
                throw new BigDataFileFormatException(String.format(
                    "Cannot write %s as timestamp with microsecond precision, given time to small or to large.",
                    v.toString()));
            } else {
                c.addLong(seconds + microSeconds);
            }
        }
    }

    static class ZonedDateTimeAsInt64NanosConsumerNoUTC implements ParquetCellValueConsumer<ZonedDateTime> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final ZonedDateTime v) throws Exception {
            final var instant = v.toLocalDateTime().atZone(ZoneOffset.UTC).toInstant();
            final long seconds = TimeUnit.NANOSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS);
            final long nanoSeconds = instant.getNano();

            if (seconds == Long.MIN_VALUE || seconds == Long.MAX_VALUE) {
                throw new BigDataFileFormatException(String.format(
                    "Cannot write %s as timestamp with nanosecond precision, given time to small or to large.",
                    v.toString()));
            } else {
                c.addLong(seconds + nanoSeconds);
            }
        }
    }

    ////////////////////////////////// UUID //////////////////////////////////
    static class UUIDasFixedLenByteArray implements ParquetCellValueConsumer<String> {
        @Override
        public void writeNonNullValue(final RecordConsumer c, final String v) throws Exception {
            try {
                final var uuid = UUID.fromString(v);
                final var bb = ByteBuffer.allocate(2 * Long.BYTES);
                bb.putLong(uuid.getMostSignificantBits());
                bb.putLong(uuid.getLeastSignificantBits());
                c.addBinary(Binary.fromConstantByteArray(bb.array()));
            } catch (final IllegalArgumentException e) {
                throw new BigDataFileFormatException(
                    String.format("Cannot write '%s' as FIXED_LEN_BYTE_ARRAY based UUID: %s", v, e.getMessage()), e);
            }
        }
    }
}
