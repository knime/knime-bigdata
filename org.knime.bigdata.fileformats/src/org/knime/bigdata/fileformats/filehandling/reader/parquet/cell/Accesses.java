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
 *   Nov 13, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

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

import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.BinaryContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.BooleanContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.DoubleContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.FloatContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.IntContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.LongContainer;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class Accesses {

    private static final ZoneId UTC = ZoneId.of("Etc/UTC");

    private Accesses() {
        // static class
    }

    interface ObjAccess<C, T> {
        T getObj(C container);
    }

    interface BooleanAccess<C> extends ObjAccess<C, Boolean> {
        boolean getBoolean(C container);

        @Override
        default Boolean getObj(final C container) {
            return getBoolean(container);
        }
    }

    interface IntAccess<C> extends ObjAccess<C, Integer> {

        int getInt(C container);

        @Override
        default Integer getObj(final C container) {
            return getInt(container);
        }
    }

    interface LongAccess<C> extends ObjAccess<C, Long> {

        long getLong(C container);

        @Override
        default Long getObj(final C container) {
            return getLong(container);
        }
    }

    interface DoubleAccess<C> extends ObjAccess<C, Double> {

        double getDouble(C container);

        @Override
        default Double getObj(final C container) {
            return getDouble(container);
        }
    }

    // Accesses for BooleanContainer

    static boolean getBoolean(final BooleanContainer container) {
        return container.getBoolean();
    }

    static String getString(final BooleanContainer container) {
        return Boolean.toString(container.getBoolean());
    }

    // Accesses for IntContainer

    static int getInt(final IntContainer container) {
        return container.getInt();
    }

    static long getLong(final IntContainer container) {
        return container.getInt();
    }

    static long getUInt32(final IntContainer container) {
        return Integer.toUnsignedLong(container.getInt());
    }

    static double getUInt32AsDouble(final IntContainer container) {
        return getUInt32(container);
    }

    static String getUInt32AsString(final IntContainer container) {
        return Long.toString(getUInt32(container));
    }

    static double getDouble(final IntContainer container) {
        return container.getInt();
    }

    static String getString(final IntContainer container) {
        return Integer.toString(container.getInt());
    }

    static LocalDate getLocalDateOfEpochDay(final IntContainer container) {
        return LocalDate.ofEpochDay(container.getInt());
    }

    static LocalDateTime getLocalDateTimeOfEpochDay(final IntContainer container) {
        return getLocalDateOfEpochDay(container).atStartOfDay();
    }

    static String getLocalDateStringOfEpochDay(final IntContainer container) {
        return getLocalDateOfEpochDay(container).toString();
    }

    static LocalTime getLocalTimeOfMillisOfDay(final IntContainer container) {
        return LocalTime.ofNanoOfDay(TimeUnit.NANOSECONDS.convert(container.getInt(), TimeUnit.MILLISECONDS));
    }

    static String getLocalTimeStringOfMillisOfDay(final IntContainer container) {
        return getLocalTimeOfMillisOfDay(container).toString();
    }

    // Accesses for LongContainer

    static long getLong(final LongContainer container) {
        return container.getLong();
    }

    static double getDouble(final LongContainer container) {
        return container.getLong();
    }

    static String getString(final LongContainer container) {
        return Long.toString(container.getLong());
    }

    // ZonedDateTime from epoch milliseconds

    static ZonedDateTime getZonedDateTimeOfEpochMillis(final LongContainer container) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(container.getLong()), UTC);
    }

    static String getZonedDateTimeStringEpochMillis(final LongContainer container) {
        return getZonedDateTimeOfEpochMillis(container).toString();
    }

    // ZonedDateTime from epoch microseconds

    static ZonedDateTime getZonedDateTimeOfEpochMicros(final LongContainer container) {
        return ZonedDateTime.ofInstant(getInstantMicros(container), UTC);
    }

    static String getZonedDateTimeStringOfEpochMicros(final LongContainer container) {
        return getZonedDateTimeOfEpochMicros(container).toString();
    }

    // ZonedDateTime from epoch nanoseconds

    static ZonedDateTime getZonedDateTimeOfEpochNanos(final LongContainer container) {
        return ZonedDateTime.ofInstant(getInstantNanos(container), UTC);
    }

    static String getZonedDateTimeStringOfEpochNanos(final LongContainer container) {
        return getZonedDateTimeOfEpochNanos(container).toString();
    }

    // LocalDate... from epoch milliseconds

    static LocalDateTime getLocalDateTimeOfEpochMillis(final LongContainer container) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(container.getLong()), ZoneOffset.UTC);
    }

    static String getLocalDateTimeStringOfEpochMillis(final LongContainer container) {
        return getLocalDateTimeOfEpochMillis(container).toString();
    }

    static LocalDate getLocalDateOfEpochMillis(final LongContainer container) {
        return getLocalDateTimeOfEpochMillis(container).toLocalDate();
    }

    // LocalDate... from epoch microseconds

    static LocalDateTime getLocalDateTimeOfEpochMicros(final LongContainer container) {
        return LocalDateTime.ofInstant(getInstantMicros(container), ZoneOffset.UTC);
    }

    static String getLocalDateTimeStringOfEpochMicros(final LongContainer container) {
        return getLocalDateTimeOfEpochMicros(container).toString();
    }

    static LocalDate getLocalDateOfEpochMicros(final LongContainer container) {
        return getLocalDateTimeOfEpochMicros(container).toLocalDate();
    }

    // LocalDate... from epoch microseconds

    static LocalDateTime getLocalDateTimeOfEpochNanos(final LongContainer container) {
        return LocalDateTime.ofInstant(getInstantNanos(container), ZoneOffset.UTC);
    }

    static String getLocalDateTimeStringOfEpochNanos(final LongContainer container) {
        return getLocalDateTimeOfEpochNanos(container).toString();
    }

    static LocalDate getLocalDateOfEpochNanos(final LongContainer container) {
        return getLocalDateTimeOfEpochNanos(container).toLocalDate();
    }

    private static Instant getInstantMicros(final LongContainer container) {
        final long value = container.getLong();
        final long seconds = TimeUnit.SECONDS.convert(value, TimeUnit.MICROSECONDS);
        final long nanos = (value % (1000 * 1000l)) * 1000l;
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private static Instant getInstantNanos(final LongContainer container) {
        final long value = container.getLong();
        final long seconds = TimeUnit.SECONDS.convert(value, TimeUnit.NANOSECONDS);
        final long nanos = (value % (1000 * 1000l * 1000l));
        return Instant.ofEpochSecond(seconds, nanos);
    }

    // LocalTime from microseconds of day

    static LocalTime getLocalTimeOfMicrosDay(final LongContainer container) {
        return LocalTime.ofNanoOfDay(TimeUnit.NANOSECONDS.convert(container.getLong(), TimeUnit.MICROSECONDS));
    }

    static String getLocalTimeStringOfMicrosDay(final LongContainer container) {
        return getLocalTimeOfMicrosDay(container).toString();
    }

    // LocalTime from nanoseconds of day

    static LocalTime getLocalTimeOfNanosDay(final LongContainer container) {
        return LocalTime.ofNanoOfDay(container.getLong());
    }

    static String getLocalTimeStringOfNanosDay(final LongContainer container) {
        return getLocalTimeOfNanosDay(container).toString();
    }

    static String getUnsignedLongString(final LongContainer container) {
        return Long.toUnsignedString(container.getLong());
    }

    // Accesses for DoubleContainer

    static double getDouble(final DoubleContainer container) {
        return container.getDouble();
    }

    static String getString(final DoubleContainer container) {
        return Double.toString(container.getDouble());
    }

    // Accesses for FloatContainer

    static double getDouble(final FloatContainer container) {
        return container.getFloat();
    }

    static String getString(final FloatContainer container) {
        return Float.toString(container.getFloat());
    }

    // Accesses for BinaryContainer

    static String getString(final BinaryContainer container) {
        return container.getBinary().toStringUsingUTF8();
    }

    static String getUuidString(final BinaryContainer container) {
        final byte[] bytes = container.getBinary().getBytes();
        if (bytes.length == 16) {
            return String.format("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", //
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], //
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
        } else {
            return "invalid-uuid";
        }
    }

    static InputStream getInputStream(final BinaryContainer container) {
        return new ByteArrayInputStream(container.getBinary().getBytes());
    }

    static ZonedDateTime getInt96ZonedDateTime(final BinaryContainer container) {
        return getInt96LocalDateTime(container).atZone(UTC);
    }

    static LocalDateTime getInt96LocalDateTime(final BinaryContainer container) {
        final ByteBuffer buf = container.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        final LocalTime localTime = LocalTime.ofNanoOfDay(buf.getLong());
        final LocalDate localDate = LocalDate.MIN.with(JulianFields.JULIAN_DAY, buf.getInt());
        return LocalDateTime.of(localDate, localTime);
    }

    static LocalDate getInt96LocalDate(final BinaryContainer container) {
        return getInt96LocalDateTime(container).toLocalDate();
    }

    static String getInt96ZonedDateTimeString(final BinaryContainer container) {
        return getInt96ZonedDateTime(container).toString();
    }

}
