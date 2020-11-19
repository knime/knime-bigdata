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
 *   Nov 6, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.orc.cell;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;

/**
 * Provides various value access interfaces and corresponding implementations.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class Accesses {

    private Accesses() {
        // static utility class
    }

    private static final ZoneId UTC = ZoneId.of("Etc/UTC");

    @FunctionalInterface
    interface ObjColumnAccess<C, T> {
        T getObj(C colVector, int idx);
    }

    @FunctionalInterface
    interface DoubleColumnAccess<C> extends ObjColumnAccess<C, Double> {
        double getDouble(C colVector, int idx);

        @Override
        default Double getObj(final C colVector, final int idx) {
            return getDouble(colVector, idx);
        }
    }

    @FunctionalInterface
    interface IntColumnAccess<C> extends ObjColumnAccess<C, Integer> {
        int getInt(C colVector, int idx);

        @Override
        default Integer getObj(final C colVector, final int idx) {
            return getInt(colVector, idx);
        }
    }

    @FunctionalInterface
    interface LongColumnAccess<C> extends ObjColumnAccess<C, Long> {
        long getLong(C colVector, int idx);

        @Override
        default Long getObj(final C colVector, final int idx) {
            return getLong(colVector, idx);
        }
    }

    @FunctionalInterface
    interface BooleanColumnAccess<C> extends ObjColumnAccess<C, Boolean> {
        boolean getBoolean(C colVector, int idx);

        @Override
        default Boolean getObj(final C colVector, final int idx) {
            return getBoolean(colVector, idx);
        }
    }

    // DecimalColumnVector accesses

    static double getDouble(final DecimalColumnVector vector, final int idx) {
        return vector.vector[idx].doubleValue();
    }

    static String getString(final DecimalColumnVector vector, final int idx) {
        return Double.toString(getDouble(vector, idx));
    }

    // DoubleColumnVector accesses

    static String getString(final DoubleColumnVector vector, final int idx) {
        return Double.toString(getDouble(vector, idx));
    }

    static double getDouble(final DoubleColumnVector vector, final int idx) {
        return vector.vector[idx];
    }

    // LongColumnVector accesses

    static double getDouble(final LongColumnVector vector, final int idx) {
        return vector.vector[idx];
    }

    static int getInt(final LongColumnVector vector, final int idx) {
        final long valueL = getLong(vector, idx);
        final int valueI = (int)valueL;
        if (valueI != valueL) {
            throw new BigDataFileFormatException(
                String.format("Written as int but read as a long (overflow): (long)%d != (int)%d", valueL, valueI));
        }
        return valueI;
    }

    static long getLong(final LongColumnVector vector, final int idx) {
        return vector.vector[idx];
    }

    static String getString(final LongColumnVector vector, final int idx) {
        return Long.toString(getLong(vector, idx));
    }

    static boolean getBoolean(final LongColumnVector vector, final int idx) {
        return vector.vector[idx] == 1L;
    }

    static String getStringBoolean(final LongColumnVector vector, final int idx) {
        return Boolean.toString(getBoolean(vector, idx));
    }

    static String getLocalDateString(final LongColumnVector vector, final int idx) {
        return getLocalDate(vector, idx).toString();
    }

    static LocalDate getLocalDate(final LongColumnVector vector, final int idx) {
        return LocalDate.ofEpochDay(vector.vector[idx]);
    }

    static LocalDateTime getLocalDateTime(final LongColumnVector vector, final int idx) {
        return getLocalDate(vector, idx).atStartOfDay();
    }

    // TimestampColumnVector accesses

    static LocalDateTime getLocalDateTime(final TimestampColumnVector vector, final int idx) {
        return vector.asScratchTimestamp(idx).toLocalDateTime();
    }

    static ZonedDateTime getZonedDateTime(final TimestampColumnVector vector, final int idx) {
        return ZonedDateTime.ofInstant(vector.asScratchTimestamp(idx).toInstant(), UTC);
    }

    static String getZonedDateTimeString(final TimestampColumnVector vector, final int idx) {
        return getZonedDateTime(vector, idx).toString();
    }

    // BytesColumnVector accesses

    static String getString(final BytesColumnVector vector, final int idx) {
        return new String(vector.vector[idx], vector.start[idx], vector.length[idx], StandardCharsets.UTF_8);
    }

    static String getBinaryString(final BytesColumnVector vector, final int idx) {
        return Arrays.toString(vector.vector[idx]);
    }

    static InputStream getInputStream(final BytesColumnVector vector, final int idx) {
        return new ByteArrayInputStream(vector.vector[idx], vector.start[idx], vector.length[idx]);
    }
}
