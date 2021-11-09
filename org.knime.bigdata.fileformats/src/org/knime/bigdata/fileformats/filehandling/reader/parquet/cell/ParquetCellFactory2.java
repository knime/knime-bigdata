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
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.ComposedParquetCell.Builder;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.BinaryContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.BooleanContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.DoubleContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.FloatContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.IntContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.LongContainer;

/**
 * Static factory class for {@link ParquetCell} instances.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class ParquetCellFactory2 {

    private ParquetCellFactory2() {
        // no instance
    }

    /**
     * Create a decimal cell using an integer container.
     * @param precision decimal precision
     * @param scale decimal scale
     * @param typeName primitive type name
     * @return cell to access decimal values
     */
    public static ComposedParquetCell<IntContainer> configureForIntDecimal(final int precision, final int scale,
        final PrimitiveTypeName typeName) {

        final Builder<IntContainer> builder = ComposedParquetCell.builder(new IntContainer());
        final var da = new DecimalAccess(precision, scale, typeName, 9);
        if (scale == 0) {
            builder.withIntAccess(da::getInt);
            builder.withLongAccess(da::getLong);
        }
        return builder//
            .withDoubleAccess(da::getDouble)//
            .withObjAccess(String.class, da::getString)//
            .build();
    }

    /**
     * Create a decimal cell using an long container.
     * @param precision decimal precision
     * @param scale decimal scale
     * @param typeName primitive type name
     * @return cell to access decimal values
     */
    public static ComposedParquetCell<LongContainer> configureForLongDecimal(final int precision, final int scale,
        final PrimitiveTypeName typeName) {

        final Builder<LongContainer> builder = ComposedParquetCell.builder(new LongContainer());
        final var da = new DecimalAccess(precision, scale, typeName, 18);
        if (scale == 0) {
            builder.withLongAccess(da::getLong);
        }
        return builder.withDoubleAccess(da::getDouble)//
            .withObjAccess(String.class, da::getString)//
            .build();
    }

    /**
     * Create a decimal cell using an binary container.
     * @param precision decimal precision
     * @param scale decimal scale
     * @return cell to access decimal values
     */
    public static ComposedParquetCell<BinaryContainer> configureForBinaryDecimal(final int precision, final int scale) {
        final Builder<BinaryContainer> builder = ComposedParquetCell.builder(new BinaryContainer());
        final DecimalAccess da = new DecimalAccess(precision, scale);
        if (scale == 0 && precision <= 9) {
            builder.withIntAccess(da::getInt);
        }
        if (scale == 0 && precision <= 18) {
            builder.withLongAccess(da::getLong);
        }
        return builder//
            .withDoubleAccess(da::getDouble)//
            .withObjAccess(String.class, da::getString)//
            .build();
    }

    /**
     * Create a integer cell using a signed integer container.
     * @return cell to access integer values
     */
    public static ComposedParquetCell<IntContainer> createForInt() {
        return ComposedParquetCell.builder(new IntContainer())//
                .withIntAccess(Accesses::getInt)//
                .withLongAccess(Accesses::getLong)//
                .withDoubleAccess(Accesses::getDouble)//
                .withObjAccess(String.class, Accesses::getString)//
                .build();
    }

    /**
     * Create a integer cell using a unsigned integer container.
     * @return cell to access integer values
     */
    public static ComposedParquetCell<IntContainer> createForUnsignedInt32() {
        return ComposedParquetCell.builder(new IntContainer())//
            .withLongAccess(Accesses::getUInt32)//
            .withDoubleAccess(Accesses::getUInt32AsDouble)//
            .withObjAccess(String.class, Accesses::getUInt32AsString)//
            .build();
    }

    /**
     * Create a long cell using a signed long container.
     * @return cell to access long values
     */
    public static ComposedParquetCell<LongContainer> createForLong() {
        return ComposedParquetCell.builder(new LongContainer())//
            .withLongAccess(Accesses::getLong)//
            .withDoubleAccess(Accesses::getDouble)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    /**
     * Create a long cell using an unsigned long container.
     * @return cell to access long values
     */
    public static ComposedParquetCell<LongContainer> createForUnsignedInt64() {
        return ComposedParquetCell.builder(new LongContainer())//
            .withObjAccess(String.class, Accesses::getUnsignedLongString)//
            .build();
    }

    /**
     * Create a date & time cell using an 96bit integer container.
     * @return cell to access date & time values
     */
    public static ComposedParquetCell<BinaryContainer> createForSignedInt96() {
        return ComposedParquetCell.builder(new BinaryContainer())//
            .withObjAccess(LocalDateTime.class, Accesses::getInt96LocalDateTime)//
            .withObjAccess(ZonedDateTime.class, Accesses::getInt96ZonedDateTime)//
            .withObjAccess(LocalDate.class, Accesses::getInt96LocalDate)//
            .withObjAccess(String.class, Accesses::getInt96ZonedDateTimeString)//
            .build();
    }

    /**
     * Create a float cell.
     * @return cell to access float values
     */
    public static ComposedParquetCell<FloatContainer> createForFloat() {
        return ComposedParquetCell.builder(new FloatContainer())//
            .withDoubleAccess(Accesses::getDouble)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    /**
     * Create a double cell.
     * @return cell to access double values
     */
    public static ComposedParquetCell<DoubleContainer> createForDouble() {
        return ComposedParquetCell.builder(new DoubleContainer())//
            .withDoubleAccess(Accesses::getDouble)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    /**
     * Create a boolean cell.
     * @return cell to access boolean values
     */
    public static ComposedParquetCell<BooleanContainer> createForBoolean() {
        return ComposedParquetCell.builder(new BooleanContainer())//
            .withBooleanAccess(Accesses::getBoolean)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    /**
     * Create a date cell.
     * @return cell to access date values
     */
    public static ComposedParquetCell<IntContainer> createForDate() {
        return ComposedParquetCell.builder(new IntContainer())//
            .withObjAccess(LocalDate.class, Accesses::getLocalDateOfEpochDay)//
            .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTimeOfEpochDay)//
            .withObjAccess(String.class, Accesses::getLocalDateStringOfEpochDay)//
            .build();
    }

    /**
     * Create a time cell using milliseconds integer container.
     * @return cell to access time values
     */
    public static ComposedParquetCell<IntContainer> createForTimeMillis() {
        return ComposedParquetCell.builder(new IntContainer())//
            .withObjAccess(LocalTime.class, Accesses::getLocalTimeOfMillisOfDay)//
            .withObjAccess(String.class, Accesses::getLocalTimeStringOfMillisOfDay)//
            .build();
    }

    /**
     * Create a time cell using microseconds long container.
     * @return cell to access time values
     */
    public static ComposedParquetCell<LongContainer> createForTimeMicros() {
        return ComposedParquetCell.builder(new LongContainer())//
            .withObjAccess(LocalTime.class, Accesses::getLocalTimeOfMicrosDay)//
            .withObjAccess(String.class, Accesses::getLocalTimeStringOfMicrosDay)//
            .build();
    }

    /**
     * Create a time cell using nanoseconds long container.
     * @return cell to access time values
     */
    public static ComposedParquetCell<LongContainer> createForTimeNanos() {
        return ComposedParquetCell.builder(new LongContainer())//
            .withObjAccess(LocalTime.class, Accesses::getLocalTimeOfNanosDay)//
            .withObjAccess(String.class, Accesses::getLocalTimeStringOfNanosDay)//
            .build();
    }

    /**
     * Create a time & date cell using milliseconds long container.
     * @param isAdjustedToUTC {@code true} if normalized to UTC, {@code false} on local date and time
     * @return cell to access time values
     */
    public static ComposedParquetCell<LongContainer> createForTimestampMillis(final boolean isAdjustedToUTC) {
        final Builder<LongContainer> builder = ComposedParquetCell.builder(new LongContainer())//
            .withObjAccess(ZonedDateTime.class, Accesses::getZonedDateTimeOfEpochMillis)//
            .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTimeOfEpochMillis)//
            .withObjAccess(LocalDate.class, Accesses::getLocalDateOfEpochMillis);

        if (isAdjustedToUTC) {
            builder.withObjAccess(String.class, Accesses::getZonedDateTimeStringEpochMillis);
        } else {
            builder.withObjAccess(String.class, Accesses::getLocalDateTimeStringOfEpochMillis);
        }

        return builder.build();
    }

    /**
     * Create a time & date cell using microseconds long container.
     * @param isAdjustedToUTC {@code true} if normalized to UTC, {@code false} on local date and time
     * @return cell to access time values
     */
    public static ComposedParquetCell<LongContainer> createForTimestampMicros(final boolean isAdjustedToUTC) {
        final Builder<LongContainer> builder = ComposedParquetCell.builder(new LongContainer())//
                .withObjAccess(ZonedDateTime.class, Accesses::getZonedDateTimeOfEpochMicros)//
                .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTimeOfEpochMicros)//
                .withObjAccess(LocalDate.class, Accesses::getLocalDateOfEpochMicros);

        if (isAdjustedToUTC) {
            builder.withObjAccess(String.class, Accesses::getZonedDateTimeStringOfEpochMicros);
        } else {
            builder.withObjAccess(String.class, Accesses::getLocalDateTimeStringOfEpochMicros);
        }

        return builder.build();
    }

    /**
     * Create a time & date cell using nanoseconds long container.
     * @param isAdjustedToUTC {@code true} if normalized to UTC, {@code false} on local date and time
     * @return cell to access time values
     */
    public static ComposedParquetCell<LongContainer> createForTimestampNanos(final boolean isAdjustedToUTC) {
        final Builder<LongContainer> builder = ComposedParquetCell.builder(new LongContainer())//
                .withObjAccess(ZonedDateTime.class, Accesses::getZonedDateTimeOfEpochNanos)//
                .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTimeOfEpochNanos)//
                .withObjAccess(LocalDate.class, Accesses::getLocalDateOfEpochNanos);

        if (isAdjustedToUTC) {
            builder.withObjAccess(String.class, Accesses::getZonedDateTimeStringOfEpochNanos);
        } else {
            builder.withObjAccess(String.class, Accesses::getLocalDateTimeStringOfEpochNanos);
        }

        return builder.build();
    }

    /**
     * Create a binary cell.
     * @return cell to access binary values
     */
    public static ComposedParquetCell<BinaryContainer> createForBinary() {
        return ComposedParquetCell.builder(new BinaryContainer()) //
            .withObjAccess(InputStream.class, Accesses::getInputStream)//
            .build();
    }

    /**
     * Create a string cell.
     * @return cell to access string values
     */
    public static ComposedParquetCell<BinaryContainer> createForString() {
        return ComposedParquetCell.builder(new BinaryContainer()) //
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    /**
     * Create a UUID cell.
     * @return cell to access UUIDs
     */
    public static ComposedParquetCell<BinaryContainer> createForUuid() {
        return ComposedParquetCell.builder(new BinaryContainer()) //
            .withObjAccess(String.class, Accesses::getUuidString)//
            .build();
    }

    /**
     * Create a repeated group converter with given cell producers.
     * @param elementCells cells of the converter
     * @return group converter
     */
    public static RepeatedGroupParquetConverter createRepeatedGroupColumn(final ParquetCell[] elementCells) {
        return new RepeatedGroupParquetConverter(//
            Arrays.stream(elementCells)//
                .map(RepeatedGroupParquetCell::new)//
                .toArray(RepeatedGroupParquetCell[]::new));
    }

    /**
     * Get cells from a repeated group converter.
     * @param groupColumn converter to get cells from
     * @return cell producers of given converter
     */
    public static BigDataCell[] getBigDataCells(final RepeatedGroupParquetConverter groupColumn) {
        return groupColumn.getBigDataCells();
    }

    /**
     * Create a legacy list cell.
     * @param elementCell list element cells
     * @return cell to access list
     */
    public static ParquetCell createListLegacyCell(final ParquetCell elementCell) {
        return new ListParquetLegacyCell(elementCell);
    }
}
