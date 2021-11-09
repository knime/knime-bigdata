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
 *   Oct 12, 2021 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import java.util.ArrayList;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.ParquetCell;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.ParquetCellFactory2;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.ListKnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderColumnSpec;

/**
 * Factory class for Parquet column instances for Parquet types.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
final class ParquetTypeHelper {

    private ParquetTypeHelper() {
        // static factory class
    }

    /**
     * Creates the matching Parquet column for the provided {@link Type}.
     *
     * @param type {@link Type} to create the Parquet column} for
     * @param failOnUnsupportedColumnType {@code true} if exception should be raised on unsupported column types
     * @return a Parquet column
     * @throws BigDataFileFormatException on unsupported column types
     */
    public static ParquetColumn getParquetColumn(final Type type, final boolean failOnUnsupportedColumnType) {
        final var column = getParquetColumn(type);
        final var error = column.getErrorMessage();

        if (error != null) {
            final var extendedError =
                String.format("Column '%s' contains unsupported structure or type: %s", type.getName(), error);
            if (failOnUnsupportedColumnType) {
                throw new BigDataFileFormatException(extendedError);
            } else {
                return new UnsupportedParquetColumn(extendedError);
            }
        } else {
            return column;
        }
    }

    /**
     * Validates if the the provided {@link Type} is supported.
     *
     * @param type {@link Type} to validate
     * @return {@code true} if type is supported, {@code false} otherwise
     */
    public static boolean isSupportedType(final Type type) {
        return !getParquetColumn(type, false).skipColumn();
    }

    static ParquetColumn fail(final String error) {
        return new UnsupportedParquetColumn(error);
    }

    private static ParquetColumn getParquetColumn(final Type type) {
        if (type.getLogicalTypeAnnotation() != null) {
            return fromLogicalType(type, type.getLogicalTypeAnnotation());
        } else if (type.isPrimitive()) {
            return fromPrimitiveType(type.asPrimitiveType());
        } else {
            return fail("Group without logical type annotation / original type");
        }
    }

    private static ParquetColumn fromLogicalType(final Type type, final LogicalTypeAnnotation logicalType) { // NOSONAR
        if (logicalType instanceof DecimalLogicalTypeAnnotation) {
            return createForDecimal(type, logicalType);
        } else if (logicalType instanceof IntLogicalTypeAnnotation) {
            final var intType = (IntLogicalTypeAnnotation) logicalType;
            if (intType.isSigned()) {
                return createForSignedInt(type, intType.getBitWidth());
            } else {
                return createForUnsignedInt(type, intType.getBitWidth());
            }
        } else if (logicalType instanceof DateLogicalTypeAnnotation) {
            return createForDate(type);
        } else if (logicalType instanceof TimeLogicalTypeAnnotation) {
            return createForTime(type);
        } else if (logicalType instanceof TimestampLogicalTypeAnnotation) {
            return createForTimestamp(type);
        } else if (logicalType instanceof StringLogicalTypeAnnotation) {
            return createForString(type);
        } else if (logicalType instanceof JsonLogicalTypeAnnotation) {
            return createForString(type);
        } else if (logicalType instanceof EnumLogicalTypeAnnotation) {
            return createForString(type);
        } else if (logicalType instanceof UUIDLogicalTypeAnnotation) {
            return createForUuid(type);
        } else if (logicalType instanceof ListLogicalTypeAnnotation) {
            return listFromGroupType(type.asGroupType());
        } else if (logicalType instanceof MapLogicalTypeAnnotation) {
            return mapFromGroupType(type.asGroupType());
        } else {
            return fail(logicalType.toString());
        }
    }

    private static ParquetColumn fromPrimitiveType(final PrimitiveType primitiveType) { // NOSONAR
        switch (primitiveType.getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return createForBinary(primitiveType);
            case BOOLEAN:
                return createForBoolean(primitiveType);
            case FLOAT:
                return createForFloat(primitiveType);
            case DOUBLE:
                return createForDouble(primitiveType);
            case INT32:
                return createForSignedInt(primitiveType, 32);
            case INT64:
                return createForSignedInt(primitiveType, 64);
            case INT96:
                return createForSignedInt96(primitiveType);
            default:
                return fail("Unsupported primitive type");
        }
    }

    private static ParquetColumn createForDecimal(final Type type, final LogicalTypeAnnotation logicalType) { // NOSONAR
        final var decimalType = (DecimalLogicalTypeAnnotation) logicalType;
        final var precision = decimalType.getPrecision();
        final var scale = decimalType.getScale();
        final var primitiveTypeName = type.asPrimitiveType().getPrimitiveTypeName();
        final PrimitiveKnimeType knimeType;
        final ParquetCell cell;

        if (primitiveTypeName == PrimitiveTypeName.INT32) {
            knimeType = scale == 0 ? PrimitiveKnimeType.INTEGER : PrimitiveKnimeType.DOUBLE;
            cell = ParquetCellFactory2.configureForIntDecimal(precision, scale, primitiveTypeName);

        } else if (primitiveTypeName == PrimitiveTypeName.INT64) {
            knimeType = scale == 0 ? PrimitiveKnimeType.LONG : PrimitiveKnimeType.DOUBLE;
            cell = ParquetCellFactory2.configureForLongDecimal(precision, scale, primitiveTypeName);

        } else if (primitiveTypeName == PrimitiveTypeName.BINARY || primitiveTypeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            if (scale == 0 && precision <= 9) {
                knimeType = PrimitiveKnimeType.INTEGER;
            } else if (scale == 0 && precision <= 18) {
                knimeType = PrimitiveKnimeType.LONG;
            } else {
                knimeType = PrimitiveKnimeType.DOUBLE;
            }
            cell = ParquetCellFactory2.configureForBinaryDecimal(precision, scale);

        } else {
            return fail("Decimal with unsupported primitive type " + primitiveTypeName);
        }

        return new PrimitiveParquetColumn(knimeType, type.getName(), cell);
    }

    private static ParquetColumn createForSignedInt(final Type type, final int bitWidth) {
        if (bitWidth <= 32) {
            return new PrimitiveParquetColumn(PrimitiveKnimeType.INTEGER, type.getName(), ParquetCellFactory2.createForInt());
        } else if (bitWidth <= 64) {
            return new PrimitiveParquetColumn(PrimitiveKnimeType.LONG, type.getName(), ParquetCellFactory2.createForLong());
        } else {
            return fail("Unsupported bit width: " + bitWidth);
        }
    }

    private static ParquetColumn createForUnsignedInt(final Type type, final int bitWidth) {
        if (bitWidth < 32) {
            return new PrimitiveParquetColumn(PrimitiveKnimeType.INTEGER, type.getName(), ParquetCellFactory2.createForInt());
        } else if (bitWidth == 32) {
            return new PrimitiveParquetColumn(PrimitiveKnimeType.LONG, type.getName(), ParquetCellFactory2.createForUnsignedInt32());
        } else if (bitWidth == 64) {
            return new PrimitiveParquetColumn(PrimitiveKnimeType.STRING, type.getName(), ParquetCellFactory2.createForUnsignedInt64());
        } else {
            return fail("Unsupported bit width: " + bitWidth);
        }
    }

    private static ParquetColumn createForSignedInt96(final Type type) {
        // INT96 is used to store Date & Time in Impala and is no valid IntLogicalTypeAnnotation bit width
        return new PrimitiveParquetColumn(PrimitiveKnimeType.INSTANT_DATE_TIME, type.getName(), ParquetCellFactory2.createForSignedInt96());
    }

    private static ParquetColumn createForFloat(final Type type) {
        return new PrimitiveParquetColumn(PrimitiveKnimeType.DOUBLE, type.getName(), ParquetCellFactory2.createForFloat());
    }

    private static ParquetColumn createForDouble(final Type type) {
        return new PrimitiveParquetColumn(PrimitiveKnimeType.DOUBLE, type.getName(), ParquetCellFactory2.createForDouble());
    }

    private static ParquetColumn createForBoolean(final Type type) {
        return new PrimitiveParquetColumn(PrimitiveKnimeType.BOOLEAN, type.getName(), ParquetCellFactory2.createForBoolean());
    }

    private static ParquetColumn createForBinary(final Type type) {
        return new PrimitiveParquetColumn(PrimitiveKnimeType.BINARY, type.getName(), ParquetCellFactory2.createForBinary());
    }

    private static ParquetColumn createForDate(final Type type) {
        return new PrimitiveParquetColumn(PrimitiveKnimeType.DATE, type.getName(), ParquetCellFactory2.createForDate());
    }

    private static ParquetColumn createForTime(final Type type) {
        final var timeAnnotation = (TimeLogicalTypeAnnotation)type.getLogicalTypeAnnotation();

        switch (timeAnnotation.getUnit()) {
            case MILLIS:
                return new PrimitiveParquetColumn(PrimitiveKnimeType.TIME, type.getName(),
                    ParquetCellFactory2.createForTimeMillis());
            case MICROS:
                return new PrimitiveParquetColumn(PrimitiveKnimeType.TIME, type.getName(),
                    ParquetCellFactory2.createForTimeMicros());
            case NANOS:
                return new PrimitiveParquetColumn(PrimitiveKnimeType.TIME, type.getName(),
                    ParquetCellFactory2.createForTimeNanos());
            default:
                return fail("Unsupported unit: " + timeAnnotation.getUnit());
        }
    }

    private static ParquetColumn createForTimestamp(final Type type) {
        final var timestampAnnotation = (TimestampLogicalTypeAnnotation)type.getLogicalTypeAnnotation();
        final var isAdjustedToUTC = timestampAnnotation.isAdjustedToUTC();
        final var knimeType =
            isAdjustedToUTC ? PrimitiveKnimeType.INSTANT_DATE_TIME : PrimitiveKnimeType.LOCAL_DATE_TIME;

        switch (timestampAnnotation.getUnit()) {
            case MILLIS:
                return new PrimitiveParquetColumn(knimeType, type.getName(), ParquetCellFactory2.createForTimestampMillis(isAdjustedToUTC));
            case MICROS:
                return new PrimitiveParquetColumn(knimeType, type.getName(), ParquetCellFactory2.createForTimestampMicros(isAdjustedToUTC));
            case NANOS:
                return new PrimitiveParquetColumn(knimeType, type.getName(), ParquetCellFactory2.createForTimestampNanos(isAdjustedToUTC));
            default:
                return fail("Unsupported unit: " + timestampAnnotation.getUnit());
        }
    }

    private static ParquetColumn createForString(final Type type) {
        return new PrimitiveParquetColumn(PrimitiveKnimeType.STRING, type.getName(), ParquetCellFactory2.createForString());
    }

    private static ParquetColumn createForUuid(final Type type) {
        return new PrimitiveParquetColumn(PrimitiveKnimeType.STRING, type.getName(), ParquetCellFactory2.createForUuid());
    }

    // TODO: add some notes to node doc that only simple arrays with primitive field in the group are supported
    private static ParquetColumn listFromGroupType(final GroupType groupType) {
        if (groupType.getFieldCount() != 1) {
            return fail("exactly one element in list group required");
        } else if (!groupType.getType(0).isRepetition(Repetition.REPEATED)) {
            return fail("repeated element in list group required");
        } else if (groupType.getType(0).isPrimitive()) {
            final var elementType = groupType.getType(0).asPrimitiveType();
            return createLegacyListColumn(groupType.getName(), elementType);
        } else {
            return createListGroupColumn(groupType.getName(), groupType.getType(0).asGroupType());
        }
    }

    private static ParquetColumn mapFromGroupType(final GroupType groupType) { // NOSONAR
        if (groupType.getFieldCount() != 1) {
            return fail("exactly one key_value group in map group required");
        } else if (!groupType.getType(0).isRepetition(Repetition.REPEATED)) {
            return fail("repeated element in map group required");
        } else {
            return createListGroupColumn(groupType.getName(), groupType.getType(0).asGroupType());
        }
    }

    private static ParquetColumn createListGroupColumn(final String groupName, final GroupType group) {
        final ArrayList<TypedReaderColumnSpec<KnimeType>> columnSpecs = new ArrayList<>(group.getFieldCount());
        final ArrayList<ParquetCell> columnCells = new ArrayList<>(group.getFieldCount());

        for (final var field : group.getFields()) {
            if (!field.isPrimitive()) {
                return fail(String.format("Unsupported group element '%s' in repeated group", field.getName()));
            }

            final var elementCol = getParquetColumn(field);

            if (elementCol.getErrorMessage() != null) {
                return fail(String.format("Unsupported child element '%s' in repeated group: %s", field.getName(), elementCol.getErrorMessage()));
            }

            final var primitivCol = elementCol.asPrimitiveColumn();

            if (group.getFieldCount() > 1) {
                // use group and element name as column name
                columnSpecs.add(TypedReaderColumnSpec.createWithName(//
                    String.format("%s (%s)", groupName, primitivCol.getName()),//
                    new ListKnimeType(primitivCol.getKnimeType()), true));
            } else {
                // use group name as column name
                columnSpecs.add(TypedReaderColumnSpec.createWithName(//
                    String.format("%s", groupName),//
                    new ListKnimeType(primitivCol.getKnimeType()), true));
            }

            columnCells.add(primitivCol.getParquetCell());
        }

        final var converter = ParquetCellFactory2.createRepeatedGroupColumn(columnCells.toArray(ParquetCell[]::new));
        return new GroupParquetColumn(converter, columnSpecs, ParquetCellFactory2.getBigDataCells(converter));
    }

    private static ParquetColumn createLegacyListColumn(final String groupName, final PrimitiveType elementType) {
        final var elementCol = getParquetColumn(elementType).asPrimitiveColumn();

        if (elementCol.getErrorMessage() == null) {
            final var knimeType = elementCol.getKnimeType();
            final var cell = elementCol.getParquetCell();

            return new PrimitiveParquetColumn(new ListKnimeType(knimeType), groupName,  ParquetCellFactory2.createListLegacyCell(cell));
        } else {
            return fail(String.format("Unsupported child type: %s", elementCol.getErrorMessage()));
        }
    }
}
