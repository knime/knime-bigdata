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
package org.knime.bigdata.delta.types;

import org.knime.bigdata.delta.util.DeltaTableFileFormatException;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

/**
 * Factory class for Delta Table column instances for Delta types.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class DeltaTableTypeHelper {

    private DeltaTableTypeHelper() {
        // static factory class
    }

    /**
     * Creates the matching column for the provided {@link DataType}.
     *
     * @param field {@link StructField} to create the column for
     * @param ordinal the index of the column
     * @param failOnUnsupportedColumnType {@code true} if exception should be raised on unsupported column types
     * @return a column
     * @throws DeltaTableFileFormatException on unsupported column types
     */
    public static DeltaTableColumn getParquetColumn(final StructField field, final int ordinal,
        final boolean failOnUnsupportedColumnType) {

        final var column = getParquetColumn(field, ordinal);
        final var error = column.getErrorMessage();

        if (error != null) {
            final var extendedError =
                String.format("Column '%s' contains unsupported structure or type: %s", field.getName(), error);
            if (failOnUnsupportedColumnType) {
                throw new DeltaTableFileFormatException(extendedError);
            } else {
                return new DeltaTableUnsupportedColumn(extendedError);
            }
        } else {
            return column;
        }
    }

    private static DeltaTableColumn fail(final String error) {
        return new DeltaTableUnsupportedColumn(error);
    }

    private static DeltaTableColumn getParquetColumn(final StructField field, final int ordinal) {
        final var type = field.getDataType();

        if (type instanceof BasePrimitiveType primitiveType) {
            return fromPrimitiveType(field.getName(), primitiveType, ordinal);
        } else if (type instanceof DecimalType decimalType) {
            return createForDecimal(field.getName(), decimalType, ordinal);
        } else if (type instanceof ArrayType at) {
            return fromArrayType(field.getName(), at, ordinal);
        } else {
            return fail("Group without logical type annotation / original type");
        }
    }

    private static DeltaTableColumn fromPrimitiveType(final String name, final BasePrimitiveType type, // NOSONAR mapping function
        final int ordinal) {

        if (type.equals(BinaryType.BINARY)) {
            return createForBinary(name, ordinal);
        } else if (type.equals(BooleanType.BOOLEAN)) {
            return createForBoolean(name, ordinal);
        } else if (type.equals(DoubleType.DOUBLE)) {
            return createForDouble(name, ordinal);
        } else if (type.equals(FloatType.FLOAT)) {
            return createForFloat(name, ordinal);
        } else if (type.equals(IntegerType.INTEGER)) {
            return createForInteger(name, ordinal);
        } else if (type.equals(LongType.LONG)) {
            return createForLong(name, ordinal);
        } else if (type.equals(ShortType.SHORT)) {
            return createForShort(name, ordinal);
        } else if (type.equals(StringType.STRING)) {
            return createForString(name, ordinal);

        // unsupported: ByteType.BYTE
        // unsupported: VariantType.VARIANT

        } else if (type.equals(DateType.DATE)) {
            return createForDate(name, ordinal);
        } else if (type.equals(TimestampType.TIMESTAMP)) {
            return createForTimestamp(name, ordinal);
        } else if (type.equals(TimestampNTZType.TIMESTAMP_NTZ)) {
            return createForTimestampNoTimeZone(name, ordinal);
        }

        return fail("Unsupported type: " + type.toString());
    }

    private static DeltaTableColumn createForBinary(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.BINARY, name,
            row -> new DeltaTableBinaryValue(row, ordinal));
    }

    private static DeltaTableColumn createForBoolean(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.BOOLEAN, name,
            row -> new DeltaTableBooleanValue(row, ordinal));
    }

    private static DeltaTableColumn createForDouble(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.DOUBLE, name,
            row -> new DeltaTableDoubleValue(row, ordinal));
    }

    private static DeltaTableColumn createForFloat(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.DOUBLE, name,
            row -> new DeltaTableFloatValue(row, ordinal));
    }

    private static DeltaTableColumn createForInteger(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.INTEGER, name,
            row -> new DeltaTableIntegerValue(row, ordinal));
    }

    private static DeltaTableColumn createForLong(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.LONG, name,
            row -> new DeltaTableLongValue(row, ordinal));
    }

    private static DeltaTableColumn createForShort(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.INTEGER, name,
            row -> new DeltaTableShortValue(row, ordinal));
    }

    private static DeltaTableColumn createForString(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.STRING, name,
            row -> new DeltaTableStringValue(row, ordinal));
    }

    private static DeltaTableColumn createForDecimal(final String name, final DecimalType decimalType,
        final int ordinal) {

        final var precision = decimalType.getPrecision();
        final var scale = decimalType.getScale();
        final DeltaTableDataType type;

        if ((scale == 0) && (precision <= 9)) {
            type = DeltaTablePrimitiveType.INTEGER;
        } else if ((scale == 0) && (precision <= 18)) {
            type = DeltaTablePrimitiveType.LONG;
        } else {
            type = DeltaTablePrimitiveType.DOUBLE;
        }

        return new DeltaTablePrimitiveColumn(type, name, row -> new DeltaTableDecimalValue(row, ordinal));
    }

    private static DeltaTableColumn createForDate(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.DATE, name,
            row -> new DeltaTableDateValue(row, ordinal));
    }

    private static DeltaTableColumn createForTimestamp(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.INSTANT_DATE_TIME, name,
            row -> new DeltaTableTimestampValue(row, ordinal));
    }

    private static DeltaTableColumn createForTimestampNoTimeZone(final String name, final int ordinal) {
        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.LOCAL_DATE_TIME, name,
            row -> new DeltaTableTimestampNTZValue(row, ordinal));
    }



    private static DeltaTableColumn fromArrayType(final String name, final ArrayType arrayType, final int ordinal) { // NOSONAR mapping function
        final var type = arrayType.getElementType();

        if (type.equals(BooleanType.BOOLEAN)) {
            return createForBooleanArray(name, ordinal);
        } else if (type.equals(DoubleType.DOUBLE)) {
            return createForDoubleArray(name, ordinal);
        } else if (type.equals(FloatType.FLOAT)) {
            return createForFloatArray(name, ordinal);
        } else if (type.equals(IntegerType.INTEGER)) {
            return createForIntegerArray(name, ordinal);
        } else if (type.equals(LongType.LONG)) {
            return createForLongArray(name, ordinal);
        } else if (type.equals(ShortType.SHORT)) {
            return createForShortArray(name, ordinal);
        } else if (type.equals(StringType.STRING)) {
            return createForStringArray(name, ordinal);

        // unsupported: ByteType.BYTE
        // unsupported: VariantType.VARIANT

        } else if (type instanceof DecimalType decimalType) {
            return createForDecimalArray(name, decimalType, ordinal);

        } else if (type.equals(DateType.DATE)) {
            return createForDateArray(name, ordinal);
        } else if (type.equals(TimestampType.TIMESTAMP)) {
            return createForTimestampArray(name, ordinal);
        } else if (type.equals(TimestampNTZType.TIMESTAMP_NTZ)) {
            return createForTimestampNoTimeZoneArray(name, ordinal);
        }

        return fail("Unsupported array element type: " + arrayType.getElementType());
    }

    private static DeltaTableArrayColumn createForBooleanArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.BOOLEAN, name,
            row -> new DeltaTableBooleanArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForDoubleArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.DOUBLE, name,
            row -> new DeltaTableDoubleArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForFloatArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.DOUBLE, name,
            row -> new DeltaTableFloatArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForIntegerArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.INTEGER, name,
            row -> new DeltaTableIntegerArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForLongArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.LONG, name,
            row -> new DeltaTableLongArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForShortArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.INTEGER, name,
            row -> new DeltaTableShortArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForStringArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.STRING, name,
            row -> new DeltaTableStringArrayValue(row, ordinal));
    }


    private static DeltaTableArrayColumn createForDecimalArray(final String name, final DecimalType decimalType,
        final int ordinal) {

        final var precision = decimalType.getPrecision();
        final var scale = decimalType.getScale();
        final DeltaTablePrimitiveType type;

        if ((scale == 0) && (precision <= 9)) {
            type = DeltaTablePrimitiveType.INTEGER;
        } else if ((scale == 0) && (precision <= 18)) {
            type = DeltaTablePrimitiveType.LONG;
        } else {
            type = DeltaTablePrimitiveType.DOUBLE;
        }

        return new DeltaTableArrayColumn(type, name, row -> new DeltaTableDecimalArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForDateArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.DATE, name,
            row -> new DeltaTableDateArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForTimestampArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.INSTANT_DATE_TIME, name,
            row -> new DeltaTableTimestampArrayValue(row, ordinal));
    }

    private static DeltaTableArrayColumn createForTimestampNoTimeZoneArray(final String name, final int ordinal) {
        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.LOCAL_DATE_TIME, name,
            row -> new DeltaTableTimestampNTZArrayValue(row, ordinal));
    }

}
