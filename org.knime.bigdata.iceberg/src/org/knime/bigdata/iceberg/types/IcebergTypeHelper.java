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
package org.knime.bigdata.iceberg.types;

import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.knime.bigdata.iceberg.util.IcebergFileFormatException;

/**
 * Factory class for Delta Table column instances for Delta types.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class IcebergTypeHelper {

    private IcebergTypeHelper() {
        // static factory class
    }

    /**
     * Creates the matching column for the provided {@link DataType}.
     *
     * @param field {@link StructField} to create the column for
     * @param ordinal the index of the column
     * @param failOnUnsupportedColumnType {@code true} if exception should be raised on unsupported column types
     * @return a column
     * @throws IcebergFileFormatException on unsupported column types
     */
    public static IcebergColumn getParquetColumn(final NestedField field, final int ordinal,
        final boolean failOnUnsupportedColumnType) {

        final var column = getParquetColumn(field, ordinal);
        final var error = column.getErrorMessage();

        if (error != null) {
            final var extendedError =
                String.format("Column '%s' contains unsupported structure or type: %s", field.name(), error);
            if (failOnUnsupportedColumnType) {
                throw new IcebergFileFormatException(extendedError);
            } else {
                return new IcebergUnsupportedColumn(extendedError);
            }
        } else {
            return column;
        }
    }

    private static IcebergColumn fail(final String error) {
        return new IcebergUnsupportedColumn(error);
    }

    private static IcebergColumn getParquetColumn(final NestedField field, final int ordinal) {
        final var type = field.type();

        if (type instanceof PrimitiveType primitiveType) {
            return fromPrimitiveType(field.name(), primitiveType, ordinal);
        } else if (type instanceof DecimalType decimalType) {
            return createForDecimal(field.name(), decimalType, ordinal);
        } else if (type instanceof ListType at) {
            return fromArrayType(field.name(), at, ordinal);
        } else {
            return fail("Group without logical type annotation / original type");
        }
    }

    private static IcebergColumn fromPrimitiveType(final String name, final PrimitiveType type, // NOSONAR mapping function
        final int ordinal) {

        if (type.equals(BinaryType.get())) {
            return createForBinary(name, ordinal);
        } else if (type.equals(BooleanType.get())) {
            return createForBoolean(name, ordinal);
        } else if (type.equals(DoubleType.get())) {
            return createForDouble(name, ordinal);
        } else if (type.equals(FloatType.get())) {
            return createForFloat(name, ordinal);
        } else if (type.equals(IntegerType.get())) {
            return createForInteger(name, ordinal);
        } else if (type.equals(LongType.get())) {
            return createForLong(name, ordinal);
//        } else if (type.equals(ShortType.SHORT)) {
//            return createForShort(name, ordinal);
        } else if (type.equals(StringType.get())) {
            return createForString(name, ordinal);

        // unsupported: ByteType.BYTE
        // unsupported: VariantType.VARIANT

//        } else if (type.equals(DateType.DATE)) {
//            return createForDate(name, ordinal);
//        } else if (type.equals(TimestampType.TIMESTAMP)) {
//            return createForTimestamp(name, ordinal);
//        } else if (type.equals(TimestampNTZType.TIMESTAMP_NTZ)) {
//            return createForTimestampNoTimeZone(name, ordinal);
        }

        return fail("Unsupported type: " + type.toString());
    }

    private static IcebergColumn createForBinary(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.BINARY, name,
            row -> new IcebergBinaryValue(row, ordinal));
    }

    private static IcebergColumn createForBoolean(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.BOOLEAN, name,
            row -> new IcebergBooleanValue(row, ordinal));
    }

    private static IcebergColumn createForDouble(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.DOUBLE, name,
            row -> new IcebergDoubleValue(row, ordinal));
    }

    private static IcebergColumn createForFloat(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.DOUBLE, name,
            row -> new IcebergFloatValue(row, ordinal));
    }

    private static IcebergColumn createForInteger(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.INTEGER, name,
            row -> new IcebergIntegerValue(row, ordinal));
    }

    private static IcebergColumn createForLong(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.LONG, name,
            row -> new IcebergLongValue(row, ordinal));
    }

//    private static DeltaTableColumn createForShort(final String name, final int ordinal) {
//        return new DeltaTablePrimitiveColumn(DeltaTablePrimitiveType.INTEGER, name,
//            row -> new DeltaTableShortValue(row, ordinal));
//    }

    private static IcebergColumn createForString(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.STRING, name,
            row -> new IcebergStringValue(row, ordinal));
    }

    private static IcebergColumn createForDecimal(final String name, final DecimalType decimalType,
        final int ordinal) {

        final var precision = decimalType.precision();
        final var scale = decimalType.scale();
        final IcebergDataType type;

        if ((scale == 0) && (precision <= 9)) {
            type = IcebergPrimitiveType.INTEGER;
        } else if ((scale == 0) && (precision <= 18)) {
            type = IcebergPrimitiveType.LONG;
        } else {
            type = IcebergPrimitiveType.DOUBLE;
        }

        return new IcebergPrimitiveColumn(type, name, row -> new IcebergDecimalValue(row, ordinal));
    }

    private static IcebergColumn createForDate(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.DATE, name,
            row -> new IcebergDateValue(row, ordinal));
    }

    private static IcebergColumn createForTimestamp(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.INSTANT_DATE_TIME, name,
            row -> new IcebergTimestampValue(row, ordinal));
    }

    private static IcebergColumn createForTimestampNoTimeZone(final String name, final int ordinal) {
        return new IcebergPrimitiveColumn(IcebergPrimitiveType.LOCAL_DATE_TIME, name,
            row -> new IcebergTimestampNTZValue(row, ordinal));
    }



    private static IcebergColumn fromArrayType(final String name, final ListType arrayType, final int ordinal) { // NOSONAR mapping function
        final var type = arrayType.elementType();

        if (type.equals(BooleanType.get())) {
            return createForBooleanArray(name, ordinal);
        } else if (type.equals(DoubleType.get())) {
            return createForDoubleArray(name, ordinal);
        } else if (type.equals(FloatType.get())) {
            return createForFloatArray(name, ordinal);
        } else if (type.equals(IntegerType.get())) {
            return createForIntegerArray(name, ordinal);
        } else if (type.equals(LongType.get())) {
            return createForLongArray(name, ordinal);
//        } else if (type.equals(ShortType.SHORT)) {
//            return createForShortArray(name, ordinal);
        } else if (type.equals(StringType.get())) {
            return createForStringArray(name, ordinal);

        // unsupported: ByteType.BYTE
        // unsupported: VariantType.VARIANT

        } else if (type instanceof DecimalType decimalType) {
            return createForDecimalArray(name, decimalType, ordinal);

//        } else if (type.equals(DateType.DATE)) {
//            return createForDateArray(name, ordinal);
//        } else if (type.equals(TimestampType.TIMESTAMP)) {
//            return createForTimestampArray(name, ordinal);
//        } else if (type.equals(TimestampNTZType.TIMESTAMP_NTZ)) {
//            return createForTimestampNoTimeZoneArray(name, ordinal);
        }

        return fail("Unsupported array element type: " + arrayType.elementType());
    }

    private static IcebergArrayColumn createForBooleanArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.BOOLEAN, name,
            row -> new IcebergBooleanArrayValue(row, ordinal));
    }

    private static IcebergArrayColumn createForDoubleArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.DOUBLE, name,
            row -> new IcebergDoubleArrayValue(row, ordinal));
    }

    private static IcebergArrayColumn createForFloatArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.DOUBLE, name,
            row -> new IcebergFloatArrayValue(row, ordinal));
    }

    private static IcebergArrayColumn createForIntegerArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.INTEGER, name,
            row -> new IcebergIntegerArrayValue(row, ordinal));
    }

    private static IcebergArrayColumn createForLongArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.LONG, name,
            row -> new IcebergLongArrayValue(row, ordinal));
    }

//    private static DeltaTableArrayColumn createForShortArray(final String name, final int ordinal) {
//        return new DeltaTableArrayColumn(DeltaTablePrimitiveType.INTEGER, name,
//            row -> new DeltaTableShortArrayValue(row, ordinal));
//    }

    private static IcebergArrayColumn createForStringArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.STRING, name,
            row -> new IcebergStringArrayValue(row, ordinal));
    }


    private static IcebergArrayColumn createForDecimalArray(final String name, final DecimalType decimalType,
        final int ordinal) {

        final var precision = decimalType.precision();
        final var scale = decimalType.scale();
        final IcebergPrimitiveType type;

        if ((scale == 0) && (precision <= 9)) {
            type = IcebergPrimitiveType.INTEGER;
        } else if ((scale == 0) && (precision <= 18)) {
            type = IcebergPrimitiveType.LONG;
        } else {
            type = IcebergPrimitiveType.DOUBLE;
        }

        return new IcebergArrayColumn(type, name, row -> new IcebergDecimalArrayValue(row, ordinal));
    }

    private static IcebergArrayColumn createForDateArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.DATE, name,
            row -> new IcebergDateArrayValue(row, ordinal));
    }

    private static IcebergArrayColumn createForTimestampArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.INSTANT_DATE_TIME, name,
            row -> new IcebergTimestampArrayValue(row, ordinal));
    }

    private static IcebergArrayColumn createForTimestampNoTimeZoneArray(final String name, final int ordinal) {
        return new IcebergArrayColumn(IcebergPrimitiveType.LOCAL_DATE_TIME, name,
            row -> new IcebergTimestampNTZArrayValue(row, ordinal));
    }

}
