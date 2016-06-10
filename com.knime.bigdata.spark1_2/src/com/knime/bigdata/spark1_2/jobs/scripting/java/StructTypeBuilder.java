package com.knime.bigdata.spark1_2.jobs.scripting.java;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

import com.knime.bigdata.spark.core.exception.InvalidSchemaException;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * Builder for Spark {@link StructType} objects.
 *
 * <p>
 * {@code StructTypes} can be built either starting {@link #fromStructType(StructType) from} another {@code StructType}
 * or by infering the fields and types {@link #fromRows(List) from} a given list of {@link Row rows}.
 *
 * <p>
 * If a {@code StructTypeBuilder} is created from another {@code StructType}, it simply duplicates that
 * {@code StructType}, i.e., calling {@link #build()} on that {@code StructTypeBuilder} yields a {@code StructType}
 * equivalent to the one the {@code StructTypeBuilder} has been created from.
 *
 * <p>
 * If a {@code StructTypeBuilder} is created from a list of rows, it tries to infer the {@link StructField StructFields}
 * from these rows. That is, the {@code StructTypeBuilder} makes an initial guess about the data types of the various
 * columns based on the values in the first row, and subsequently refines these guesses based on the values in the
 * following rows. If the rows have different lengths, or have values with incompatible data types in the same column,
 * an {@link InvalidSchemaException} is thrown.
 *
 * <p>
 * As {@code StructFields} require a name attribute, {@link #fromRows(List, FieldNameGenerator)} allows providing a
 * {@link FieldNameGenerator}, which should create a non-empty and unique field resp. column name.
 *
 * <p>
 * {@code StructTypeBuilder} currently supports the following Spark {@link DataType DataTypes} resp. Java types:
 * <table>
 * <tr>
 * <th>Spark <code>DataType</code></th>
 * <th>Java type</th>
 * </tr>
 * <tr>
 * <td><code>DataType.NullType</code></td>
 * <td>&mdash;</td>
 * </tr>
 * <tr>
 * <td><code>DataType.BooleanType</code></td>
 * <td><code>Boolean.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.ByteType</code></td>
 * <td><code>Byte.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.ShortType</code></td>
 * <td><code>Short.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.IntegerType</code></td>
 * <td><code>Integer.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.LongType</code></td>
 * <td><code>Long.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.FloatType</code></td>
 * <td><code>Float.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.DoubleType</code></td>
 * <td><code>Double.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.DateType</code></td>
 * <td><code>Date.class</code></td>
 * </tr>
 * <tr>
 * <td><code>DataType.StringType</code></td>
 * <td><code>String.class</code></td>
 * </tr>
 * </table>
 */
@SparkClass
public class StructTypeBuilder {
    /**
     * Interface for field name generators used by {@link StructTypeBuilder#fromRows(List, FieldNameGenerator)}.
     */
    public static interface FieldNameGenerator {
        /**
         * Called by {@link StructTypeBuilder#fromRows(List, FieldNameGenerator)} to generate the field resp. column
         * name for a {@link StructField}.
         *
         * <p>
         * This method should create a unique, non-empty identifier.
         *
         * @param fieldIndex the 0-based field index
         * @param dataType the {@link DataType} of the {@code StructField}
         * @param isNullable indicates whether the {@code StructField} can hold {@code null} values
         *
         * @return a non-empty field name
         */
        String generateFieldName(int fieldIndex, DataType dataType, boolean isNullable);
    }

    private static FieldNameGenerator DEFAULT_FIELD_NAME_GENERATOR = new FieldNameGenerator() {
        @Override
        public String generateFieldName(final int columnIndex, final DataType dataType,
            final boolean isNullable) {
            return "column" + Integer.toString(columnIndex);
        }
    };

    /**
     * mapping of Java primitive types to Spark sql data types
     */
    public static final Map<Class<?>, DataType> DATA_TYPES_BY_CLASS = new HashMap<>();
    static {
        DATA_TYPES_BY_CLASS.put(Boolean.class, DataType.BooleanType);
        DATA_TYPES_BY_CLASS.put(Byte.class, DataType.ByteType);
        DATA_TYPES_BY_CLASS.put(Short.class, DataType.ShortType);
        DATA_TYPES_BY_CLASS.put(Integer.class, DataType.IntegerType);
        DATA_TYPES_BY_CLASS.put(Long.class, DataType.LongType);
        DATA_TYPES_BY_CLASS.put(Float.class, DataType.FloatType);
        DATA_TYPES_BY_CLASS.put(Double.class, DataType.DoubleType);
        DATA_TYPES_BY_CLASS.put(Date.class, DataType.DateType);
        DATA_TYPES_BY_CLASS.put(String.class, DataType.StringType);
    }

    private static final Set<DataType> SUPPORTED_DATA_TYPES;
    static {
        SUPPORTED_DATA_TYPES = new HashSet<>(DATA_TYPES_BY_CLASS.values());
        SUPPORTED_DATA_TYPES.add(DataType.NullType);
    }

    private static final DataType dataTypeForValue(final Object value) {
        return value != null ? DATA_TYPES_BY_CLASS.get(value.getClass()) : DataType.NullType;
    }

    private static class DataTypeDescriptor {
        private DataType dataType;

        private boolean isNullable;

        public DataTypeDescriptor(final DataType aDataType, final boolean aIsNullable) {
            assert aDataType != null;

            this.dataType = aDataType;
            this.isNullable = aIsNullable;
        }

        public DataType getDataType() {
            return dataType;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public void generalizeToValue(final Object value) throws InvalidSchemaException {
            final DataType valueDataType = dataTypeForValue(value);

            if (valueDataType != null && canBeGeneralizedTo(valueDataType)) {
                if (valueDataType != DataType.NullType) {
                    dataType = valueDataType;
                }
                isNullable = isNullable || value == null;
            } else {
                final StringBuilder message = new StringBuilder("Cannot adjust ");
                if (isNullable) {
                    message.append("nullable ");
                }
                message.append("data type ").append(dataType.getClass().getSimpleName()).append(" to value '")
                    .append(value).append("'");
                if (value != null) {
                    message.append(" of type ").append(value.getClass().getSimpleName());
                }

                throw new InvalidSchemaException(message.toString());
            }
        }

        private boolean canBeGeneralizedTo(final DataType otherDataType) {
            assert otherDataType != null;

            return dataType.equals(DataType.NullType) || otherDataType.equals(DataType.NullType)
                || otherDataType.equals(dataType);
        }

        public StructField toStructField(final String name) {
            assert name != null;

            return DataType.createStructField(name, dataType, isNullable);
        }
    }

    private static class FieldDescriptor {
        private String name;

        private DataTypeDescriptor dataTypeDescriptor;

        public FieldDescriptor(final StructField structField) {
            assert structField != null;

            name = structField.getName();
            dataTypeDescriptor = new DataTypeDescriptor(structField.getDataType(), structField.isNullable());
        }

        public FieldDescriptor(final String aName, final DataTypeDescriptor aDataTypeDescriptor) {
            assert aDataTypeDescriptor != null;

            this.name = aName;
            this.dataTypeDescriptor = aDataTypeDescriptor;
        }


        public StructField toStructField() {
            return dataTypeDescriptor.toStructField(name);
        }
    }

    private List<FieldDescriptor> fieldDescriptors;

    private StructTypeBuilder(final List<FieldDescriptor> aFieldDescriptors) {
        this.fieldDescriptors = aFieldDescriptors;
    }

    /**
     * Creates a {@code StructTypeBuilder} based on the given {@link StructType}.
     *
     * <p>
     * The new {@code StructTypeBuilder} is initialized with all {@link StructField StructFields} of the given
     * {@code StructType}. That is, calling {@link #build()} on the new {@code StructTypeBuilder} yields a
     * {@code StructType} equivalent to the given {@code StructType}.
     *
     * @param structType the {@code StructType} from which to create the new {@code StructTypeBuilder} (must not be
     *            {@code null})
     *
     * @return a new {@code StructTypeBuilder} initialized with all fields of the given {@code StructType} (will not be
     *         {@code null})
     *
     * @throws InvalidSchemaException if the given {@code StructType} contains fields with an unsupported
     *             {@code DataType}
     */

    public static StructTypeBuilder fromStructType(final StructType structType) throws InvalidSchemaException {
        Validate.notNull(structType, "Struct type must not be null");

        final StructField[] structFields = structType.getFields();
        final List<FieldDescriptor> fieldDescriptors = new ArrayList<FieldDescriptor>(structFields.length);

        for (final StructField structField : structFields) {
            final DataType dataType = structField.getDataType();
            if (SUPPORTED_DATA_TYPES.contains(dataType)) {
                fieldDescriptors.add(new FieldDescriptor(structField));
            } else {
                throw new InvalidSchemaException("'StructField's '" + structField.getName()
                    + "' has unsupported data type " + dataType.getClass().getSimpleName());
            }
        }

        return new StructTypeBuilder(fieldDescriptors);
    }

    /**
     * Creates a {@code StructTypeBuilder} based on the given list of {@link Row rows}, using
     * "column&lt;<var>columnIndex</var>&gt;" for the field names.
     *
     * @param rows the list of rows from which to infer the data types of the {@link StructType StructType's} fields
     *            (must not be {@code null} or empty)
     *
     * @return a new {@code StructTypeBuilder} initialized with all fields inferred from the given rows (will not be
     *         {@code null})
     *
     * @throws InvalidSchemaException if no consistent data types could be inferred or the columns have different
     *             lengths
     *
     * @see #fromRows(List, FieldNameGenerator) fromRows(List, FieldNameGenerator) for further details
     */

    public static StructTypeBuilder fromRows(final List<Row> rows) throws InvalidSchemaException {
        return fromRows(rows, DEFAULT_FIELD_NAME_GENERATOR);
    }

    /**
     * Creates a {@code StructTypeBuilder} based on the given list of {@link Row rows}.
     *
     * <p>
     * The {@link DataType DataTypes} of the various fields resp. column are inferred from the column values of the
     * given rows. The initial guess is based on the values in the first row, and is made according to the table shown
     * above. If the first row contains a {@code null} value in a given column, it initially assumes the
     * {@link DataType#NullType} for this column.
     *
     * <p>
     * The initial guesses are subsequently refined based on the values in the other rows:
     * <ul>
     * <li>If the current guess for a column's data type is {@code NullType} and the current row contains a non-null
     * value for that column, the column's data type is refined to match the Java type of the non-null value, but the
     * column will be assumed to be nullable.</li>
     * <li>If the current guess for a column's data type is <em>not</em> {@code NullType} but the current row contains a
     * {@code null} value for that column, the column will be assumed to be nullable.</li>
     * <li>If a column's data type is <em>not</em> {@code NullType} and the Java type of the current row value does not
     * match this {@code DataType}, an {@link InvalidSchemaException} is thrown.
     * <li>If the columns have different lengths, an {@link InvalidSchemaException} is thrown.
     * </ul>
     *
     * <p>
     * When all given rows have been taken into account, the given {@link FieldNameGenerator} is used to provide names
     * for all {@link StructField StructFields}.
     *
     * @param rows the list of rows from which to infer the data types of the {@link StructType StructType's} fields
     *            (must not be {@code null} or empty)
     *
     * @param fieldNameGenerator the {@code FieldNameGenerator} used to provide the names for the {@code StructFields}
     *            (must not be {@code null})
     *
     * @return a new {@code StructTypeBuilder} initialized with all fields inferred from the given rows (will not be
     *         {@code null})
     *
     * @throws InvalidSchemaException if no consistent data types could be inferred or the columns have different
     *             lengths
     */
    public static StructTypeBuilder fromRows(final List<Row> rows,
        final FieldNameGenerator fieldNameGenerator) throws InvalidSchemaException {

        Validate.notEmpty(rows, "List of rows must not be null or empty");
        Validate.noNullElements(rows, "List of rows must not contain null elements");
        Validate.notNull(fieldNameGenerator, "Field name generator must not be null");

        final Iterator<Row> rowsIter = rows.iterator();
        Row row = rowsIter.next();
        final List<DataTypeDescriptor> dataTypeDescriptors = inferDataTypeDescriptors(row);

        while (rowsIter.hasNext()) {
            row = rowsIter.next();

            if (row.length() != dataTypeDescriptors.size()) {
                throw new InvalidSchemaException("Rows have different lengths");
            }

            for (int columnIndex = 0; columnIndex < row.length(); ++columnIndex) {
                final Object value = row.get(columnIndex);
                final DataTypeDescriptor dataTypeDescriptor = dataTypeDescriptors.get(columnIndex);

                dataTypeDescriptor.generalizeToValue(value);
            }
        }

        final List<FieldDescriptor> fieldDescriptors = toFieldDescriptors(fieldNameGenerator, dataTypeDescriptors);

        return new StructTypeBuilder(fieldDescriptors);
    }



    private static List<DataTypeDescriptor> inferDataTypeDescriptors(final Row row)
        throws InvalidSchemaException {
        assert row != null;

        final List<DataTypeDescriptor> dataTypeDescriptors = new ArrayList<>(row.length());

        for (int i = 0; i < row.length(); ++i) {
            final Object value = row.get(i);
            final DataType dataType = dataTypeForValue(value);
            if (dataType == null) {
                throw new InvalidSchemaException("Could not infer data type for value at position " + i);
            }
            final boolean isNullable = value == null;

            dataTypeDescriptors.add(new DataTypeDescriptor(dataType, isNullable));
        }

        return dataTypeDescriptors;
    }


    private static List<FieldDescriptor> toFieldDescriptors(final FieldNameGenerator fieldNameGenerator,
        final List<DataTypeDescriptor> dataTypeDescriptors) {
        assert fieldNameGenerator != null;
        assert dataTypeDescriptors != null;

        final List<FieldDescriptor> fieldDescriptors = new ArrayList<>(dataTypeDescriptors.size());
        int columnIndex = 0;
        for (final DataTypeDescriptor dataTypeDescriptor : dataTypeDescriptors) {
            final String fieldName =
                fieldNameGenerator.generateFieldName(columnIndex, dataTypeDescriptor.getDataType(),
                    dataTypeDescriptor.isNullable());
            fieldDescriptors.add(new FieldDescriptor(fieldName, dataTypeDescriptor));

            ++columnIndex;
        }

        return fieldDescriptors;
    }

    /**
     * The current number of fields of this {@code StructTypeBuilder}.
     *
     * @return the current number of fields
     */
    public int size() {
        return fieldDescriptors.size();
    }

    /**
     * Builds a {@link StructType} according to the fields of this {@code StructTypeBuilder}.
     *
     * @return a {@code StructType} containing all fields of this {@code StructTypeBuilder} (will not be {@code null})
     */

    public StructType build() {
        final List<StructField> structFields = new ArrayList<StructField>(fieldDescriptors.size());

        for (final FieldDescriptor fieldDescriptor : fieldDescriptors) {
            structFields.add(fieldDescriptor.toStructField());
        }

        return DataType.createStructType(structFields);
    }

    /**
     *
     * @param aType
     * @return Java primitive class of given data type
     */
    public static Class<?> getJavaTypeFromDataType(final DataType aType) {
        for (Map.Entry<Class<?>, DataType> entry : DATA_TYPES_BY_CLASS.entrySet()) {
            if (entry.getValue().getClass().equals(aType.getClass())) {
                return entry.getKey();
            }
        }
        return Object.class;
    }
}
