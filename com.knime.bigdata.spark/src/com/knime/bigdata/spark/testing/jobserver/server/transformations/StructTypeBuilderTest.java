package com.knime.bigdata.spark.testing.jobserver.server.transformations;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.ExpectedException;

import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder.FieldNameGenerator;

/**
 * Unit tests for {@link StructTypeBuilder}.
 */
public class StructTypeBuilderTest {
    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test that {@link StructTypeBuilder} throws an {@link InvalidSchemaException} if the given {@link StructType}
     * contains fields with unsupported data types.
     *
     * @throws Exception
     */
    @Test
    public void throwsAnInvalidSchemaExceptionIfTheGivenStructTypeContainsFieldsWithUnsupportedDataTypes()
        throws Exception {
        expectInvalidSchemaExceptionForUnsupportedDataType(DataType.BinaryType);
        expectInvalidSchemaExceptionForUnsupportedDataType(DataType.TimestampType);
    }

    private void expectInvalidSchemaExceptionForUnsupportedDataType(@Nonnull final DataType unsupportedDataType) {
        assert unsupportedDataType != null;

        final StructField[] invalidField =
            new StructField[]{DataType.createStructField("invalid", unsupportedDataType, false)};
        final StructType invalidStructType = DataType.createStructType(invalidField);

        boolean caughtInvalidSchemaException = false;
        try {
            StructTypeBuilder.fromStructType(invalidStructType);
        } catch (final InvalidSchemaException ex) {
            caughtInvalidSchemaException = true;
        } catch (final Exception ex) {
            errorCollector.addError(ex);
        } finally {
            errorCollector.checkThat(caughtInvalidSchemaException, is(true));
        }
    }

    /**
     * Test that {@link StructTypeBuilder} replicates the {@link StructType} it has been created from.
     */
    @Test
    public void replicatesTheStructTypeItHasBeenCreatedFrom() throws Exception {
        final StructField[] fields =
            new StructField[]{DataType.createStructField("first", DataType.LongType, true),
                DataType.createStructField("second", DataType.DoubleType, false),
                DataType.createStructField("3rd", DataType.BooleanType, false),
                DataType.createStructField("FouRth", DataType.DateType, true)};
        final StructType expectedType = DataType.createStructType(fields);

        final StructType actualStructType = StructTypeBuilder.fromStructType(expectedType).build();

        assertThat(actualStructType, is(equalTo(expectedType)));
    }

    /**
     * Test that {@link StructTypeBuilder} has the same size as the number of fields of the {@link StructType} it has
     * been created from.
     */
    @Test
    public void hasTheSameSizeAsTheNumberOfFieldsOfTheStructTypeItHasBeenCreatedFrom() throws Exception {
        final StructField[] fields =
            new StructField[]{DataType.createStructField("1", DataType.NullType, true),
                DataType.createStructField("2", DataType.StringType, false),
                DataType.createStructField("3", DataType.ShortType, false)};
        final StructType structType = DataType.createStructType(fields);

        final StructTypeBuilder builder = StructTypeBuilder.fromStructType(structType);

        assertThat(builder.size(), is(fields.length));
    }

    @Test
    public void throwsAnInvalidSchemaExceptionIfTheInitialRowContainsValuesWithUnsupportedDataTypes() throws Exception {
        expectedException.expect(InvalidSchemaException.class);

        final Row invalidRow = Row.create(new Object());

        StructTypeBuilder.fromRows(Collections.singletonList(invalidRow));
    }

    /**
     * Test that {@link StructTypeBuilder} infers the correct {@link DataType} if it is created from a list of
     * {@link Row rows}.
     */
    @Test
    public void infersTheCorrectDataTypeIfItIsCreatedFromAListOfRows() throws Exception {
        expectCorrectDataTypeInference(DataType.NullType, (Object)null);
        expectCorrectDataTypeInference(DataType.BooleanType, Boolean.TRUE, Boolean.FALSE, null);
        expectCorrectDataTypeInference(DataType.ByteType, (Byte)null, (byte)0, (byte)127);
        expectCorrectDataTypeInference(DataType.ShortType, Short.MIN_VALUE);
        expectCorrectDataTypeInference(DataType.IntegerType, null, null, Integer.MAX_VALUE, -31);
        expectCorrectDataTypeInference(DataType.LongType, Long.MIN_VALUE, 0L, Long.MAX_VALUE, null, null);
        expectCorrectDataTypeInference(DataType.FloatType, 3.141f, 0.0f, (Float)null);
        expectCorrectDataTypeInference(DataType.DoubleType, StrictMath.PI, StrictMath.E, (Double)null, 0.0);
        expectCorrectDataTypeInference(DataType.DateType, (Date)null, new Date(1234567890L));
        expectCorrectDataTypeInference(DataType.StringType, (String)null, "A", "sequence", "of", "strings");
    }

    private <T> void expectCorrectDataTypeInference(@Nonnull final DataType expectedDataType,
        @SuppressWarnings("unchecked") final T... values) throws Exception {
        assert expectedDataType != null;
        assert values != null;
        assert values.length > 0;

        final List<Row> rows = new ArrayList<>(values.length);
        for (final T value : values) {
            rows.add(Row.create(value));
        }

        final StructType actualStructType = StructTypeBuilder.fromRows(rows).build();

        errorCollector.checkThat(actualStructType.getFields()[0], hasDataType(expectedDataType));
    }

    private static Matcher<StructField> hasDataType(@Nonnull final DataType expectedDataType) {
        assert expectedDataType != null;

        return new TypeSafeDiagnosingMatcher<StructField>() {
            @Override
            public void describeTo(final Description description) {
                description.appendText("a 'StructField' with data type ").appendValue(expectedDataType);
            }

            @Override
            protected boolean matchesSafely(final StructField structField, final Description mismatchDescription) {
                final DataType actualDataType = structField.getDataType();
                final boolean result = expectedDataType.equals(actualDataType);

                if (!result) {
                    mismatchDescription.appendText("'StructField' has data type ").appendValue(actualDataType);
                }

                return result;
            }
        };
    }

    /**
     * Test that {@link StructTypeBuilder} infers the correct nullable flag if it is created from a list of {@link Row
     * rows}.
     */
    @Test
    public void infersTheCorrectNullableFlagIfItIsCreatedFromAListOfRows() throws Exception {
        expectCorrectNullableFlagInference(true, (Object)null);
        expectCorrectNullableFlagInference(true, Boolean.TRUE, Boolean.FALSE, null);
        expectCorrectNullableFlagInference(true, (Byte)null);
        expectCorrectNullableFlagInference(false, Short.MIN_VALUE);
        expectCorrectNullableFlagInference(true, Integer.MAX_VALUE, null, -31);
        expectCorrectNullableFlagInference(false, Long.MIN_VALUE, 0L, Long.MAX_VALUE);
        expectCorrectNullableFlagInference(false, 3.141f, 0.0f);
        expectCorrectNullableFlagInference(true, StrictMath.PI, StrictMath.E, (Double)null, 0.0);
        expectCorrectNullableFlagInference(true, (Date)null, new Date(1234567890L));
        expectCorrectNullableFlagInference(false, "A", "sequence", "of", "strings");
    }

    private <T> void expectCorrectNullableFlagInference(@Nonnull final boolean expectedNullableFlag,
        @SuppressWarnings("unchecked") final T... values) throws Exception {
        assert values != null;
        assert values.length > 0;

        final List<Row> rows = new ArrayList<>(values.length);
        for (final T value : values) {
            rows.add(Row.create(value));
        }

        final StructType structType = StructTypeBuilder.fromRows(rows).build();

        errorCollector.checkThat(structType.getFields()[0], hasNullableFlag(expectedNullableFlag));
    }

    private Matcher<StructField> hasNullableFlag(final boolean expectedNullableFlag) {
        return new TypeSafeDiagnosingMatcher<StructField>() {
            @Override
            public void describeTo(final Description description) {
                description.appendText("a 'StructField' with nullable flag ").appendValue(expectedNullableFlag);
            }

            @Override
            protected boolean matchesSafely(final StructField structField, final Description mismatchDescription) {
                boolean actualNullableFlag = structField.isNullable();
                final boolean result = expectedNullableFlag == actualNullableFlag;

                if (!result) {
                    mismatchDescription.appendText("'StructField' has nullable flag ").appendValue(actualNullableFlag);
                }

                return result;
            }
        };
    }

    /**
     * Test that {@link StructTypeBuilder} throws an {@link InvalidSchemaException} if the rows have different lengths.
     *
     * @throws Exception
     */
    @Test
    public void throwsAnInvalidSchemaExceptionIfTheRowsHaveDifferentLengths() throws Exception {
        expectedException.expect(InvalidSchemaException.class);

        final List<Row> rows =
            Arrays.asList(Row.create(Integer.valueOf(0)), Row.create(Integer.valueOf(0), Integer.valueOf(0)));

        StructTypeBuilder.fromRows(rows);
    }

    /**
     * Test that {@link StructTypeBuilder} throws an {@link InvalidSchemaException} if the rows have inconsistent data
     * types.
     *
     * @throws Exception
     */
    @Test
    public void throwsAnInvalidSchemaExceptionIfTheRowsHaveInconsistentDataTypes() throws Exception {
        expectedException.expect(InvalidSchemaException.class);

        final List<Row> rows = Arrays.asList(Row.create(Integer.valueOf(0)), Row.create(Long.valueOf(0L)));

        StructTypeBuilder.fromRows(rows);
    }

    private static class FieldNameGeneratorMock implements FieldNameGenerator {
        private static final String DEFAULT_FIELD_NAME = "";

        private static class MethodArgs {
            public final int fieldIndex;

            public final DataType dataType;

            public final boolean isNullable;

            public MethodArgs(final int fieldIndex, final DataType dataType, final boolean isNullable) {
                this.fieldIndex = fieldIndex;
                this.dataType = dataType;
                this.isNullable = isNullable;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
                result = prime * result + fieldIndex;
                result = prime * result + (isNullable ? 1231 : 1237);
                return result;
            }

            @Override
            public boolean equals(final Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (!(obj instanceof MethodArgs)) {
                    return false;
                }

                final MethodArgs other = (MethodArgs)obj;

                boolean result = (dataType == other.dataType || (dataType != null && dataType.equals(other.dataType)));
                result = result && (fieldIndex == other.fieldIndex);
                result = result && (isNullable == other.isNullable);

                return result;
            }
        }

        private final Set<MethodArgs> expectedMethodArgs;

        public FieldNameGeneratorMock() {
            expectedMethodArgs = new HashSet<>();
        }

        public FieldNameGeneratorMock expectGenerateFieldNameInvocation(final int fieldIndex, final DataType dataType,
            final boolean isNullable) {
            expectedMethodArgs.add(new MethodArgs(fieldIndex, dataType, isNullable));

            return this;
        }

        @Override
        public String generateFieldName(final int fieldIndex, final DataType dataType, final boolean isNullable) {
            final MethodArgs actualArgs = new MethodArgs(fieldIndex, dataType, isNullable);

            final boolean wasExpected = expectedMethodArgs.remove(actualArgs);
            if (!wasExpected) {
                final StringBuilder assertionMessage =
                    new StringBuilder("Unexpected invocation: generateFieldName(").append(fieldIndex).append(", ")
                        .append(dataType).append(", ").append(isNullable).append(")");
                throw new AssertionError(assertionMessage.toString());
            }

            return DEFAULT_FIELD_NAME;
        }
    }

    /**
     * Test that {@link StructTypeBuilder} uses the {@link StructTypeBuilder.FieldNameGenerator FieldNameGenerator} to
     * specify the {@code StructField} names.
     *
     * @param fieldNameGenerator the mocked {@code FieldNameGenerator} to use
     *
     * @throws Exception
     */
    @Test
    public void usesTheFieldNameGeneratorToSpecifyTheStructFieldNames() throws Exception {
        final List<Row> rows =
            Arrays.asList(Row.create(Boolean.FALSE, Byte.valueOf((byte)0), Short.valueOf((short)0), Integer.valueOf(0),
                Long.valueOf(0L), null, Double.valueOf(0.0), new Date(0L), null, null), Row.create(null,
                Byte.valueOf((byte)1), null, Integer.valueOf(0), Long.valueOf(0L), Float.valueOf(0.0f),
                Double.valueOf(0.0), new Date(0L), new String(), null));

        final FieldNameGenerator fieldNameGenerator =
            new FieldNameGeneratorMock().expectGenerateFieldNameInvocation(0, DataType.BooleanType, true)
                .expectGenerateFieldNameInvocation(1, DataType.ByteType, false)
                .expectGenerateFieldNameInvocation(2, DataType.ShortType, true)
                .expectGenerateFieldNameInvocation(3, DataType.IntegerType, false)
                .expectGenerateFieldNameInvocation(4, DataType.LongType, false)
                .expectGenerateFieldNameInvocation(5, DataType.FloatType, true)
                .expectGenerateFieldNameInvocation(6, DataType.DoubleType, false)
                .expectGenerateFieldNameInvocation(7, DataType.DateType, false)
                .expectGenerateFieldNameInvocation(8, DataType.StringType, true)
                .expectGenerateFieldNameInvocation(9, DataType.NullType, true);

        StructTypeBuilder.fromRows(rows, fieldNameGenerator);
    }
}
