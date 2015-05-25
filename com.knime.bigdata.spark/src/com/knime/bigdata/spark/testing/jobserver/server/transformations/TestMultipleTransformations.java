package com.knime.bigdata.spark.testing.jobserver.server.transformations;


import static org.junit.Assert.assertThat;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.transformation.AbstractTransformation;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;


public class TestMultipleTransformations {
  private static final SparkConf conf = new SparkConf().setAppName(TestMultipleTransformations.class.getSimpleName())
      .setMaster("local");

  private static class ConstantAppender<C extends Serializable> implements AbstractTransformation {
    private static final long serialVersionUID = 1L;

    private final C constant;

    public ConstantAppender(final C constant) {
      this.constant = constant;
    }

    @Override
    public <T extends JavaRDDLike<Row, ?>> JavaRDD<Row> apply(final T input) {
      return input.map(new Function<Row, Row>() {
        private static final long serialVersionUID = 1L;

        @Override
        public Row call(final Row r) throws Exception {
          return RowBuilder.fromRow(r).add(constant).build();
        }
      });
    }
  }

  @Test
  public void canApplyMultipleTransformations() throws InvalidSchemaException {
    try (final JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
      final JavaHiveContext hiveContext = new JavaHiveContext(sparkContext);

      final JavaSchemaRDD inputRDD = hiveContext.hql("select * from iris");

      for (final StructField field : inputRDD.schema().getFields()) {
        System.out.println("Field '" + field.getName() + "' of type '" + field.getDataType() + "'");
      }

      final JavaRDD<Row> appendedRDD = new ConstantAppender<String>("Test").apply(inputRDD);
      final StructType appendedRDDSchema = StructTypeBuilder.fromRows(appendedRDD.take(10)).build();

      assertThat(
          appendedRDDSchema,
          hasFieldTypes(DataType.FloatType, DataType.FloatType, DataType.FloatType, DataType.FloatType,
              DataType.StringType, DataType.StringType));

      final JavaSchemaRDD outputRDD = hiveContext.applySchema(appendedRDD, appendedRDDSchema);
      outputRDD.saveAsTable("iris_processed");
    }
  }

  private static Matcher<StructType> hasFieldTypes(@Nonnull final DataType... fieldTypes) {
    assert fieldTypes != null;

    return new TypeSafeDiagnosingMatcher<StructType>() {
      @Override
      public void describeTo(final Description description) {
        description.appendText("a 'StructType' with field types ").appendValueList("[", ", ", "]", fieldTypes);
      }

      @Override
      protected boolean matchesSafely(final StructType structType, final Description mismatchDescription) {
        StructField[] fields = structType.getFields();

        boolean result = fields.length == fieldTypes.length;
        if (result) {
          for (int i = 0; i < fields.length; ++i) {
            result = fields[i].getDataType().equals(fieldTypes[i]);

            if (!result) {
              mismatchDescription.appendText("expected 'DataType' ").appendValue(fieldTypes[i])
                  .appendText(" for 'StructField' ").appendValue(i).appendText(", but field has type ")
                  .appendValue(fields[i].getDataType());
              break;
            }
          }
        } else {
          mismatchDescription.appendText("expected ").appendValue(fieldTypes.length)
              .appendText(" fields, but 'StructType' has ").appendValue(fields.length);
        }

        return result;
      }
    };
  }
}
