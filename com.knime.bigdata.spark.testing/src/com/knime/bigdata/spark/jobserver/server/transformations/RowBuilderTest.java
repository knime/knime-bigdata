package com.knime.bigdata.spark.jobserver.server.transformations;


import static org.junit.Assert.assertThat;

import java.util.Arrays;

import org.apache.spark.sql.api.java.Row;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;


/**
 * Unit tests for {@link RowBuilder}.
 */
public class RowBuilderTest {
  /**
   * Test that {@link RowBuilder} adds all elements if it is constructed from another {@link Row}.
   */
  @Test
  public void addsAllElementsIfItIsConstructedFromAnotherRow() {
    final Object[] values = { new Byte((byte) 111), new Long(-987654321L), new Short((short) 1234),
        new Integer(Integer.MAX_VALUE), new Object() };
    final Row row = Row.create(values);

    final Row builtRow = RowBuilder.fromRow(row).build();

    assertThat(builtRow, containsSameObjectsAs(values));
  }

  private static Matcher<Row> containsSameObjectsAs(final Object... expectedValues) {
    return new TypeSafeDiagnosingMatcher<Row>() {
      @Override
      public void describeTo(final Description description) {
        description.appendText("a 'Row' containing values ").appendValueList("[", ", ", "]", expectedValues);
      }

      @Override
      protected boolean matchesSafely(final Row row, final Description mismatchDescription) {
        boolean result = row.length() == expectedValues.length;
        if (!result) {
          mismatchDescription.appendText("expected " + expectedValues.length + " values");
        } else {
          for (int i = 0; i < row.length(); ++i) {
            result = result && row.get(i) == expectedValues[i];
            if (!result) {
              mismatchDescription.appendText("differing values at index " + i + ", expected ")
                  .appendValue(expectedValues[i]).appendText(", but got ").appendValue(row.get(i));
            }
          }
        }

        return result;
      }
    };
  }

  /**
   * Test that {@link RowBuilder} appends the given {@code Object} to the end of its {@link Row}.
   */
  @Test
  public void appendTheGivenObjectToTheEndOfItsRow() {
    final Row originalRow = Row.create(new Object(), new Object());
    final Object value = new Long(Long.MIN_VALUE);

    final Row row = RowBuilder.fromRow(originalRow).add(value).build();

    assertThat(row, endsWith(value));
  }

  private static Matcher<Row> endsWith(final Object... expectedValues) {
    return new TypeSafeDiagnosingMatcher<Row>() {
      @Override
      public void describeTo(final Description description) {
        description.appendText("a 'Row' ending with values ").appendValueList("[", ", ", "]", expectedValues);
      }

      @Override
      protected boolean matchesSafely(final Row row, final Description mismatchDescription) {
        boolean result = row.length() >= expectedValues.length;
        if (!result) {
          mismatchDescription.appendText("expected at least " + expectedValues.length + " values");
        } else {
          for (int i = 0, j = row.length() - expectedValues.length; i < expectedValues.length; ++i, ++j) {
            result = result && row.get(j) == expectedValues[i];
            if (!result) {
              mismatchDescription.appendText("differing values at index " + i + ", expected ")
                  .appendValue(expectedValues[i]).appendText(", but got ").appendValue(row.get(j));
            }
          }
        }

        return result;
      }
    };
  }

  /**
   * Test that {@link RowBuilder} appends the given {@code Object} to the end of its {@link Row}.
   */
  @Test
  public void appendAllGivenObjectsToTheEndOfItsRow() {
    final Row originalRow = Row.create(new Object(), new Object());

    final Object[] values = new Object[] { new Object(), new Integer(Integer.MIN_VALUE), new Double(3.1415926) };
    final Iterable<Object> valuesIterable = Arrays.asList(values);

    final Row row = RowBuilder.fromRow(originalRow).addAll(valuesIterable).build();

    assertThat(row, endsWith(values));
  }
}
