package com.knime.bigdata.spark1_2.api;


import java.util.ArrayList;

import org.apache.commons.lang3.Validate;
import org.apache.spark.sql.api.java.Row;

import com.knime.bigdata.spark.core.job.SparkClass;


/**
 * Builder for Spark {@link Row} objects.
 *
 * <p>
 * Rows can be built either starting {@link #fromRow(Row) from} a given row or with an {@link #emptyRow()} empty
 * {@code RowBuilder}. Values can be appended to the end of the row either {@link #add(Object) individually} or in
 * {@link #addAll(Iterable) batch}. The resulting {@code Row} is built with a call to {@link #build()}.
 */
@SparkClass
public class RowBuilder {
  private static final int DEFAULT_SIZE_HINT = 16;
  private final ArrayList<Object> columnValues;

  private RowBuilder(final int sizeHint) {
    assert sizeHint > 0;

    columnValues = new ArrayList<>(sizeHint);
  }

  /**
   * Constructs a {@code RowBuilder} containing all objects of the given {@code row}, in the order they are returned by
   * {@link Row#get(int)}.
   *
   * @param row
   *          the row whose elements shall be put into the new {@code RowBuilder} (must not be {@code null}).
   *
   * @return a new {@code RowBuilder} containing all objects of the given row.
   *
   * @throws NullPointerException
   *           if the specified row is {@code null}.
   */
  public static RowBuilder fromRow(final Row row) {
    Validate.notNull(row, "Row must not be null");

    final RowBuilder builder = new RowBuilder(row.length());
    for (int i = 0; i < row.length(); ++i) {
      builder.columnValues.add(row.get(i));
    }

    return builder;
  }

  /**
   * Constructs an empty {@code RowBuilder}.
   *
   * @return a new, empty {@code RowBuilder}.
   */
  public static RowBuilder emptyRow() {
    return new RowBuilder(DEFAULT_SIZE_HINT);
  }

  /**
   * Builds a new {@code Row} containing all objects of this {@code RowBuilder}.
   *
   * @return a new row containing all of this {@code RowBuilder} (will not be {@code null}).
   */
  public Row build() {
    return Row.create(columnValues.toArray());
  }

  /**
   * Appends the specified value to the end of this {@code RowBuilder}.
   *
   * @param value
   *          the value to append
   *
   * @return this {@code RowBuilder}.
   */
  public <E extends Object> RowBuilder add(final E value) {
    columnValues.add(value);

    return this;
  }

  /**
   * Appends all values provided by the given the {@code Iterable} to the end of this {@code RowBuilder}, in the order
   * they are returned by the {@code Iterable}.
   *
   * @param values
   *          the {@code Iterable} providing the values to be added to this {@code RowBuilder} (may not be {@code null}
   *          ).
   *
   * @return this {@code RowBuilder}.
   *
   * @throws NullPointerException
   *           if the specified {@code Iterable} is {@code null}.
   */
  public RowBuilder addAll(final Iterable<? extends Object> values) {
    Validate.notNull(values, "Values iterable must not be null");

    for (final Object value : values) {
      columnValues.add(value);
    }

    return this;
  }
}
