/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.knime.bigdata.delta.example;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Base class for reading Delta Lake tables using the Delta Kernel APIs.
 */
public abstract class BaseTableReader {
    public static final int DEFAULT_LIMIT = 20;

    protected final String tablePath;

    protected final Engine engine;

    public BaseTableReader(final String tablePath) {
        this.tablePath = requireNonNull(tablePath);
        this.engine = DefaultEngine.create(new Configuration());
    }

    /**
     * Show the given {@code limit} rows containing the given columns with the predicate from the table.
     *
     * @param limit Max number of rows to show.
     * @param columnsOpt If null, show all columns in the table.
     * @param predicateOpt Optional predicate
     * @return Number of rows returned by the query.
     * @throws TableNotFoundException
     * @throws IOException
     */
    public abstract int show(int limit, Optional<List<String>> columnsOpt, Optional<Predicate> predicateOpt)
        throws TableNotFoundException, IOException;

    /**
     * Utility method to return a pruned schema that contains the given {@code columns} from {@code baseSchema}
     */
    protected static StructType pruneSchema(final StructType baseSchema, final Optional<List<String>> columns) {
        if (!columns.isPresent()) {
            return baseSchema;
        }
        List<StructField> selectedFields = columns.get().stream().map(column -> {
            if (baseSchema.indexOf(column) == -1) {
                throw new IllegalArgumentException(format("Column %s is not found in table", column));
            }
            return baseSchema.get(column);
        }).collect(Collectors.toList());

        return new StructType(selectedFields);
    }

    protected static int printData(final FilteredColumnarBatch data, final int maxRowsToPrint) {
        int printedRowCount = 0;
        try (CloseableIterator<Row> rows = data.getRows()) {
            while (rows.hasNext()) {
                printRow(rows.next());
                printedRowCount++;
                if (printedRowCount == maxRowsToPrint) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return printedRowCount;
    }

    protected static void printSchema(final StructType schema) {
        System.out.printf(formatter(schema.length()), schema.fieldNames().toArray(new String[0]));
    }

    protected static void printRow(final Row row) {
        int numCols = row.getSchema().length();
        Object[] rowValues = IntStream.range(0, numCols).mapToObj(colOrdinal -> getValue(row, colOrdinal)).toArray();

        // TODO: Need to handle the Row, Map, Array, Timestamp, Date types specially to
        // print them in the format they need. Copy this code from Spark CLI.

        System.out.printf(formatter(numCols), rowValues);
    }

    private static String formatter(final int length) {
        return IntStream.range(0, length).mapToObj(i -> "%20s").collect(Collectors.joining("|")) + "\n";
    }

    private static String getValue(final Row row, final int columnOrdinal) {
        DataType dataType = row.getSchema().at(columnOrdinal).getDataType();
        if (row.isNullAt(columnOrdinal)) {
            return null;
        } else if (dataType instanceof BooleanType) {
            return Boolean.toString(row.getBoolean(columnOrdinal));
        } else if (dataType instanceof ByteType) {
            return Byte.toString(row.getByte(columnOrdinal));
        } else if (dataType instanceof ShortType) {
            return Short.toString(row.getShort(columnOrdinal));
        } else if (dataType instanceof IntegerType) {
            return Integer.toString(row.getInt(columnOrdinal));
        } else if (dataType instanceof DateType) {
            // DateType data is stored internally as the number of days since 1970-01-01
            int daysSinceEpochUTC = row.getInt(columnOrdinal);
            return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
        } else if (dataType instanceof LongType) {
            return Long.toString(row.getLong(columnOrdinal));
        } else if (dataType instanceof TimestampType) {
            // TimestampType data is stored internally as the number of microseconds since epoch
            long microSecsSinceEpochUTC = row.getLong(columnOrdinal);
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
                (int)(1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */, ZoneOffset.UTC);
            return dateTime.toString();
        } else if (dataType instanceof FloatType) {
            return Float.toString(row.getFloat(columnOrdinal));
        } else if (dataType instanceof DoubleType) {
            return Double.toString(row.getDouble(columnOrdinal));
        } else if (dataType instanceof StringType) {
            return row.getString(columnOrdinal);
        } else if (dataType instanceof BinaryType) {
            return new String(row.getBinary(columnOrdinal));
        } else if (dataType instanceof DecimalType) {
            return row.getDecimal(columnOrdinal).toString();
        } else if (dataType instanceof StructType) {
            return "TODO: struct value";
        } else if (dataType instanceof ArrayType) {
            return "TODO: list value";
        } else if (dataType instanceof MapType) {
            return "TODO: map value";
        } else {
            throw new UnsupportedOperationException("unsupported data type: " + dataType);
        }
    }
}
