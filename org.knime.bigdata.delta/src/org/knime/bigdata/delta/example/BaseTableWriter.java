/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

public class BaseTableWriter {

    protected Engine engine = DefaultEngine.create(new Configuration());

    /**
     * Schema used in examples for table create and/or writes
     */
    protected final StructType exampleTableSchema = new StructType().add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING).add("address", StringType.STRING).add("salary", DoubleType.DOUBLE);

    /**
     * Schema and partition columns used in examples for partitioned table create and/or writes.
     */
    protected final StructType examplePartitionedTableSchema = new StructType().add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING).add("city", StringType.STRING).add("salary", DoubleType.DOUBLE);

    protected final List<String> examplePartitionColumns = Collections.singletonList("city");

    void verifyCommitSuccess(final String tablePath, final TransactionCommitResult result) {
        // Verify the commit was successful
        if (result.getVersion() >= 0) {
            System.out.println("Table created successfully at: " + tablePath);
        } else {
            // This should never happen. If there is a reason for table be not created
            // `Transaction.commit` always throws an exception.
            throw new RuntimeException("Table creation failed");
        }
    }

    /**
     * Create data batch for a un-partitioned table with schema {@link #exampleTableSchema}.
     *
     * @param offset Offset that affects the generated data.
     * @return
     */
    FilteredColumnarBatch generateUnpartitionedDataBatch(final int offset) {
        ColumnVector[] vectors = new ColumnVector[exampleTableSchema.length()];
        // Create a batch with 5 rows

        // id
        vectors[0] = intVector(Arrays.asList(offset, 1 + offset, 2 + offset, 3 + offset, 4 + offset));

        // name
        vectors[1] = stringVector(Arrays.asList("Alice", "Bob", "Charlie", "David", "Eve"));

        // address
        vectors[2] =
            stringVector(Arrays.asList("123 Main St", "456 Elm St", "789 Cedar St", "101 Oak St", "121 Pine St"));

        // salary
        vectors[3] = doubleVector(
            Arrays.asList(100.0d + offset, 200.0d + offset, 300.0d + offset, 400.0d + offset, 500.0d + offset));

        ColumnarBatch batch = new DefaultColumnarBatch(5, exampleTableSchema, vectors);
        return new FilteredColumnarBatch(batch, // data
            // Optional selection vector. If want to write only a subset of rows from the batch.
            Optional.empty());
    }

    /**
     * Create data batch for a partitioned table with schema {@link #examplePartitionedTableSchema}.
     *
     * @param offset Offset that affects the generated data.
     * @param city City value for the partition column.
     * @return
     */
    FilteredColumnarBatch generatedPartitionedDataBatch(final int offset, final String city) {
        ColumnVector[] vectors = new ColumnVector[examplePartitionedTableSchema.length()];
        // Create a batch with 5 rows

        // id
        vectors[0] = intVector(Arrays.asList(offset, 1 + offset, 2 + offset, 3 + offset, 4 + offset));

        // name
        vectors[1] = stringVector(Arrays.asList("Alice", "Bob", "Charlie", "David", "Eve"));

        // city - given city is a partition column we expect the batch to contain the same
        // value for all rows.
        vectors[2] = stringSingleValueVector(city, 5);

        // salary
        vectors[3] = doubleVector(
            Arrays.asList(100.0d + offset, 200.0d + offset, 300.0d + offset, 400.0d + offset, 500.0d + offset));

        ColumnarBatch batch = new DefaultColumnarBatch(5, examplePartitionedTableSchema, vectors);
        return new FilteredColumnarBatch(batch, // data
            // Optional selection vector. If want to write only a subset of rows from the batch.
            Optional.empty());
    }

    //////////////////////// Helper methods to create ColumnVectors ////////////////////////
    // These are sample vectors which can be created as wrappers as engine specific       //
    // vector types.                                                                      //
    ////////////////////////////////////////////////////////////////////////////////////////
    static ColumnVector intVector(final List<Integer> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return IntegerType.INTEGER;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(final int rowId) {
                return false;
            }

            @Override
            public int getInt(final int rowId) {
                return data.get(rowId);
            }
        };
    }

    static ColumnVector doubleVector(final List<Double> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return DoubleType.DOUBLE;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(final int rowId) {
                return data.get(rowId) == null;
            }

            @Override
            public double getDouble(final int rowId) {
                return data.get(rowId);
            }
        };
    }

    static ColumnVector stringVector(final List<String> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return StringType.STRING;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(final int rowId) {
                return data.get(rowId) == null;
            }

            @Override
            public String getString(final int rowId) {
                return data.get(rowId);
            }
        };
    }

    static ColumnVector stringSingleValueVector(final String value, final int size) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return StringType.STRING;
            }

            @Override
            public int getSize() {
                return size;
            }

            @Override
            public void close() {

            }

            @Override
            public boolean isNullAt(final int rowId) {
                return value == null;
            }

            @Override
            public String getString(final int rowId) {
                return value;
            }
        };
    }

}