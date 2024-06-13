package org.knime.bigdata.spark3_5.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;

import scala.Tuple2;

/**
 * converts various intermediate Java RDD forms to JavaRDD of type JavaRDD[Row] or vice versa
 *
 * @author dwk
 */
@SparkClass
public class RDDUtilsInJava {

    /**
     * Drops the given columns from the given row.
     *
     * @param aColumnIdsToDrop
     * @param aRow
     * @return rowBuilder with subset of columns already added
     */
    public static RowBuilder dropColumnsFromRow(final Set<Integer> aColumnIdsToDrop, final Row aRow) {
        final RowBuilder builder;
        builder = RowBuilder.emptyRow();
        for (int ix = 0; ix < aRow.length(); ix++) {
            if (!aColumnIdsToDrop.contains(ix)) {
                builder.add(aRow.get(ix));
            }
        }
        return builder;
    }


    /**
     * Convert a dataset of Rows to JavaRDD of LabeledPoint, all selected row values must be numeric or booleans.
     *
     * @param dataset set of rows to be converted
     * @param columnIndicesList column selector (and, possibly, re-ordering)
     * @param labelColumnIndex index of label column (must be numeric)
     * @return container with mapped data and mapping
     * @throws IllegalArgumentException if values are encountered that are not numeric, <code>null</code> or <code>NaN</code>
     */
    public static JavaRDD<LabeledPoint> toLabeledPointRDD(final Dataset<Row> dataset,
        final List<Integer> columnIndicesList, final int labelColumnIndex) {

        final StructField[] fields = dataset.schema().fields();
        final String[] columnNames = dataset.columns();
        final Integer[] columnIndices = columnIndicesList.toArray(new Integer[0]);
        final boolean[] isNumericCol = isNumericCol(fields);
        final boolean[] isBoolCol = isBoolCol(fields);

        return dataset.javaRDD().map(new Function<Row, LabeledPoint>() {
            private static final long serialVersionUID = 1L;

            @Override
            public LabeledPoint call(final Row row) {
                return new LabeledPoint(
                    getDouble(row, labelColumnIndex, columnNames[labelColumnIndex], isNumericCol[labelColumnIndex], isBoolCol[labelColumnIndex]),
                    toVector(row, columnIndices, columnNames, isNumericCol, isBoolCol));
            }
        });
    }

    /**
     * Convert given dataset of rows into an RDD of vectors containing values of given column indices.
     *
     * @param dataset input data of rows
     * @param columnIndicesList indices of columns to use
     * @return RDD with vectors
     */
    public static JavaRDD<Vector> toVectorRdd(final Dataset<Row> dataset, final List<Integer> columnIndicesList) {
        final StructField[] fields = dataset.schema().fields();
        final String[] columnNames = dataset.columns();
        final Integer[] columnIndices = columnIndicesList.toArray(new Integer[0]);
        final boolean[] isNumericCol = isNumericCol(fields);
        final boolean[] isBoolCol = isBoolCol(fields);

        return dataset.javaRDD().map((row) -> toVector(row, columnIndices, columnNames, isNumericCol, isBoolCol));
    }

    private static Vector toVector(final Row row, final Integer[] columnIndices, final String[] columnNames,
        final boolean[] numericCols, final boolean[] boolCols) {

        final double[] values = new double[columnIndices.length];
        for (int i = 0; i < columnIndices.length; i++) {
            final int colIndex = columnIndices[i];
            values[i] = getDouble(row, colIndex, columnNames[colIndex], numericCols[colIndex], boolCols[colIndex]);
        }
        return Vectors.dense(values);
    }

    /**
     * Returns a numeric or boolean value as double and fails on other types, <code>null</code> or <code>NaN</code>
     * values. This is the same behavior like the VectorAssembler in Spark has, therefore use this method only to create
     * vectors where VectorAssembler is not an option (RDD/Spark 1.x).
     *
     * @param row with numeric or boolean values
     * @param index column index to extract
     * @param name column name (for error messages)
     * @param isNumeric <code>true</code> if value at index is numeric
     * @param isBool <code>true</code> if value at index is a boolean
     * @return value as double
     * @throws IllegalArgumentException on <code>null</code> or <code>NaN</code> values
     * @throws IllegalArgumentException if value is not a numeric or boolean value
     */
    public static double getDouble(final Row row, final int index, final String name, final boolean isNumeric, final boolean isBool) {
        final Object o = row.get(index);

        if (o == null) {
            throw new IllegalArgumentException(
                String.format("Unsupported missing value at column '%s' detected.", name));
        } else if (isNumeric) {
            final double d = ((Number)o).doubleValue();
            if (Double.isNaN(d)) {
                throw new IllegalArgumentException(
                    String.format("Unsupported NaN value at column '%s' detected.", name));
            } else {
                return d;
            }
        } else if (isBool) {
            return ((boolean)o) ? 1d : 0d;
        } else {
            throw new IllegalArgumentException(
                String.format("Unsupported non-numeric value type '%s' at column '%s' detected.",
                    o.getClass(), name));
        }
    }

    /** validates if given column indices are of given type */
    private static boolean[] isNumericCol(final StructField[] fields) {
        final boolean[] isOfType = new boolean[fields.length];
        for (int i = 0; i < fields.length; i++) {
            isOfType[i] = fields[i].dataType() instanceof NumericType;
        }
        return isOfType;
    }

    /** validates if given column indices are of given type */
    private static boolean[] isBoolCol(final StructField[] fields) {
        final boolean[] isOfType = new boolean[fields.length];
        for (int i = 0; i < fields.length; i++) {
            isOfType[i] = fields[i].dataType().equals(DataTypes.BooleanType);
        }
        return isOfType;
    }

    private static JavaRDD<Row> fromVectorRdd(final JavaRDD<Vector> aInputRdd) {
        return aInputRdd.map(new Function<Vector, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Vector row) {
                final double[] values = row.toArray();
                RowBuilder builder = RowBuilder.emptyRow();
                for (int i = 0; i < values.length; i++) {
                    builder.add(values[i]);
                }
                return builder.build();
            }
        });
    }

    /**
     * Convert the given data frame of rows to a {@link RowMatrix}.
     *
     * @param dataset input data frame
     * @param columnIndices indices of columns to use
     * @return converted RowMatrix
     */
    public static RowMatrix toRowMatrix(final Dataset<Row> dataset, final List<Integer> columnIndices) {
        return new RowMatrix(toVectorRdd(dataset, columnIndices).rdd());
    }

    /**
     * Convert the given {@link RowMatrix} to a data frame of rows.
     *
     * @param context spark context to use to create data frames
     * @param rowMatrix matrix to convert
     * @param columnPrefix prefix of output column names
     * @return data set of rows
     */
    public static Dataset<Row> fromRowMatrix(final SparkContext context, final RowMatrix rowMatrix, final String columnPrefix) {
        final JavaRDD<Row> vectorRows = fromVectorRdd(rowMatrix.rows().toJavaRDD());
        return createDoubleDataFrame(context, vectorRows, (int) rowMatrix.numCols(), columnPrefix);
    }

    /**
     * Convert the given Matrix to a data frame (ML version).
     *
     * @param context spark context to use for data frame construction
     * @param matrix matrix to convert
     * @param columnPrefix prefix of output column names
     * @return converted matrix as data frame
     */
    @SuppressWarnings("resource")
    public static Dataset<Row> fromMatrix(final SparkContext context, final DenseMatrix matrix, final String columnPrefix) {
        final int nRows = matrix.numRows();
        final int nCols = matrix.numCols();
        final List<Row> rows = new ArrayList<>(nRows);
        for (int i = 0; i < nRows; i++) {
            RowBuilder builder = RowBuilder.emptyRow();
            for (int j = 0; j < nCols; j++) {
                builder.add(matrix.apply(i, j));
            }
            rows.add(builder.build());
        }
        final JavaSparkContext javaContext = JavaSparkContext.fromSparkContext(context);
        final JavaRDD<Row> resultRdd = javaContext.parallelize(rows);
        return createDoubleDataFrame(context, resultRdd, nCols, columnPrefix);
    }

    /**
     * Convert the given Matrix to a data frame (MLlib version).
     *
     * @param context spark context to use for data frame construction
     * @param matrix matrix to convert
     * @param columnPrefix prefix of output column names
     * @return converted matrix as data frame
     */
    @SuppressWarnings("resource")
    public static Dataset<Row> fromMatrix(final SparkContext context, final Matrix matrix, final String columnPrefix) {
        final int nRows = matrix.numRows();
        final int nCols = matrix.numCols();
        final List<Row> rows = new ArrayList<>(nRows);
        for (int i = 0; i < nRows; i++) {
            RowBuilder builder = RowBuilder.emptyRow();
            for (int j = 0; j < nCols; j++) {
                builder.add(matrix.apply(i, j));
            }
            rows.add(builder.build());
        }
        final JavaSparkContext javaContext = JavaSparkContext.fromSparkContext(context);
        final JavaRDD<Row> resultRdd = javaContext.parallelize(rows);
        return createDoubleDataFrame(context, resultRdd, nCols, columnPrefix);
    }

    /**
     * Create a data frame containing double columns named by index and given column prefix.
     */
    @SuppressWarnings("resource")
    private static Dataset<Row> createDoubleDataFrame(final SparkContext sparkContext, final JavaRDD<Row> rdd, final int numColumns, final String columnPrefix) {
        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        final List<StructField> fields = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            fields.add(DataTypes.createStructField(String.format("%s%d", columnPrefix, i), DataTypes.DoubleType, false));
        }
        final StructType schema = DataTypes.createStructType(fields);

        return spark.createDataFrame(rdd, schema);
    }

    /**
     * Explode a {@link org.apache.spark.ml.linalg.Vector} column into n double columns (ML vector version).
     *
     * @param input rows with double vectors
     * @param vectorColumn name of column with vectors
     * @param n dimensions of vector
     * @param columnPrefix output column prefix
     * @return rows with extracted double columns and removed vector column
     */
    public static Dataset<Row> explodeVector(final Dataset<Row> input, final String vectorColumn, final int n, final String columnPrefix) {
        final ArrayList<StructField> outputFields = new ArrayList<>();
        for (StructField field : input.schema().fields()) {
            if (!field.name().equals(vectorColumn)) {
                outputFields.add(field);
            }
        }
        for (int i = 0; i < n; i++) {
            outputFields.add(DataTypes.createStructField(String.format("%s%d", columnPrefix, i), DataTypes.DoubleType, false));
        }
        final StructType outputSchema = DataTypes.createStructType(outputFields);
        final int vectorColIdx = input.schema().fieldIndex(vectorColumn);

        return input.map((MapFunction<Row, Row>) row -> {
            final RowBuilder rb = RowBuilder.emptyRow();
            for (int i = 0; i < row.length(); i++) {
                if (i != vectorColIdx) {
                    rb.add(row.get(i));
                }
            }
            for (double value : ((org.apache.spark.ml.linalg.Vector)row.get(vectorColIdx)).toArray()) {
                rb.add(value);
            }
            return rb.build();
        }, RowEncoder.apply(outputSchema));
    }

    /**
     * Validates that given data set has values on given columns or fails with given error message.
     *
     * @param input data set to validate
     * @param columnIndices columns to check
     * @param errorMessageFormat error message to throw on <code>null</code> or <code>NaN</code> values. The message
     *            will be formated with the column name as first argument
     * @return input data set
     */
    public static Dataset<Row> failOnMissingValues(final Dataset<Row> input, final List<Integer> columnIndices, final String errorMessageFormat) {
        final String[] columns = input.columns();
        final boolean[] numericValue = new boolean[columns.length];

        for (int index : columnIndices) {
            numericValue[index] = input.schema().apply(index).dataType() instanceof NumericType;
        }

        return input.map((MapFunction<Row, Row>) row -> {
            for (int index : columnIndices) {
                if (row.isNullAt(index) || (numericValue[index] && Double.isNaN(((Number)row.get(index)).doubleValue()))) {
                    throw new KNIMESparkException(String.format(errorMessageFormat, columns[index]));
                }
            }
            return row;
        }, RowEncoder.apply(input.schema()));
    }

    /**
     * Validates that schema of given data set has numeric, boolean or vector types at given columns indices or fails
     * with given error message.
     *
     * @param input data set to validate
     * @param columnIndices columns to check
     * @param errorMessage error message to throw on non numeric columns
     * @throws KNIMESparkException on numeric columns
     */
    public static void failOnNonNumericColumn(final Dataset<Row> input, final List<Integer> columnIndices,
        final String errorMessage) throws KNIMESparkException {

        final StructField fields[] = input.schema().fields();
        for (int index : columnIndices) {
            if (!(fields[index].dataType() instanceof NumericType)
                    && !(fields[index].dataType() instanceof BooleanType)
                    && !(fields[index].dataType() instanceof VectorUDT)) {
                throw new KNIMESparkException(String.format(errorMessage, fields[index].name()));
            }
        }
    }

    /**
     * converts ratings to rows
     *
     * @param aInputRdd
     * @return JavaRDD of Rows
     */
    public static JavaRDD<Row> convertRatings2RowRDDRdd(final JavaRDD<Rating> aInputRdd) {
        JavaRDD<Row> rows = aInputRdd.map(new Function<Rating, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Rating aRating) {
                RowBuilder rb = RowBuilder.emptyRow();
                rb.add(aRating.user()).add(aRating.product()).add(aRating.rating());
                return rb.build();
            }
        });
        return rows;
    }

    /**
     * count the number of times each pair of values of the given two indices occurs in the rdd
     *
     * @param aInputRdd
     * @param aIndex1 - first index in pair
     * @param aIndex2 - second index in pair
     * @return map with counts for all pairs of values that occur at least once
     */
    public static Map<Tuple2<Object, Object>, Integer> aggregatePairs(final JavaRDD<Row> aInputRdd, final int aIndex1,
        final int aIndex2) {
        Map<Tuple2<Object, Object>, Integer> emptyMap = new HashMap<>();

        Map<Tuple2<Object, Object>, Integer> counts =
            aInputRdd
                .aggregate(
                    emptyMap,
                    new Function2<Map<Tuple2<Object, Object>, Integer>, Row, Map<Tuple2<Object, Object>, Integer>>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Map<Tuple2<Object, Object>, Integer> call(
                            final Map<Tuple2<Object, Object>, Integer> aAggregatedValues, final Row row)
                            throws Exception {

                            Object val1 = row.get(aIndex1);
                            Object val2 = row.get(aIndex2);
                            final Tuple2<Object, Object> key = new Tuple2<>(val1, val2);
                            final Integer count;
                            if (aAggregatedValues.containsKey(key)) {
                                count = aAggregatedValues.get(key) + 1;
                            } else {
                                count = 1;
                            }
                            aAggregatedValues.put(key, count);
                            return aAggregatedValues;
                        }
                    },
                    new Function2<Map<Tuple2<Object, Object>, Integer>, Map<Tuple2<Object, Object>, Integer>, Map<Tuple2<Object, Object>, Integer>>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Map<Tuple2<Object, Object>, Integer> call(
                            final Map<Tuple2<Object, Object>, Integer> aAggregatedValues0,
                            final Map<Tuple2<Object, Object>, Integer> aAggregatedValues1) throws Exception {
                            for (Map.Entry<Tuple2<Object, Object>, Integer> entry : aAggregatedValues0.entrySet()) {
                                if (aAggregatedValues1.containsKey(entry.getKey())) {
                                    final Integer val = aAggregatedValues1.remove(entry.getKey());
                                    aAggregatedValues0.put(entry.getKey(), entry.getValue() + val);
                                }
                            }
                            //copy remaining values over
                            aAggregatedValues0.putAll(aAggregatedValues1);
                            return aAggregatedValues0;
                        }
                    });
        return counts;
    }
}
