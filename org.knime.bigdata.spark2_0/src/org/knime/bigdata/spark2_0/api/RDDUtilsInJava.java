package org.knime.bigdata.spark2_0.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;
import org.knime.bigdata.spark.node.preproc.convert.category2number.NominalValueMappingFactory;

import com.knime.bigdata.spark.jobserver.server.RDDUtils;

/**
 * converts various intermediate Java RDD forms to JavaRDD of type JavaRDD[Row] or vice versa
 *
 * @author dwk
 */
@SparkClass
public class RDDUtilsInJava {

    /**
     * (Java friendly version) convert nominal values in columns for given column indices to integers and append columns
     * with mapped values
     *
     * @param inputDataset dataset to be processed
     * @param aColumnIds indices of columns to be mapped, columns that have no mapping are ignored
     * @param aMappingType indicates how values are to be mapped
     * @param aKeepOriginalColumns - keep original columns as well or not
     * @note throws SparkException thrown if no mapping is known for some value, but only when row is actually read!
     * @return dataset container with converted data (columns are appended)
     */
    public static MappedDatasetContainer convertNominalValuesForSelectedIndices(final Dataset<Row> inputDataset,
            final int[] aColumnIds, final MappingType aMappingType, final boolean aKeepOriginalColumns) {

        final Set<Integer> inputColIndices = new HashSet<>(aColumnIds.length);
        for (int i : aColumnIds) {
            inputColIndices.add(i);
        }
        final NominalValueMapping mappings = toLabelMapping(inputDataset.javaRDD(), aColumnIds, aMappingType);

        JavaRDD<Row> mappedRdd = inputDataset.javaRDD().map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row row) {
                final RowBuilder builder;
                if (aKeepOriginalColumns) {
                    builder = RowBuilder.fromRow(row);
                } else {
                    builder = dropColumnsFromRow(inputColIndices, row);
                }

                for (int ix : aColumnIds) {
                    //ignore columns that have no mapping
                    if (mappings.hasMappingForColumn(ix)) {
                        Object val = row.get(ix);
                        if (val == null) {
                            if (mappings.getType() == MappingType.BINARY) {
                                int numValues = mappings.getNumberOfValues(ix);
                                for (int i = 0; i < numValues; i++) {
                                    builder.add(0.0d);
                                }
                            } else {
                                builder.add(null);
                            }
                        } else {
                            Integer labelOrIndex = mappings.getNumberForValue(ix, val.toString());
                            if (mappings.getType() == MappingType.BINARY) {
                                int numValues = mappings.getNumberOfValues(ix);
                                for (int i = 0; i < numValues; i++) {
                                    if (labelOrIndex == i) {
                                        builder.add(1.0d);
                                    } else {
                                        builder.add(0.0d);
                                    }
                                }
                            } else {
                                builder.add(labelOrIndex.doubleValue());
                            }
                        }
                    }
                }
                return builder.build();
            }
        });

        return MappedDatasetContainer.createContainer(inputDataset, mappedRdd, aColumnIds, mappings, aKeepOriginalColumns);
    }

    /**
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
     * extract all distinct values from the given input RDD and the given indices and create a mapping from value to int
     *
     * @param aInputRdd
     * @param aNominalColumnIndices
     * @param aMappingType
     * @return mapping from distinct value to unique integer value
     */
    public static NominalValueMapping toLabelMapping(final JavaRDD<Row> aInputRdd, final int[] aNominalColumnIndices,
        final MappingType aMappingType) {

        switch (aMappingType) {
            case GLOBAL: {
                return toLabelMappingGlobalMapping(aInputRdd, aNominalColumnIndices, aMappingType);
            }
            case COLUMN: {
                return toLabelMappingColumnMapping(aInputRdd, aNominalColumnIndices, aMappingType);
            }
            case BINARY: {
                return toLabelMappingColumnMapping(aInputRdd, aNominalColumnIndices, aMappingType);
            }
            default: {
                throw new UnsupportedOperationException("ERROR: unknown mapping type !");
            }
        }
    }

    private static NominalValueMapping toLabelMappingGlobalMapping(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices, final MappingType aMappingType) {

        Map<Integer, Set<String>> labels = aggregateValues(aInputRdd, aNominalColumnIndices);

        Map<String, Integer> mappings = new HashMap<>();
        {
            Set<String> allValues = new HashSet<>();
            for (Set<String> labs : labels.values()) {
                allValues.addAll(labs);
            }

            int idx = 0;
            for (String label : allValues) {
                mappings.put(label, idx++);
            }
        }

        Map<Integer, Map<String, Integer>> labelMapping = new HashMap<>(labels.size());
        for (Map.Entry<Integer, Set<String>> entry : labels.entrySet()) {
            Set<String> values = entry.getValue();
            Map<String, Integer> mapping = new HashMap<>(values.size());
            for (String val : values) {
                mapping.put(val, mappings.get(val));
            }
            labelMapping.put(entry.getKey(), mapping);
        }
        return NominalValueMappingFactory.createColumnMapping(labelMapping, aMappingType);
    }

    private static NominalValueMapping toLabelMappingColumnMapping(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices, final MappingType aMappingType) {

        Map<Integer, Set<String>> labels = aggregateValues(aInputRdd, aNominalColumnIndices);

        Map<Integer, Map<String, Integer>> labelMapping = new HashMap<>(labels.size());
        for (Map.Entry<Integer, Set<String>> entry : labels.entrySet()) {
            int idx = 0;
            Set<String> values = entry.getValue();
            Map<String, Integer> mapping = new HashMap<>(values.size());
            for (String val : values) {
                mapping.put(val, idx++);
            }
            labelMapping.put(entry.getKey(), mapping);
        }

        return NominalValueMappingFactory.createColumnMapping(labelMapping, aMappingType);
    }

    /**
     * @param aInputRdd
     * @param aNominalColumnIndices
     * @return
     */
    private static Map<Integer, Set<String>> aggregateValues(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices) {
        Map<Integer, Set<String>> emptyMap = new HashMap<>();
        for (int ix : aNominalColumnIndices) {
            emptyMap.put(ix, new HashSet<String>());
        }

        Map<Integer, Set<String>> labels =
            aInputRdd.aggregate(emptyMap, new Function2<Map<Integer, Set<String>>, Row, Map<Integer, Set<String>>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<Integer, Set<String>> call(final Map<Integer, Set<String>> aAggregatedValues, final Row row)
                    throws Exception {
                    for (int ix : aNominalColumnIndices) {
                        Object val = row.get(ix);
                        if (val != null) {
                            //no need to add modified set as the modification is done implicitly
                            aAggregatedValues.get(ix).add(val.toString());
                        }
                    }
                    return aAggregatedValues;
                }
            }, new Function2<Map<Integer, Set<String>>, Map<Integer, Set<String>>, Map<Integer, Set<String>>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<Integer, Set<String>> call(final Map<Integer, Set<String>> aAggregatedValues0,
                    final Map<Integer, Set<String>> aAggregatedValues1) throws Exception {
                    for (Map.Entry<Integer, Set<String>> entry : aAggregatedValues0.entrySet()) {
                        entry.getValue().addAll(aAggregatedValues1.get(entry.getKey()));
                    }
                    return aAggregatedValues0;
                }
            });
        return labels;
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

        return dataset.javaRDD().map(new Function<Row, Vector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Vector call(final Row row) {
                return toVector(row, columnIndices, columnNames, isNumericCol, isBoolCol);
            }
        });
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

        return input.map(new MapFunction<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row row) throws Exception {
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
            }
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

        return input.map(new MapFunction<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row row) throws Exception {
                for (int index : columnIndices) {
                    if (row.isNullAt(index) || (numericValue[index] && Double.isNaN(((Number)row.get(index)).doubleValue()))) {
                        throw new KNIMESparkException(String.format(errorMessageFormat, columns[index]));
                    }
                }
                return row;
            }
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
     *
     * @param aUserIx
     * @param aProductIx
     * @param aRatingIx - optional ratings index, use -1 if no ratings are available
     * @param aInputRdd
     * @return ratings rdd
     */
    public static JavaRDD<Rating> convertRowRDD2RatingsRdd(final int aUserIx, final int aProductIx,
        final int aRatingIx, final JavaRDD<Row> aInputRdd) {
        final JavaRDD<Rating> ratings = aInputRdd.map(new Function<Row, Rating>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Rating call(final Row aRow) {
                if (aRatingIx > -1) {
                    return new Rating(aRow.getInt(aUserIx), aRow.getInt(aProductIx),
                        RDDUtils.getDouble(aRow, aRatingIx));
                } else {
                    return new Rating(aRow.getInt(aUserIx), aRow.getInt(aProductIx), -1);
                }
            }
        });
        return ratings;
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
}
