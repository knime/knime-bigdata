package org.knime.bigdata.spark2_2.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.core.job.util.MyJoinKey;
import org.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;
import org.knime.bigdata.spark.node.preproc.convert.category2number.NominalValueMappingFactory;

import com.knime.bigdata.spark.jobserver.server.RDDUtils;

import scala.Tuple2;

/**
 * converts various intermediate Java RDD forms to JavaRDD of type JavaRDD[Row] or vice versa
 *
 * @author dwk
 */
@SparkClass
public class RDDUtilsInJava {

    //TODO: Improve missing value handling in scala     RDDUtils line 157 in getDouble() method

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

        final NominalValueMapping mappings = toLabelMapping(inputDataset.javaRDD(), aColumnIds, aMappingType);
        JavaRDD<Row> mappedRdd = inputDataset.javaRDD().map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row row) {
                final RowBuilder builder;
                if (aKeepOriginalColumns) {
                    builder = RowBuilder.fromRow(row);
                } else {
                    builder = dropColumnsFromRow(aColumnIds, row);
                }
                for (int ix : aColumnIds) {
                    //ignore columns that have no mapping
                    if (mappings.hasMappingForColumn(ix)) {
                        Object val = row.get(ix);
                        if (val == null) {
                            builder.add(null);
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
     * Create a number to category schema.
     * @param dataset
     * @param map
     * @param keepOriginalColumns
     * @param colSuffix - column suffix to append
     * @return Number to category schema
     */
    public static StructType createSchema(final Dataset<Row> dataset, final ColumnBasedValueMapping map,
            final boolean keepOriginalColumns, final String colSuffix) {

        StructType schema = dataset.schema();
        List<Integer> columnIdsList = map.getColumnIndices();
        Collections.sort(columnIdsList);
        List<StructField> fields = new ArrayList<>();
        StructField oldFields[] = schema.fields();

        for (int i = 0; i < oldFields.length; i++) {
            if (keepOriginalColumns || !columnIdsList.contains(i)) {
                fields.add(oldFields[i]);
            }
        }

        for (int idx : columnIdsList) {
            String name = oldFields[idx].name() + colSuffix;
            fields.add(DataTypes.createStructField(name, DataTypes.StringType, false));
        }

        return DataTypes.createStructType(fields);
    }

    /**
     * @param aColumnIdsToDrop
     * @param aRow
     * @return rowBuilder with subset of columns already added
     */
    public static RowBuilder dropColumnsFromRow(final List<Integer> aColumnIdsToDrop, final Row aRow) {
        final RowBuilder builder;
        builder = RowBuilder.emptyRow();
        for (int ix = 0; ix < aRow.length(); ix++) {
            if (!aColumnIdsToDrop.contains(ix)) {
                builder.add(aRow.get(ix));
            }
        }
        return builder;
    }

    private static RowBuilder dropColumnsFromRow(final int[] aColumnIdsToDrop, final Row aRow) {
        List<Integer> cols = new ArrayList<>();
        for (int ix : aColumnIdsToDrop) {
            cols.add(ix);
        }
        return dropColumnsFromRow(cols, aRow);
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
     * Note that now only the class label is converted (no matter whether it is already numeric or not)
     *
     * convert a RDD of Rows to JavaRDD of LabeledPoint, the string label column is converted to integer values, all
     * other indices must be numeric
     *
     * @param aInputRdd Row RDD to be converted
     * @param aColumnIndices column selector (and, possibly, re-ordering)
     * @param aLabelColumnIndex index of label column (can be numeric or string)
     * @return container with mapped data and mapping
     * @throws IllegalArgumentException if values are encountered that are neither numeric nor string
     */
    public static LabeledDataInfo toJavaLabeledPointRDDConvertNominalValues(final JavaRDD<Row> aInputRdd,
        final List<Integer> aColumnIndices, final int aLabelColumnIndex) {
        final NominalValueMapping labelMapping =
            toLabelMapping(aInputRdd, new int[]{aLabelColumnIndex}, MappingType.COLUMN);
        final JavaRDD<LabeledPoint> labeledRdd =
            toLabeledVectorRdd(aInputRdd, aColumnIndices, aLabelColumnIndex, labelMapping);

        return new LabeledDataInfo(labeledRdd, labelMapping);
    }

    private static JavaRDD<LabeledPoint>
        toLabeledVectorRdd(final JavaRDD<Row> inputRdd, final List<Integer> aColumnIndices, final int labelColumnIndex, final NominalValueMapping labelMapping) {
        final int numFeatures = Math.min(aColumnIndices.size(), inputRdd.take(1).get(0).length() - 1);

        return inputRdd.map(new Function<Row, LabeledPoint>() {
            private static final long serialVersionUID = 1L;

            @Override
            public LabeledPoint call(final Row row) {
                int insertionIndex = 0;
                final double[] convertedValues = new double[numFeatures];
                for (int idx : aColumnIndices) {
                    if (idx != labelColumnIndex && idx < row.length()) {
                        convertedValues[insertionIndex] = RDDUtils.getDouble(row, idx);
                        insertionIndex += 1;
                    }
                }
                final double label;
                if (labelMapping != null) {
                    label =
                        labelMapping.getNumberForValue(labelColumnIndex, row.get(labelColumnIndex).toString())
                            .doubleValue();
                } else {
                    //no mapping given - label must already be numeric
                    label = RDDUtils.getDouble(row, labelColumnIndex);
                }
                return new LabeledPoint(label, Vectors.dense(convertedValues));
            }
        });
    }

    /**
     * Convert given data set into an RDD of vectors containing values of given column indices.
     *
     * @param dataset input data set
     * @param columnIndices indices of columns to use
     * @return RDD with vectors
     */
    public static JavaRDD<Vector> toVectorRdd(final Dataset<Row> dataset, final List<Integer> columnIndices) {
        return toVectorRdd(dataset.javaRDD(), columnIndices);
    }

    private static JavaRDD<Vector> toVectorRdd(final JavaRDD<Row> inputRdd, final List<Integer> columnIndices) {
        return toVectorRdd(inputRdd, columnIndices.toArray(new Integer[0]));
    }

    private static JavaRDD<Vector> toVectorRdd(final JavaRDD<Row> inputRdd, final Integer columnIndices[]) {
        return inputRdd.map(new Function<Row, Vector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Vector call(final Row row) {
                final double[] values = new double[columnIndices.length];
                for (int i = 0; i < columnIndices.length; i++) {
                    values[i] = row.getDouble(columnIndices[i]);
                }
                return Vectors.dense(values);
            }
        });
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
     *
     * convert a RDD of Rows to JavaRDD of LabeledPoint, all selected row values must be numeric
     *
     * @param aInputRdd Row RDD to be converted
     * @param aColumnIndices column selector (and, possibly, re-ordering)
     * @param aLabelColumnIndex index of label column (must be numeric)
     * @return container with mapped data and mapping
     * @throws IllegalArgumentException if values are encountered that are not numeric
     */
    public static JavaRDD<LabeledPoint> toJavaLabeledPointRDD(final JavaRDD<Row> aInputRdd,
        final List<Integer> aColumnIndices, final int aLabelColumnIndex) {
        return toLabeledVectorRdd(aInputRdd, aColumnIndices, aLabelColumnIndex, null);
    }

    /**
    *
    * sub-select given columns by index from the given RDD and put result into new RDD
    *
    * @param aInputRdd Row RDD to be converted
    * @param aColumnIndices column selector (and, possibly, re-ordering)
    * @return RDD with selected columns and same number of rows as original
    * @throws IllegalArgumentException if values are encountered that are not numeric
    */
    public static JavaRDD<Row> selectColumnsFromRDD(final JavaRDD<Row> aInputRdd, final List<Integer> aColumnIndices) {
       return aInputRdd.map(new Function<Row, Row>() {
           private static final long serialVersionUID = 1L;

           @Override
           public Row call(final Row row) {
               RowBuilder rb = RowBuilder.emptyRow();
               for (int idx : aColumnIndices) {
                   rb.add(row.get(idx));
               }
               return rb.build();
           }
       });
   }

    /**
     * extracts the given keys from the given rdd and constructs a pair rdd from it
     *
     * @param aRdd Row JavaRDD to be converted
     * @param aKeys keys to be extracted
     * @return pair rdd with keys and original rows as values (no columns are filtered out)
     */
    public static JavaPairRDD<MyJoinKey, Row> extractKeys(final JavaRDD<Row> aRdd, final Integer[] aKeys) {
        return aRdd.mapToPair(new PairFunction<Row, MyJoinKey, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<MyJoinKey, Row> call(final Row aRow) throws Exception {
                final Object[] keyValues = new Object[aKeys.length];
                int ix = 0;
                for (int keyIx : aKeys) {
                    keyValues[ix++] = aRow.get(keyIx);
                }
                return new Tuple2<>(new MyJoinKey(keyValues), aRow);
            }
        });
    }

    /**
     * <L> and <R> must be either of class Row or of class Optional<Row>
     *
     * @param aTuples
     * @param aColIdxLeft - indices of <L> to be kept
     * @param aColIdxRight - indices of <R> to be kept
     * @return corresponding rows from left and right merged into instances of Row (one for each original pair),
     *         possibly with null values for outer joins
     */
    public static <L, R> JavaRDD<Row> mergeRows(final JavaRDD<Tuple2<L, R>> aTuples, final List<Integer> aColIdxLeft,
        final List<Integer> aColIdxRight) {
        return aTuples.map(new Function<Tuple2<L, R>, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Tuple2<L, R> aTuple) throws Exception {
                RowBuilder builder = RowBuilder.emptyRow();
                extractColumns(aColIdxLeft, aTuple._1, builder);
                extractColumns(aColIdxRight, aTuple._2, builder);
                return builder.build();
            }

        });
    }

    /**
     * @param aColIdxLeft
     * @param aTuple
     * @param builder
     */
    @SuppressWarnings("unchecked")
    private static <J> void extractColumns(final List<Integer> aColIdx, final J aRow, final RowBuilder builder) {
        for (int ix : aColIdx) {
            if (aRow instanceof Row) {
                builder.add(((Row)aRow).get(ix));
            } else if ((aRow instanceof Optional<?>) && ((Optional<Row>)aRow).isPresent()) {
                builder.add(((Optional<Row>)aRow).get().get(ix));
            } else {
                builder.add(null);
            }
        }
    }

    /**
     * Creates a list of fields matching schema of rows produced by {@link #mergeRows(JavaRDD, List, List)}.
     *
     * @param left - left data frame
     * @param aColIdxLeft - indices of <L> to be kept
     * @param right - right data frame
     * @param aColIdxRight - indices of <R> to be kept
     * @return list of fields
     */
    public static List<StructField> getFields(final Dataset<Row> left, final List<Integer> aColIdxLeft,
            final Dataset<Row> right, final List<Integer> aColIdxRight) {

        final List<StructField> fields = new ArrayList<>(aColIdxLeft.size() + aColIdxRight.size());
        final StructField leftFields[] = left.schema().fields();
        final StructField rightFields[] = right.schema().fields();

        for (int index : aColIdxLeft) {
            fields.add(leftFields[index]);
        }

        for (int index : aColIdxRight) {
            fields.add(rightFields[index]);
        }

        return fields;
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
     * Convert the given Matrix to a data frame.
     *
     * @param context spark context to use for data frame construction
     * @param matrix matrix to convert
     * @param columnPrefix prefix of output column names
     * @return converted matrix as data frame
     */
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
