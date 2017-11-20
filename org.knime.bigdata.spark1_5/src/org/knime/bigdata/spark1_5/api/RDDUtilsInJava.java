package org.knime.bigdata.spark1_5.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.core.job.util.MyJoinKey;
import org.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;
import org.knime.bigdata.spark.node.preproc.convert.category2number.NominalValueMappingFactory;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizationSettings;

import com.google.common.base.Optional;
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
     * @param inputRdd Row RDD to be processed
     * @param columnIds - array of indices to be converted
     * @param columnNames - array of column names to be converted
     * @param mappingType indicates how values are to be mapped
     * @param keepOriginalColumns - keep original columns as well or not
     * @note throws SparkException thrown if no mapping is known for some value, but only when row is actually read!
     * @return container JavaRDD<Row> with original data plus appended columns and mapping
     */
    public static MappedRDDContainer convertNominalValuesForSelectedIndices(final JavaRDD<Row> inputRdd,
            final int[] columnIds, final String[] columnNames, final MappingType mappingType,
            final boolean keepOriginalColumns) {

        final NominalValueMapping mappings = toLabelMapping(inputRdd, columnIds, mappingType);
        final JavaRDD<Row> rddWithConvertedValues =
            applyLabelMapping(inputRdd, columnIds, mappings, keepOriginalColumns);

        return MappedRDDContainer.createContainer(rddWithConvertedValues, columnIds, columnNames, mappings, keepOriginalColumns);
    }

    /**
     * apply the given mapping to the given input RDD
     *
     * @param aInputRdd
     * @param aColumnIds indices of columns to be mapped, columns that have no mapping are ignored
     * @param aMappings
     * @param aKeepOriginalColumns - keep original columns as well or not
     * @note throws SparkException thrown if no mapping is known for some value, but only when row is actually read!
     * @return JavaRDD<Row> with converted data (columns are appended)
     */
    public static JavaRDD<Row> applyLabelMapping(final JavaRDD<Row> aInputRdd, final int[] aColumnIds,
        final NominalValueMapping aMappings, final boolean aKeepOriginalColumns) {

        JavaRDD<Row> rddWithConvertedValues = aInputRdd.map(new Function<Row, Row>() {
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
                    if (aMappings.hasMappingForColumn(ix)) {
                        Object val = row.get(ix);
                        if (val == null) {
                            builder.add(null);
                        } else {
                            Integer labelOrIndex = aMappings.getNumberForValue(ix, val.toString());
                            if (aMappings.getType() == MappingType.BINARY) {
                                int numValues = aMappings.getNumberOfValues(ix);
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
        return rddWithConvertedValues;
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
     * convert given RDD to an RDD<Vector> with selected columns and compute statistics for these columns
     *
     * @param aInputRdd
     * @param aColumnIndices
     * @return MultivariateStatisticalSummary
     */
    public static MultivariateStatisticalSummary findColumnStats(final JavaRDD<Row> aInputRdd,
        final Collection<Integer> aColumnIndices) {

        List<Integer> columnIndices = new ArrayList<>();
        columnIndices.addAll(aColumnIndices);
        Collections.sort(columnIndices);

        JavaRDD<Vector> mat = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(aInputRdd, columnIndices);

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        return summary;
    }

    /**
     * computes the scale and translation parameters from the given data and according to the given normalization
     * settings, then applies these parameters to the input RDD
     *
     * @param aInputRdd
     * @param aColumnIndices indices of numeric columns to be normalized
     * @param aNormalization
     * @return container with normalization parameters for each of the given columns, other columns are just copied over
     */
    public static NormalizedRDDContainer normalize(final JavaRDD<Row> aInputRdd,
        final Collection<Integer> aColumnIndices, final NormalizationSettings aNormalization) {

        MultivariateStatisticalSummary stats = findColumnStats(aInputRdd, aColumnIndices);
        final NormalizedRDDContainer rddNormalizer =
            NormalizedRDDContainerFactory.getNormalizedRDDContainer(stats, aNormalization);

        rddNormalizer.normalizeRDD(aInputRdd, aColumnIndices);
        return rddNormalizer;
    }

    /**
     * applies the given the scale and translation parameters to the given data to the input RDD
     *
     * @param aInputRdd
     * @param aColumnIndices indices of numeric columns to be normalized
     * @param aScalesAndTranslations normalization parameters
     * @return container with normalization parameters for each of the given columns, other columns are just copied over
     */
    public static NormalizedRDDContainer normalize(final JavaRDD<Row> aInputRdd,
        final Collection<Integer> aColumnIndices, final Double[][] aScalesAndTranslations) {

        final NormalizedRDDContainer rddNormalizer =
            NormalizedRDDContainerFactory.getNormalizedRDDContainer(aScalesAndTranslations[0],
                aScalesAndTranslations[1]);

        rddNormalizer.normalizeRDD(aInputRdd, aColumnIndices);
        return rddNormalizer;
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

    private static JavaRDD<Vector> toVectorRdd(final JavaRDD<Row> inputRdd, final List<Integer> aColumnIndices) {
        final int numFeatures = Math.min(aColumnIndices.size(), inputRdd.take(1).get(0).length());

        return inputRdd.map(new Function<Row, Vector>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Vector call(final Row row) {
                int insertionIndex = 0;
                final double[] convertedValues = new double[numFeatures];
                for (int idx : aColumnIndices) {
                    if (idx < row.length()) {
                        convertedValues[insertionIndex] = RDDUtils.getDouble(row, idx);
                        insertionIndex += 1;
                    }
                }

                return Vectors.dense(convertedValues);
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
     * convert the given JavaRDD of Row to a RowMatrix
     *
     * @param aRowRDD
     * @param aColumnIds - indices of columns to select
     * @return converted RowMatrix
     */
    public static RowMatrix toRowMatrix(final JavaRDD<Row> aRowRDD, final List<Integer> aColumnIds) {
        final JavaRDD<Vector> vectorRDD = toVectorRdd(aRowRDD, aColumnIds);
        return new RowMatrix(vectorRDD.rdd());
    }

    /**
     * convert the given RowMatrix to a JavaRDD of Row
     *
     * @param aRowMatrix
     * @return converted JavaRDD
     */
    public static JavaRDD<Row> fromRowMatrix(final RowMatrix aRowMatrix) {
        final RDD<Vector> rows = aRowMatrix.rows();
        final JavaRDD<Vector> vectorRows = new JavaRDD<>(rows, rows.elementClassTag());
        return fromVectorRdd(vectorRows);
    }

    /**
     * convert the given Matrix to a JavaRDD of Row
     *
     * @param aContext java context, required for RDD construction
     *
     * @param aMatrix
     * @return converted JavaRDD
     */
    public static JavaRDD<Row> fromMatrix(final JavaSparkContext aContext, final Matrix aMatrix) {
        final int nRows = aMatrix.numRows();
        final int nCols = aMatrix.numCols();
        final List<Row> rows = new ArrayList<>(nRows);
        for (int i = 0; i < nRows; i++) {
            RowBuilder builder = RowBuilder.emptyRow();
            for (int j = 0; j < nCols; j++) {
                builder.add(aMatrix.apply(i, j));
            }
            rows.add(builder.build());
        }
        return aContext.parallelize(rows);
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
