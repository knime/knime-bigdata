package com.knime.bigdata.spark.jobserver.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;

/**
 * converts various intermediate Java RDD forms to JavaRDD of type JavaRDD[Row] or vice versa
 *
 * @author dwk
 */
public class RDDUtilsInJava {

    /**
     * (Java friendly version) convert nominal values in columns for given column indices to integers and append columns
     * with mapped values
     *
     * @param aInputRdd Row RDD to be processed
     * @param aColumnIds array of indices to be converted
     * @param aMappingType indicates how values are to be mapped
     * @return container JavaRDD<Row> with original data plus appended columns and mapping
     */
    public static MappedRDDContainer convertNominalValuesForSelectedIndices(final JavaRDD<Row> aInputRdd,
        final int[] aColumnIds, final MappingType aMappingType) {
        final NominalValueMapping mappings = toLabelMapping(aInputRdd, aColumnIds, aMappingType);

        final JavaRDD<Row> rddWithConvertedValues = applyLabelMapping(aInputRdd, aColumnIds, mappings);
        return new MappedRDDContainer(rddWithConvertedValues, mappings);
    }

    /**
     * apply the given mapping to the given input RDD
     * @param aInputRdd
     * @param aColumnIds
     * @param aMappings
     * @return JavaRDD<Row> with converted data (columns are appended)
     */
    public static JavaRDD<Row> applyLabelMapping(final JavaRDD<Row> aInputRdd, final int[] aColumnIds,
        final NominalValueMapping aMappings) {
        JavaRDD<Row> rddWithConvertedValues = aInputRdd.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row row) {
                RowBuilder builder = RowBuilder.fromRow(row);
                for (int ix : aColumnIds) {
                    Integer labelOrIndex = aMappings.getNumberForValue(ix, row.getString(ix));
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
                return builder.build();
            }
        });
        return rddWithConvertedValues;
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
            Set<String> allValues = new HashSet<String>();
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
                        //no need to add modified set as the modification is done implicitly
                        aAggregatedValues.get(ix).add(row.getString(ix));
                    }
                    return aAggregatedValues;
                }
            }, new Function2<Map<Integer, Set<String>>, Map<Integer, Set<String>>, Map<Integer, Set<String>>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<Integer, Set<String>> call(final Map<Integer, Set<String>> aAggregatedValues0,
                    final Map<Integer, Set<String>> aAggregatedValues1) throws Exception {
                    for (Map.Entry<Integer, Set<String>> entry : aAggregatedValues0.entrySet()) {
                        aAggregatedValues0.get(entry.getKey()).addAll(aAggregatedValues1.get(entry.getKey()));
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
        toLabeledVectorRdd(final JavaRDD<Row> inputRdd, final List<Integer> aColumnIndices, final int labelColumnIndex,
            @Nullable final NominalValueMapping labelMapping) {
        return inputRdd.map(new Function<Row, LabeledPoint>() {
            private static final long serialVersionUID = 1L;

            @Override
            public LabeledPoint call(final Row row) {
                double[] convertedValues = new double[row.length() - 1];
                int insertionIndex = 0;
                for (int idx : aColumnIndices) {
                    if (idx != labelColumnIndex) {
                        convertedValues[insertionIndex] = RDDUtils.getDouble(row, idx);
                        insertionIndex += 1;
                    }
                }
                final double label;
                if (labelMapping != null) {
                    label =
                        labelMapping.getNumberForValue(labelColumnIndex, row.getString(labelColumnIndex)).doubleValue();
                } else {
                    //no mapping given - label must already be numeric
                    label = row.getDouble(labelColumnIndex);
                }
                return new LabeledPoint(label, Vectors.dense(convertedValues));
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
     * extracts the given keys from the given rdd and constructs a pair rdd from it
     *
     * @param aRdd Row JavaRDD to be converted
     * @param aKeys keys to be extracted
     * @return pair rdd with keys and original rows as values (no columns are filtered out)
     */
    public static JavaPairRDD<String, Row> extractKeys(final JavaRDD<Row> aRdd, final Integer[] aKeys) {
        return aRdd.mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(final Row aRow) throws Exception {
                final StringBuilder keyValues = new StringBuilder();
                //TODO - do we really need to merge the keys into a single String?
                // (Object[] did not work)
                int ix = 0;
                boolean first = true;
                for (int keyIx : aKeys) {
                    if (!first) {
                        keyValues.append("|");
                        first = false;
                    }
                    keyValues.append(aRow.get(keyIx));
                }
                return new Tuple2<String, Row>(keyValues.toString(), aRow);
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

}
