package com.knime.bigdata.spark.jobserver.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

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
    public static MappedRDDContainer convertNominalValuesForSelectedIndices(
        final JavaRDD<Row> aInputRdd, final int[] aColumnIds, final MappingType aMappingType) {
        final NominalValueMapping mappings = toLabelMapping(aInputRdd, aColumnIds, aMappingType);

        JavaRDD<Row> rddWithConvertedValues = aInputRdd.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row row) {
                RowBuilder builder = RowBuilder.fromRow(row);
                for (int ix : aColumnIds) {
                    Integer labelOrIndex = mappings.getNumberForValue(ix, row.getString(ix));
                    if (aMappingType == MappingType.BINARY) {
                        int numValues = mappings.getNumberOfValues(ix);
                        for (int i=0; i<numValues; i++) {
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
        return new MappedRDDContainer(rddWithConvertedValues, mappings);
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
                return toLabelMappingGlobalMapping(aInputRdd, aNominalColumnIndices);
            }
            case COLUMN: {
                if (aNominalColumnIndices.length < 2) {
                    return toLabelMappingGlobalMapping(aInputRdd, aNominalColumnIndices);
                }
                return toLabelMappingColumnMapping(aInputRdd, aNominalColumnIndices);
            }
            case BINARY: {
                return toLabelMappingColumnMapping(aInputRdd, aNominalColumnIndices);
            }
            default: {
                throw new UnsupportedOperationException(
                        "ERROR: unknown mapping type !");
            }
        }
    }

    private static NominalValueMapping toLabelMappingGlobalMapping(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices) {

        List<String> labels = aInputRdd.flatMap(new FlatMapFunction<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(final Row row) {
                ArrayList<String> val = new ArrayList<>();
                for (int ix : aNominalColumnIndices) {
                    val.add(row.getString(ix));
                }
                return val;
            }
        }).distinct().collect();

        Map<String, Integer> labelMapping = new java.util.HashMap<String, Integer>(labels.size());
        int idx = 0;
        Iterator<String> iter = labels.iterator();
        while (iter.hasNext()) {
            labelMapping.put(iter.next(), idx);
            idx += 1;
        }

        return new GlobalMapping(labelMapping);
    }

    private static NominalValueMapping toLabelMappingColumnMapping(final JavaRDD<Row> aInputRdd,
        final int[] aNominalColumnIndices) {

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

        return new ColumnMapping(labelMapping);
    }

//    private static NominalValueMapping toLabelMappingBinaryMapping(final JavaRDD<Row> aInputRdd,
//        final int[] aNominalColumnIndices) {
//
//        Map<Integer, Set<String>> labels = aggregateValues(aInputRdd, aNominalColumnIndices);
//
//        //this is the only difference to the above method 'toLabelMappingColumnMapping'
//        // the idx is the index of the new column
//        int idx = 0;
//        Map<Integer, Map<String, Integer>> labelMapping = new HashMap<>(labels.size());
//        for (Map.Entry<Integer, Set<String>> entry : labels.entrySet()) {
//            Set<String> values = entry.getValue();
//            Map<String, Integer> mapping = new HashMap<>(values.size());
//            for (String val : values) {
//                mapping.put(val, idx++);
//            }
//            labelMapping.put(entry.getKey(), mapping);
//        }
//
//        return new BinaryMapping(labelMapping);
//    }

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
     * @param aSchema - table schema type
     * @param aLabelColumnIndex index of label column (can be numeric or string)
     * @return container with mapped data and mapping
     * @throws IllegalArgumentException if values are encountered that are neither numeric nor string
     */
    public static LabeledDataInfo toJavaLabeledPointRDDConvertNominalValues(final JavaRDD<Row> aInputRdd,
        final StructType aSchema, final int aLabelColumnIndex) {
        final NominalValueMapping labelMapping =
            toLabelMapping(aInputRdd, new int[]{aLabelColumnIndex}, MappingType.COLUMN);
        final JavaRDD<LabeledPoint> labeledRdd = toLabeledVector(aInputRdd, aLabelColumnIndex, labelMapping);

        return new LabeledDataInfo(labeledRdd, labelMapping);
    }

    private static JavaRDD<LabeledPoint> toLabeledVector(final JavaRDD<Row> inputRdd, final int labelColumnIndex,
        final NominalValueMapping labelMapping) {
        return inputRdd.map(new Function<Row, LabeledPoint>() {
            private static final long serialVersionUID = 1L;

            @Override
            public LabeledPoint call(final Row row) {
                double[] convertedValues = new double[row.length() - 1];
                int insertionIndex = 0;
                for (int idx = 0; idx < row.length(); idx++) {
                    if (idx != labelColumnIndex) {
                        convertedValues[insertionIndex] = RDDUtils.getDouble(row, idx);
                        insertionIndex += 1;
                    }
                }
                Integer label = labelMapping.getNumberForValue(labelColumnIndex, row.getString(labelColumnIndex));
                return new LabeledPoint(label.doubleValue(), Vectors.dense(convertedValues));
            }
        });
    }

}
