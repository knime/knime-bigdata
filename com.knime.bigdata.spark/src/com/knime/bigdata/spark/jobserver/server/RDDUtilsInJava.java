package com.knime.bigdata.spark.jobserver.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;
import org.knime.core.util.Pair;

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
     * @return container JavaRDD<Row> with original data plus appended columns and Map<String, Integer> with mapping
     */
    public static Pair<JavaRDD<Row>, Map<String, Integer>> convertNominalValuesForSelectedIndices(
        final JavaRDD<Row> aInputRdd, final int[] aColumnIds) {
        final Map<String, Integer> mappings = toLabelMapping(aInputRdd, aColumnIds);

        JavaRDD<Row> rddWithConvertedValues = aInputRdd.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(final Row row) {
                RowBuilder builder = RowBuilder.fromRow(row);
                for (int ix = 0; ix < aColumnIds.length; ix++) {
                    Integer label = mappings.get(row.getString(aColumnIds[ix]));
                    builder.add(label.doubleValue());
                }
                return builder.build();
            }
        });

        return new Pair<JavaRDD<Row>, Map<String, Integer>>(rddWithConvertedValues, mappings);
    }

    /**
     * extract all distinct values from the given input RDD and the given indices and create a mapping from value to int
     *
     * @param inputRdd
     * @param nominalColumnIndices
     * @return mapping from distinct value to unique integer value
     */
    public static Map<String, Integer> toLabelMapping(final JavaRDD<Row> inputRdd, final int[] nominalColumnIndices) {
        List<String> labels = inputRdd.flatMap(new FlatMapFunction<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(final Row row) {
                ArrayList<String> val = new ArrayList<>();
                for (int ix : nominalColumnIndices) {
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

        return labelMapping;
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
        final Map<String, Integer> labelMapping = toLabelMapping(aInputRdd, new int[]{aLabelColumnIndex});
        final JavaRDD<LabeledPoint> labeledRdd = toLabeledVector(aInputRdd, aLabelColumnIndex, labelMapping);

        return new LabeledDataInfo(labeledRdd, labelMapping);
    }

    private static JavaRDD<LabeledPoint> toLabeledVector(final JavaRDD<Row> inputRdd, final int labelColumnIndex,
        final Map<String, Integer> labelMapping ) {
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
                Integer label = labelMapping.get(row.getString(labelColumnIndex));
                return new LabeledPoint(label.doubleValue(), Vectors.dense(convertedValues));
            }
        });
    }

}
