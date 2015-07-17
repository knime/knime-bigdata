package com.knime.bigdata.spark.testing.jobserver.server;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.knime.bigdata.spark.jobserver.server.LabeledDataInfo;
import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.NominalValueMapping;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;

/**
 *
 * @author dwk
 *
 */
public class RDDUtilsInJavaTest {
    private static class SparkContextResource extends ExternalResource {
        private static final SparkConf conf = new SparkConf().setAppName(RDDUtilsInJavaTest.class.getSimpleName())
            .setMaster("local");

        public JavaSparkContext sparkContext;

        @Override
        protected void before() {
            sparkContext = new JavaSparkContext(conf);
        }

        @Override
        protected void after() {
            sparkContext.close();
        }
    }

    @Rule
    public SparkContextResource sparkContextResource = new SparkContextResource();

    private static class MyMapper implements Serializable {
        private static final long serialVersionUID = 1L;

        JavaRDD<Vector> apply(final JavaDoubleRDD o) {
            return o.map(new Function<Double, Vector>() {
                private static final long serialVersionUID = 1L;

                private int ix = 0;

                @Override
                public Vector call(final Double x) {
                    ix = ix + 1;
                    return Vectors.dense(x, 2.0 * x, ix, x + 45);
                }
            });
        }

        JavaRDD<Row> toRowRdd(final JavaDoubleRDD o) {
            return o.map(new Function<Double, Row>() {
                private static final long serialVersionUID = 1L;

                private int ix = 0;

                @Override
                public Row call(final Double x) {
                    ix = ix + 1;
                    return Row.create(x, 2.0 * x, ix, x + 14);
                }
            });
        }

        final static String[] colors = {"red", "blue", "green", "black", "white"};

        final static String[] teams = {"FC1", "FC 2", "FC 1987", "Mein Klub"};

        JavaRDD<Row> toRowRddWithNominalLabels(final JavaDoubleRDD o) {
            return o.map(new Function<Double, Row>() {
                private static final long serialVersionUID = 1L;

                private int ix = 0;

                @Override
                public Row call(final Double x) {
                    ix = ix + 1;
                    final String color = colors[(int)(colors.length * Math.random())];
                    return Row.create(x, 2.0 * x, ix, color);
                }
            });
        }

        JavaRDD<Row> toRowRddWithNominalValues(final JavaDoubleRDD o) {
            return o.map(new Function<Double, Row>() {
                private static final long serialVersionUID = 1L;

                private int ix = 0;

                @Override
                public Row call(final Double x) {
                    final String color = colors[ix % colors.length];
                    final String team = teams[ix % teams.length];
                    ix = ix + 1;
                    return Row.create(team, x, team + color, team, color.substring(0, 1), color);
                }
            });
        }

        JavaRDD<String> extractLabelColumn(final JavaRDD<Row> aJavaRDDWithNominalLabels) {
            return aJavaRDDWithNominalLabels.map(new Function<Row, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String call(final Row x) {
                    return x.getString(3);
                }
            });
        }

    }

    @Test
    public void whatDoesRowCreateDo() {
        Row row = Row.create(new Object[]{"a", 1, 2.8d});
        assertEquals("should have three elements", 3, row.length());
    }

    @Test
    public void conversionOfJavaPairedRDD2JavaRDDWithRows() throws Exception {
        // JavaRDD input1 = sparkContext.makeRDD();
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
        JavaRDD<Vector> v = new MyMapper().apply(o);
        JavaRDD<Row> rowRDD = RDDUtils.toJavaRDDOfRows(v.zip(o));

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
        assertEquals("conversion should create correct length of rows ", 5, rowRDD.collect().get(0).length());
    }

    @Test
    public void conversionOfJavaRowRDD2JavaRDDWithVector() throws Exception {
        // JavaRDD input1 = sparkContext.makeRDD();
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
        JavaRDD<Row> v = new MyMapper().toRowRdd(o);
        JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectors(v);

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
        assertEquals("conversion should create correct length of vectors ", 4, rowRDD.collect().get(0).size());
    }

    @Test
    public void addColumn2JavaRowRDD() throws Exception {
        // JavaRDD input1 = sparkContext.makeRDD();
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
        JavaRDD<Row> v = new MyMapper().toRowRdd(o);

        JavaRDD<Row> rowRDD = RDDUtils.addColumn(v.zip(o));

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
        assertEquals("conversion should add single column ", 5, rowRDD.collect().get(0).length());
    }

    @Test
    public void conversionOfJavaRowRDD2JavaRDDWithVectorKeepOnlySomeFeatures() throws Exception {
        // JavaRDD input1 = sparkContext.makeRDD();
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 2);
        JavaRDD<Row> v = new MyMapper().toRowRdd(o);
        List<Integer> ix = new ArrayList<Integer>();
        ix.add(0);
        ix.add(1);
        ix.add(3);
        JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(v, ix);

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
        assertEquals("conversion should create correct length of vectors ", 3, rowRDD.collect().get(0).size());

    }

    @Test
    public void conversionOfJavaRowRDD2JavaRDDWithLabeledPoint() throws Exception {
        // JavaRDD input1 = sparkContext.makeRDD();
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRdd(o);
        JavaRDD<LabeledPoint> rowRDD = RDDUtils.toJavaLabeledPointRDD(v, 2);

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);

        List<LabeledPoint> rows = rowRDD.collect();
        for (int i = 0; i < rows.size(); i++) {
            double[] features = rows.get(i).features().toArray();
            assertEquals("conversion should create correct length of vectors ", 3, features.length);
            for (int j = 0; j < features.length; j++) {
                assertTrue("label should not be contained in features", Math.abs(features[j] - i) > 0.0001);
            }
            assertEquals("conversion should set proper label ", 1 + i, (int)rows.get(i).label());
        }
    }

    @Test
    public void conversionOfJavaRowRDDWithNominalValues2JavaRDDWithLabeledPoint() throws Exception {
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 100L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalLabels(o).cache();
        StructType schema = StructTypeBuilder.fromRows(v.take(2)).build();
        LabeledDataInfo info = RDDUtilsInJava.toJavaLabeledPointRDDConvertNominalValues(v, schema, 3);

        assertEquals("Incorrect number of classes", 5, info.getNumberClasses());

        JavaRDD<LabeledPoint> rowRDD = info.getLabeledPointRDD();
        assertEquals("Conversion changed the number of rows ", rowRDD.count(), 100);

        List<LabeledPoint> rows = rowRDD.collect();
        List<String> labels = new MyMapper().extractLabelColumn(v).collect();
        for (int i = 0; i < rows.size(); i++) {
            double[] features = rows.get(i).features().toArray();
            assertEquals("conversion should create correct length of vectors ", 3, features.length);

            for (int j = 0; j < features.length; j++) {
                assertTrue("label should not be contained in features", Math.abs(features[j] - i) > 0.0001);
            }
            assertEquals("conversion should set proper label ",
                info.getClassLabelToIntMapping().getNumberForValue(3, labels.get(i)).intValue(), (int)rows.get(i)
                    .label());
        }
    }

    /**
     * convert all nominal values in selected columns to corresponding columns with numbers (not binary), use one
     * mapping for all columns
     *
     * @throws Exception
     */
    @Test
    public void conversionOfNominalValuesInRDDOfRowsOneMap() throws Exception {
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 10L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();

        //convert all but the last column with nominal values:
        MappedRDDContainer info =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(v, new int[]{0, 2, 3, 4}, MappingType.GLOBAL);

        JavaRDD<Row> rddWithConvertedValues = info.m_RddWithConvertedValues;
        NominalValueMapping mappings = info.m_Mappings;

        assertEquals("Incorrect number of mapped values", 18, mappings.size());

        assertEquals("Conversion changed the number of rows ", rddWithConvertedValues.count(), 10);

        List<Row> rows = rddWithConvertedValues.collect();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            //6 original columns + 4 converted columns
            assertEquals("conversion should create correct length of rows ", 6 + 4, row.length());

            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(0, row.getString(0)) == (int)row.getDouble(6));
            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(2, row.getString(2)) == (int)row.getDouble(7));
            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(3, row.getString(3)) == (int)row.getDouble(8));
            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(4, row.getString(4)) == (int)row.getDouble(9));
        }
    }

    /**
     * convert all nominal values in selected columns to corresponding columns with numbers (not binary), use separate
     * mappings for each column
     *
     * @throws Exception
     */
    @Test
    public void conversionOfNominalValuesInRDDOfRowsSeparateMaps() throws Exception {
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 25L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();

        //convert all but the last column with nominal values:
        MappedRDDContainer info =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(v, new int[]{0, 2, 3, 4}, MappingType.COLUMN);

        JavaRDD<Row> rddWithConvertedValues = info.m_RddWithConvertedValues;
        NominalValueMapping mappings = info.m_Mappings;

        assertEquals("Incorrect number of mapped values", 32, mappings.size());

        assertEquals("Conversion changed the number of rows ", rddWithConvertedValues.count(), 25);

        List<Row> rows = rddWithConvertedValues.collect();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            //6 original columns + 4 converted columns
            assertEquals("conversion should create correct length of rows ", 6 + 4, row.length());

            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(0, row.getString(0)) == (int)row.getDouble(6));
            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(2, row.getString(2)) == (int)row.getDouble(7));
            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(3, row.getString(3)) == (int)row.getDouble(8));
            assertTrue("converted values should be numbers and at the end",
                mappings.getNumberForValue(4, row.getString(4)) == (int)row.getDouble(9));
        }
    }

    /**
     * convert all nominal values in selected columns to corresponding binary columns
     *
     * @throws Exception
     */
    @Test
    public void conversionOfNominalValuesInRDDOfRowsBinary() throws Exception {
        JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext, 25L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();

        //convert all but the last column with nominal values:
        MappedRDDContainer info =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(v, new int[]{0, 2, 3, 4}, MappingType.BINARY);

        JavaRDD<Row> rddWithConvertedValues = info.m_RddWithConvertedValues;
        NominalValueMapping mappings = info.m_Mappings;

        assertEquals("Conversion changed the number of rows ", rddWithConvertedValues.count(), 25);

        List<Row> rows = rddWithConvertedValues.collect();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            int offset = 6;
            //6 original columns + 32 converted values
            assertEquals("conversion should create correct length of rows ", offset + 32, row.length());
            {
                int colIx = mappings.getNumberForValue(0, row.getString(0));
                assertEquals("incorrect number of values for 'teams'", MyMapper.teams.length,
                    mappings.getNumberOfValues(0));
                assertTrue("row "+i+": converted values should be in proper column", colIx >= 0
                    && colIx < MyMapper.teams.length);
                assertEquals("converted values should be 1", 1, (int)row.getDouble(colIx + offset));
                offset += MyMapper.teams.length;
            }

            {
                int colIx = mappings.getNumberForValue(2, row.getString(2));
                assertTrue("converted values should be in proper column", colIx >= 0
                    && colIx < mappings.getNumberOfValues(2));
                assertEquals("converted values should be 1", 1, (int)row.getDouble(offset+colIx));
                offset += mappings.getNumberOfValues(2);
            }

            //teams
            {
                int colIx =  mappings.getNumberForValue(3, row.getString(3));
                assertEquals("incorrect number of values for 'teams'", MyMapper.teams.length,
                    mappings.getNumberOfValues(3));
                assertTrue("converted values should be in proper column", colIx >= 0
                    && colIx < MyMapper.teams.length);
                assertEquals("converted values should be 1", 1, (int)row.getDouble(colIx + offset));
                offset += mappings.getNumberOfValues(3);
            }

            //4 values
            {
                int colIx =  mappings.getNumberForValue(4, row.getString(4));
                assertEquals("incorrect number of values for first letter of 'colors'", 4,
                    mappings.getNumberOfValues(4));
                assertTrue("converted values should be in proper column", colIx >= 0
                    && colIx < 4);
                assertEquals("converted values should be 1", 1, (int)row.getDouble(colIx + offset));
                offset += mappings.getNumberOfValues(4);
            }

            //5 colors, but not mapped!
//            {
//                int colIx =  mappings.getNumberForValue(0, row.getString(4));
//                assertEquals("incorrect number of values for 'colors'", MyMapper.colors.length,
//                    mappings.getNumberOfValues(4));
//                assertTrue("converted values should be in proper column", colIx >= 0
//                    && colIx < 4);
//                assertEquals("converted values should be 1", 1, (int)row.getDouble(colIx + offset));
//                offset += mappings.getNumberOfValues(4);
//            }
            assertEquals("incorrect number of new columns added ", 6+ 32, offset);

        }
    }

    @Nonnull
    private static List<Row> repeatRowValues(final int numberOfRows, final Object... rowValues) {
        assert numberOfRows > 0;
        assert rowValues != null;

        final List<Row> rows = new ArrayList<>(numberOfRows);
        for (int i = 0; i < numberOfRows; ++i) {
            rows.add(Row.create(rowValues));
        }

        return rows;
    }

}