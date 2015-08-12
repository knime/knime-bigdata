package com.knime.bigdata.spark.jobserver.server;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.sql.api.java.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.knime.bigdata.spark.jobserver.server.LabeledDataInfo;
import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.NominalValueMapping;
import com.knime.bigdata.spark.jobserver.server.NormalizationSettings;
import com.knime.bigdata.spark.jobserver.server.NormalizationSettingsFactory;
import com.knime.bigdata.spark.jobserver.server.NormalizedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
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
    public final SparkContextResource sparkContextResource = new SparkContextResource();

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
            return toRowRddWithNominalValues(o, colors);
        }

        JavaRDD<Row> toRowRddWithNominalValues(final JavaDoubleRDD o, final String[] aColors) {
            return o.map(new Function<Double, Row>() {
                private static final long serialVersionUID = 1L;

                private int ix = 0;

                @Override
                public Row call(final Double x) {
                    final String color = aColors[ix % aColors.length];
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
    public void toLabeledPointRDDShouldCreateVectorsForIndicatedColumnsOnly() throws Exception {

        JavaDoubleRDD o = getRandomDoubleRDD(100L, 2);
        JavaRDD<Vector> v = new MyMapper().apply(o);
        JavaRDD<Row> rowRDD = RDDUtils.toJavaRDDOfRows(v.zip(o));

        List<Integer> colIdxs = new ArrayList<>();
        colIdxs.add(1);
        colIdxs.add(3);
        final List<LabeledPoint> inputRdd = RDDUtilsInJava.toJavaLabeledPointRDD(rowRDD, colIdxs, 2).collect();

        List<Row> rows = rowRDD.collect();
        assertEquals("number of rows must not change", rows.size(), inputRdd.size());

        int ix = 0;
        for (LabeledPoint p : inputRdd) {
            assertEquals("length of feature vector must be equivalent to length of colum selector", colIdxs.size(), p
                .features().size());
            assertEquals("selected features incorrect (col 1 of row[" + ix + "])", rows.get(ix).get(1), p.features()
                .apply(0));
            assertEquals("selected features incorrect (col 3 of row[" + ix + "])", rows.get(ix).get(3), p.features()
                .apply(1));
            ix++;
        }
    }

    @Test
    public void conversionOfJavaPairedRDD2JavaRDDWithRows() throws Exception {
        JavaDoubleRDD o = getRandomDoubleRDD(100L, 2);
        JavaRDD<Vector> v = new MyMapper().apply(o);
        JavaRDD<Row> rowRDD = RDDUtils.toJavaRDDOfRows(v.zip(o));

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
        assertEquals("conversion should create correct length of rows ", 5, rowRDD.collect().get(0).length());
    }

    private final Map<String, JavaDoubleRDD> m_randomRDDs = new HashMap<>();

    /**
     * @return
     */
    private JavaDoubleRDD getRandomDoubleRDD(final long aNumRows, final int aNumCols) {
        JavaDoubleRDD cached = m_randomRDDs.get(aNumRows + "-" + aNumCols);
        if (cached == null) {
            cached = normalJavaRDD(sparkContextResource.sparkContext, aNumRows, aNumCols);
            m_randomRDDs.put(aNumRows + "-" + aNumCols, cached);
        }
        return cached;
    }

    @Test
    public void conversionOfJavaRowRDD2JavaRDDWithVector() throws Exception {
        JavaDoubleRDD o = getRandomDoubleRDD(100L, 2);
        JavaRDD<Row> v = new MyMapper().toRowRdd(o);
        JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectors(v);

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
        assertEquals("conversion should create correct length of vectors ", 4, rowRDD.collect().get(0).size());
    }

    @Test
    public void addColumn2JavaRowRDD() throws Exception {
        JavaDoubleRDD o = getRandomDoubleRDD(100L, 2);
        JavaRDD<Row> v = new MyMapper().toRowRdd(o);

        JavaRDD<Row> rowRDD = RDDUtils.addColumn(v.zip(o));

        assertEquals("conversion should keep number rows constant ", rowRDD.count(), 100);
        assertEquals("conversion should add single column ", 5, rowRDD.collect().get(0).length());
    }

    @Test
    public void conversionOfJavaRowRDD2JavaRDDWithVectorKeepOnlySomeFeatures() throws Exception {
        JavaDoubleRDD o = getRandomDoubleRDD(100L, 2);
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
        JavaDoubleRDD o = getRandomDoubleRDD(100L, 1);
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
        JavaDoubleRDD o = getRandomDoubleRDD(100L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalLabels(o).cache();
        List<Integer> selector = new ArrayList<>();
        selector.add(0);
        selector.add(1);
        selector.add(2);
        LabeledDataInfo info = RDDUtilsInJava.toJavaLabeledPointRDDConvertNominalValues(v, selector, 3);

        assertEquals("Incorrect number of classes", 5, info.getClassLabelToIntMapping().size());

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
        JavaDoubleRDD o = getRandomDoubleRDD(10L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();

        //convert all but the last column with nominal values:
        MappedRDDContainer info =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(v, new int[]{0, 2, 3, 4}, MappingType.GLOBAL);

        JavaRDD<Row> rddWithConvertedValues = info.m_RddWithConvertedValues;
        NominalValueMapping mappings = info.m_Mappings;

        assertEquals("Incorrect number of mapped values", 22, mappings.size());

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

    @Test
    public void applyLabelMappingShouldIgnoreUnknownColumns() throws Exception {
        JavaDoubleRDD o = getRandomDoubleRDD(10L, 1);
        // v has 6 columns: team, x, team + color, team, color.substring(0, 1), color
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();

        MappedRDDContainer info =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(v, new int[]{0, 2}, MappingType.GLOBAL);

        // 3 and 5 were not mapped, should be ignored
        List<Row> rows = RDDUtilsInJava.applyLabelMapping(v, new int[]{0, 2, 3, 5}, info.m_Mappings).collect();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            //6 original columns + 2! converted columns
            assertEquals("conversion should create correct length of rows ", 6 + 2, row.length());

            assertTrue("converted values should be numbers and at the end",
                info.m_Mappings.getNumberForValue(0, row.getString(0)) == (int)row.getDouble(6));
            assertTrue("converted values should be numbers and at the end",
                info.m_Mappings.getNumberForValue(2, row.getString(2)) == (int)row.getDouble(7));
        }
    }

    @Test(expected = SparkException.class)
    public void applyLabelMappingShouldReportUnknownValuesInKnownColumns() throws Throwable {
        JavaDoubleRDD o = getRandomDoubleRDD(10L, 1);
        // v has 6 columns: team, x, team + color, team, color.substring(0, 1), color
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();

        MappedRDDContainer info =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(v, new int[]{2}, MappingType.GLOBAL);
        JavaRDD<Row> v2 = new MyMapper().toRowRddWithNominalValues(o, new String[]{"red", "blue", "v1", "v2"}).cache();

        try {
            RDDUtilsInJava.applyLabelMapping(v2, new int[]{2}, info.m_Mappings).collect();
        } catch (Exception nse) {
            nse.printStackTrace();
            throw nse;
        }
        // not strictly required, but easier for debugging:
        fail("Expected exception not thrown");
    }

    /**
     * convert all nominal values in selected columns to corresponding columns with numbers (not binary), use separate
     * mappings for each column
     *
     * @throws Exception
     */
    @Test
    public void conversionOfNominalValuesInRDDOfRowsSeparateMaps() throws Exception {
        JavaDoubleRDD o = getRandomDoubleRDD(25L, 1);
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
        JavaDoubleRDD o = getRandomDoubleRDD(25L, 1);
        JavaRDD<Row> v = new MyMapper().toRowRddWithNominalValues(o).cache();

        //convert all but the last column with nominal values:
        MappedRDDContainer info =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(v, new int[]{0, 3, 2, 4}, MappingType.BINARY);

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
                assertTrue("row " + i + ": converted values should be in proper column", colIx >= 0
                    && colIx < MyMapper.teams.length);
                assertEquals("converted values should be 1", 1, (int)row.getDouble(colIx + offset));
                offset += MyMapper.teams.length;
            }

            //teams
            {
                int colIx = mappings.getNumberForValue(3, row.getString(3));
                assertEquals("incorrect number of values for 'teams'", MyMapper.teams.length,
                    mappings.getNumberOfValues(3));
                assertTrue("converted values should be in proper column", colIx >= 0 && colIx < MyMapper.teams.length);
                assertEquals("converted values should be 1", 1, (int)row.getDouble(colIx + offset));
                offset += mappings.getNumberOfValues(3);
            }

            {
                int colIx = mappings.getNumberForValue(2, row.getString(2));
                assertTrue("converted values should be in proper column",
                    colIx >= 0 && colIx < mappings.getNumberOfValues(2));
                assertEquals("converted values should be 1", 1, (int)row.getDouble(offset + colIx));
                offset += mappings.getNumberOfValues(2);
            }

            //4 values
            {
                int colIx = mappings.getNumberForValue(4, row.getString(4));
                assertEquals("incorrect number of values for first letter of 'colors'", 4,
                    mappings.getNumberOfValues(4));
                assertTrue("converted values should be in proper column", colIx >= 0 && colIx < 4);
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
            assertEquals("incorrect number of new columns added ", 6 + 32, offset);

        }
    }

    static class CreateData implements Serializable {
        private static final long serialVersionUID = 1L;

        final JavaRDD<Row> rowData;

        CreateData(final JavaSparkContext aContext) {
            // Load and parse the data
            JavaRDD<Double[]> data = aContext.parallelize(getData());
            rowData = data.map(new Function<Double[], Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Row call(final Double[] line) {
                    RowBuilder builder = RowBuilder.emptyRow();
                    for (int i = 0; i < line.length; i++) {
                        builder.add(line[i]);
                    }
                    return builder.build();
                }
            });
        }

        NormalizedRDDContainer normalize(final Collection<Integer> indices, final NormalizationSettings aSettings) {
            return RDDUtilsInJava.normalize(rowData, indices, aSettings);
        }
    }

    @Test
    public void findColumnStatsShouldComputeMinMaxValues() throws Throwable {

        CreateData data = new CreateData(sparkContextResource.sparkContext);
        List<Integer> colIds = new ArrayList<>();
        colIds.add(0);
        colIds.add(2);
        colIds.add(1);
        colIds.add(3);

        MultivariateStatisticalSummary stats = RDDUtilsInJava.findColumnStats(data.rowData, colIds);

        assertEquals("sepal length", -666.3d, stats.min().apply(0), 0.0001);
        assertEquals("sepal width", -3.7d, stats.min().apply(1), 0.0001);
        assertEquals("petal length", 1d, stats.min().apply(2), 0.0001);
        assertEquals("petal width", 0.0d, stats.min().apply(3), 0.0001);
        assertEquals("sepal length", 176.3d, stats.max().apply(0), 0.0001);
        assertEquals("sepal width", 4.4d, stats.max().apply(1), 0.0001);
        assertEquals("petal length", 6.9d, stats.max().apply(2), 0.0001);
        assertEquals("petal width", 0.9d, stats.max().apply(3), 0.0001);

    }

    @Test
    public void minMaxComputationShouldNormalizeDataTo01Range() throws Throwable {

        CreateData data = new CreateData(sparkContextResource.sparkContext);
        List<Integer> colIds = new ArrayList<>();
        colIds.add(3);
        colIds.add(1);
        colIds.add(0);
        JavaRDD<Row> normalizedData =
            data.normalize(colIds,
                NormalizationSettingsFactory.createNormalizationSettingsForMinMaxScaling(0, 1)).getRdd();

        colIds = new ArrayList<>();
        colIds.add(0);
        colIds.add(1);
        colIds.add(2);
        colIds.add(3);

        MultivariateStatisticalSummary stats = RDDUtilsInJava.findColumnStats(normalizedData, colIds);

        assertEquals("sepal length", 0d, stats.min().apply(0), 0.0001);
        assertEquals("sepal width", 0d, stats.min().apply(1), 0.0001);
        assertEquals("petal length unnormalized", 1d, stats.min().apply(2), 0.0001);
        assertEquals("petal width", 0.0d, stats.min().apply(3), 0.0001);
        assertEquals("sepal length", 1d, stats.max().apply(0), 0.0001);
        assertEquals("sepal width", 1d, stats.max().apply(1), 0.0001);
        assertEquals("petal length unnormalized", 6.9d, stats.max().apply(2), 0.0001);
        assertEquals("petal width", 1d, stats.max().apply(3), 0.0001);

    }

    @Test
    public void minMaxComputationShouldNormalizeDataToXYRange() throws Throwable {
        CreateData data = new CreateData(sparkContextResource.sparkContext);

        List<Integer> colIds = new ArrayList<>();
        colIds.add(3);
        colIds.add(1);
        colIds.add(0);
        NormalizedRDDContainer normalizedData =
                data.normalize(colIds,
                NormalizationSettingsFactory.createNormalizationSettingsForMinMaxScaling(-1, 3));

        MultivariateStatisticalSummary stats = RDDUtilsInJava.findColumnStats(data.rowData, colIds);

        assertEquals("sepal length", -1d, normalizedData.normalize(0, stats.min().apply(0)), 0.0001);
        assertEquals("sepal width", -1d, normalizedData.normalize(1, stats.min().apply(1)), 0.0001);
        assertEquals("petal width", -1d, normalizedData.normalize(2, stats.min().apply(2)), 0.0001);
        assertEquals("sepal length", 3d, normalizedData.normalize(0, stats.max().apply(0)), 0.0001);
        assertEquals("sepal width", 3d, normalizedData.normalize(1, stats.max().apply(1)), 0.0001);
        assertEquals("petal width", 3d, normalizedData.normalize(2, stats.max().apply(2)), 0.0001);

        // new range is 4, middle value should be normalized to 1
        assertEquals("sepal width normalized mean value", 1,
            normalizedData.normalize(1, stats.min().apply(1) + (stats.max().apply(1) - stats.min().apply(1)) * 0.5),
            0.0001);

        //check values inbetween
        final double scale = 4 / (stats.max().apply(0) - stats.min().apply(0));
        for (double d = stats.min().apply(0); d < stats.max().apply(0); d = d + 0.1d) {
            assertEquals("sepal length normalized value", -1d + scale * (d - stats.min().apply(0)),
                normalizedData.normalize(0, d), 0.0001);
        }

        colIds = new ArrayList<>();
        colIds.add(0);
        colIds.add(1);
        colIds.add(2);
        colIds.add(3);
        NormalizedRDDContainer minMax =
            RDDUtilsInJava.normalize(normalizedData.getRdd(), colIds,
                NormalizationSettingsFactory.createNormalizationSettingsForMinMaxScaling(0, 1));

        stats = RDDUtilsInJava.findColumnStats(normalizedData.getRdd(), colIds);

        //the normalized columns should have min / max value -1/3, the unnormalized column the original min/max
        assertEquals("petal length unnormalized", -1d, stats.min().apply(0), 0.0001);
        assertEquals("petal length unnormalized", 3d, stats.max().apply(0), 0.0001);
        assertEquals("petal length unnormalized", -1d, stats.min().apply(1), 0.0001);
        assertEquals("petal length unnormalized", 3d, stats.max().apply(1), 0.0001);
        assertEquals("petal length unnormalized", 1d, stats.min().apply(2), 0.0001);
        assertEquals("petal length unnormalized", 6.9d, stats.max().apply(2), 0.0001);
        assertEquals("petal length unnormalized", -1d, stats.min().apply(3), 0.0001);
        assertEquals("petal length unnormalized", 3d, stats.max().apply(3), 0.0001);

        assertEquals("sepal length", 0d, minMax.normalize(0, stats.min().apply(0)), 0.0001);
        assertEquals("sepal width", 0d, minMax.normalize(1, stats.min().apply(1)), 0.0001);
        assertEquals("petal length unnormalized", 0d, minMax.normalize(2, stats.min().apply(2)), 0.0001);
        assertEquals("petal width", 0.0d, minMax.normalize(3, stats.min().apply(3)), 0.0001);
        assertEquals("sepal length", 1d, minMax.normalize(0, stats.max().apply(0)), 0.0001);
        assertEquals("sepal width", 1d, minMax.normalize(1, stats.max().apply(1)), 0.0001);
        assertEquals("petal length unnormalized", 1d, minMax.normalize(2, stats.max().apply(2)), 0.0001);
        assertEquals("petal width", 1d, minMax.normalize(3, stats.max().apply(3)), 0.0001);
    }

    @Test
    public void normalizeDataUsingZScore() throws Throwable {

        CreateData data = new CreateData(sparkContextResource.sparkContext);

        List<Integer> colIds = new ArrayList<>();
        colIds.add(3);
        colIds.add(1);
        colIds.add(0);
        NormalizedRDDContainer normalizedData =
            data.normalize(colIds,
                NormalizationSettingsFactory.createNormalizationSettingsForZScoreNormalization());

        MultivariateStatisticalSummary stats0 = RDDUtilsInJava.findColumnStats(data.rowData, colIds);

        //original mean value should be normalized to 0
        assertEquals("sepal length", 0, normalizedData.normalize(0, stats0.mean().apply(0)), 0.0001);
        assertEquals("sepal width", 0, normalizedData.normalize(1, stats0.mean().apply(1)), 0.0001);
        assertEquals("petal width", 0, normalizedData.normalize(2, stats0.mean().apply(2)), 0.0001);
        //        assertEquals("sepal length",  0,normalizedData.normalize(0, normalizedData.getMean(0)), 0.0001);
        //        assertEquals("sepal width",  0,normalizedData.normalize(0, normalizedData.getMean(0)), 0.0001);
        //        assertEquals("petal width",  0,normalizedData.normalize(0, normalizedData.getMean(0)), 0.0001);

        // compute stats for normalized data, mean should be 0, variance 1
        MultivariateStatisticalSummary stats1 = RDDUtilsInJava.findColumnStats(normalizedData.getRdd(), colIds);

        // data is normalized to mean 0 and std dev 1, middle value should be normalized to 0
        assertEquals("sepal length normalized mean value", 0, stats1.mean().apply(0), 0.0000001);
        assertEquals("sepal width normalized mean value", 0, stats1.mean().apply(1), 0.0000001);
        assertEquals("petal width normalized mean value", 0, stats1.mean().apply(2), 0.0000001);

        // square root of 1 is 1
        assertEquals("sepal length normalized standard deviation", 1, stats1.variance().apply(0), 0.000001);
        assertEquals("sepal width normalized standard deviation", 1, stats1.variance().apply(1), 0.000001);
        assertEquals("petal width normalized standard deviation", 1, stats1.variance().apply(2), 0.000001);

        //check values inbetween
        final double mean = stats0.mean().apply(0);
        for (double d = stats0.min().apply(0); d < stats0.max().apply(0); d = d + 0.1d) {
            assertEquals("sepal length normalize value "+d+ " for mean: "+mean+": "+normalizedData.normalize(0, d), d < mean, 0 > normalizedData.normalize(0, d));
        }

    }

    @Test
    public void normalizeDataUsingDecimalScaling() throws Throwable {
        List<Integer> colIds = new ArrayList<>();
        colIds.add(3);
        colIds.add(1);
        colIds.add(0);
        NormalizedRDDContainer normalizedInfo =
            new CreateData(sparkContextResource.sparkContext).normalize(colIds,
                NormalizationSettingsFactory.createNormalizationSettingsForDecimalScaling());

        List<Row> normalizedData = normalizedInfo.getRdd().collect();
        //all values should be divided by: Col 0 - 1000, Col 1 - 10, Col 2 - no normalization, Col 3 - 10
        int ix = 0;
        for (Double[] row : getData()) {
            Row normalizedRow = normalizedData.get(ix++);
            assertEquals("sepal length", row[0] / 1000d, normalizedRow.getDouble(0), 0.0001);
            assertEquals("sepal width", row[1] / 10d, normalizedRow.getDouble(1), 0.0001);
            //not normalized
            assertEquals("petal length", row[2], normalizedRow.getDouble(2), 0.0001);
            //all values smaller 1 already
            assertEquals("petal width", row[3], normalizedRow.getDouble(3), 0.0001);
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

    private static List<Double[]> getData() {
        final List<Double[]> res = new ArrayList<Double[]>(modifiedIrisData.length);
        for (Double[] row : modifiedIrisData) {
            res.add(row);
        }
        return res;
    }

    private final static Double[][] modifiedIrisData = new Double[][]{{5.1, 3.5, 1.4, 0.2}, {4.9, 3.0, 1.4, 0.2},
        {4.7, 3.2, 1.3, 0.2}, {4.6, 3.1, 1.5, 0.2}, {5.0, 3.6, 1.4, 0.2}, {5.4, 3.9, 1.7, 0.4}, {4.6, 3.4, 1.4, 0.3},
        {5.0, 3.4, 1.5, 0.2}, {4.4, 2.9, 1.4, 0.2}, {4.9, 3.1, 1.5, 0.1}, {5.4, 3.7, 1.5, 0.2}, {4.8, 3.4, 1.6, 0.2},
        {4.8, 3.0, 1.4, 0.1}, {4.3, 3.0, 1.1, 0.1}, {5.8, 4.0, 1.2, 0.2}, {5.7, 4.4, 1.5, 0.4}, {5.4, 3.9, 1.3, 0.4},
        {5.1, 3.5, 1.4, 0.3}, {5.7, 3.8, 1.7, 0.3}, {5.1, 3.8, 1.5, 0.3}, {5.4, 3.4, 1.7, 0.2}, {5.1, 3.7, 1.5, 0.4},
        {4.6, 3.6, 1.0, 0.2}, {5.1, -3.3, 1.7, 0.5}, {4.8, 3.4, 1.9, 0.2}, {5.0, 3.0, 1.6, 0.2}, {5.0, 3.4, 1.6, 0.4},
        {5.2, 3.5, 1.5, 0.2}, {5.2, 3.4, 1.4, 0.2}, {4.7, 3.2, 1.6, 0.2}, {4.8, 3.1, 1.6, 0.2}, {5.4, 3.4, 1.5, 0.4},
        {5.2, 4.1, 1.5, 0.1}, {5.5, 4.2, 1.4, 0.2}, {4.9, 3.1, 1.5, 0.2}, {5.0, 3.2, 1.2, 0.2}, {5.5, 3.5, 1.3, 0.2},
        {4.9, 3.6, 1.4, 0.1}, {4.4, 3.0, 1.3, 0.2}, {5.1, 3.4, 1.5, 0.2}, {5.0, 3.5, 1.3, 0.3}, {4.5, 2.3, 1.3, 0.3},
        {4.4, 3.2, 1.3, 0.2}, {-5.0, 3.5, 1.6, 0.6}, {5.1, 3.8, 1.9, 0.4}, {4.8, 3.0, 1.4, 0.3}, {5.1, 3.8, 1.6, 0.2},
        {4.6, 3.2, 1.4, 0.2}, {5.3, -3.7, 1.5, 0.2}, {5.0, 3.3, 1.4, 0.2}, {7.0, 3.2, 4.7, 0.4}, {6.4, 3.2, 4.5, 0.5},
        {6.9, 3.1, 4.9, 0.5}, {5.5, 2.3, 4.0, 0.3}, {6.5, 2.8, 4.6, 0.5}, {5.7, 2.8, 4.5, 0.3}, {6.3, 3.3, 4.7, 0.6},
        {4.9, 2.4, 3.3, 0.0}, {6.6, 2.9, 4.6, 0.3}, {5.2, 2.7, 3.9, 0.4}, {5.0, 2.0, 3.5, 0.0}, {5.9, 3.0, 4.2, 0.5},
        {6.0, 2.2, 4.0, 0.0}, {6.1, 2.9, 4.7, 0.4}, {5.6, 2.9, 3.6, 0.3}, {6.7, 3.1, 4.4, 0.4}, {5.6, 3.0, 4.5, 0.5},
        {5.8, 2.7, 4.1, 0.0}, {-6.2, -2.2, 4.5, 0.5}, {5.6, 2.5, 3.9, 0.1}, {5.9, 3.2, 4.8, 0.8}, {6.1, 2.8, 4.0, 0.3},
        {6.3, 2.5, 4.9, 0.5}, {6.1, 2.8, 4.7, 0.2}, {6.4, 2.9, 4.3, 0.3}, {6.6, 3.0, 4.4, 0.4}, {6.8, 2.8, 4.8, 0.4},
        {6.7, 3.0, 5.0, 0.7}, {6.0, 2.9, 4.5, 0.5}, {5.7, 2.6, 3.5, 0.0}, {5.5, 2.4, 3.8, 0.1}, {5.5, 2.4, 3.7, 0.0},
        {5.8, 2.7, 3.9, 0.2}, {6.0, 2.7, 5.1, 0.6}, {5.4, 3.0, 4.5, 0.5}, {6.0, 3.4, 4.5, 0.6}, {6.7, 3.1, 4.7, 0.5},
        {6.3, 2.3, 4.4, 0.3}, {5.6, -3.0, 4.1, 0.3}, {5.5, 2.5, 4.0, 0.3}, {5.5, 2.6, 4.4, 0.2}, {6.1, 3.0, 4.6, 0.4},
        {5.8, 2.6, 4.0, 0.2}, {5.0, 2.3, 3.3, 0.0}, {5.6, 2.7, 4.2, 0.3}, {5.7, 3.0, 4.2, 0.2}, {5.7, 2.9, 4.2, 0.3},
        {6.2, 2.9, 4.3, 0.3}, {5.1, 2.5, 3.0, 0.1}, {5.7, 2.8, 4.1, 0.3}, {6.3, 3.3, 6.0, 0.5}, {5.8, 2.7, 5.1, 0.9},
        {7.1, 3.0, 5.9, 0.1}, {6.3, 2.9, 5.6, 0.8}, {6.5, 3.0, 5.8, 0.2}, {7.6, 3.0, 6.6, 0.1}, {4.9, 2.5, 4.5, 0.7},
        {7.3, 2.9, 6.3, 0.8}, {6.7, 2.5, 5.8, 0.8}, {7.2, 3.6, 6.1, 0.5}, {6.5, 3.2, 5.1, 0.0}, {6.4, 2.7, 5.3, 0.9},
        {6.8, 3.0, 5.5, 0.1}, {5.7, -2.5, 5.0, 0.0}, {5.8, 2.8, 5.1, 0.4}, {6.4, 3.2, 5.3, 0.3}, {6.5, 3.0, 5.5, 0.8},
        {7.7, 3.8, 6.7, 0.2}, {7.7, 2.6, 6.9, 0.3}, {6.0, 2.2, 5.0, 0.5}, {6.9, 3.2, 5.7, 0.3}, {5.6, 2.8, 4.9, 0.0},
        {7.7, 2.8, 6.7, 0.0}, {176.3, 0.7, 4.9, 0.8}, {6.7, 3.3, 5.7, 0.1}, {7.2, 3.2, 6.0, 0.8}, {6.2, 2.8, 4.8, 0.8},
        {6.1, 3.0, 4.9, 0.8}, {86.4, 2.8, 5.6, 0.1}, {7.2, 3.0, 5.8, 0.6}, {7.4, 2.8, 6.1, 0.9}, {7.9, 3.8, 6.4, 0.0},
        {6.4, 2.8, 5.6, 0.2}, {-666.3, 2.8, 5.1, 0.5}, {6.1, 2.6, 5.6, 0.4}, {7.7, 3.0, 6.1, 0.3},
        {6.3, 3.4, 5.6, 0.4}, {6.4, 3.1, 5.5, 0.8}, {6.0, 3.0, 4.8, 0.8}, {6.9, 3.1, 5.4, 0.1}, {6.7, 3.1, 5.6, 0.4},
        {6.9, 3.1, 5.1, 0.3}, {5.8, 2.7, 5.1, 0.9}, {6.8, 3.2, 5.9, 0.3}, {6.7, 3.3, 5.7, 0.5}, {6.7, 3.0, 5.2, 0.3},
        {6.3, 0.5, 5.0, 0.9}, {6.5, 3.0, 5.2, 0.0}, {6.2, 3.4, 5.4, 0.3}, {5.9, 3.0, 5.1, 0.8}};

}