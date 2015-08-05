package com.knime.bigdata.spark.testing.node;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.knime.bigdata.spark.jobserver.jobs.NormalizeColumnsJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.node.preproc.pmml.normalize.SparkNormalizerPMMLApplyNodeModel;
import com.knime.bigdata.spark.testing.jobserver.server.RDDUtilsInJavaTest;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SparkNormalizerPMMLApplyNodeModelTest {

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

    static class CreateRDD implements Serializable {
        private static final long serialVersionUID = 1L;

        final JavaRDD<Row> rowData;

        CreateRDD(final JavaSparkContext aContext, final List<Double[]> aData) {
            // Load and parse the data
            JavaRDD<Double[]> data = aContext.parallelize(aData);
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
    }

    @Test
    public void missingNormalizationModeShouldYieldInvalidConfig() throws Throwable {

        final String json =
            SparkNormalizerPMMLApplyNodeModel.paramsToJson("tab1", new Integer[]{1, 5, 2, 7}, null, "out");

        JobConfig config = new JobConfig(ConfigFactory.parseString(json));
        final KnimeSparkJob testObj = new NormalizeColumnsJob();
        assertEquals(
            "Configuration should be recognized as invalid",
            ValidationResultConverter.invalid("Input parameter '"
                + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_STRING, 1) + "' missing."),
            testObj.validate(config));

    }

    @Test
    public void ensureThatAllRequiredParametersAreSet() throws Throwable {

        final String json =
            SparkNormalizerPMMLApplyNodeModel.paramsToJson("tab1", new Integer[]{1, 5, 2, 7}, new Double[][]{
                new Double[]{0.0001d}, new Double[]{67889.7654d}}, "out");

        JobConfig config = new JobConfig(ConfigFactory.parseString(json));
        final KnimeSparkJob testObj = new NormalizeColumnsJob();
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            testObj.validate(config));

        Double[][] values = NormalizeColumnsJob.getNormalizationApplySettings(config);
        assertEquals("scale parameter", 0.0001d, values[0][0], 0.0000001);
        assertEquals("translation parameter", 67889.7654d, values[1][0], 0.0000001);

    }

    @Test
    public void applySingleColumnNormalizationToSmallRDD() throws Throwable {

        final String json =
            SparkNormalizerPMMLApplyNodeModel.paramsToJson("tab1", new Integer[]{0}, new Double[][]{new Double[]{0.1d},
                new Double[]{33d}}, "out");

        final List<Double[]> data = new ArrayList<Double[]>(4);
        data.add(new Double[]{0d});
        data.add(new Double[]{0.0001d});
        data.add(new Double[]{0.5d});
        data.add(new Double[]{1d});

        CreateRDD rddData = new CreateRDD(sparkContextResource.sparkContext, data);
        JobConfig config = new JobConfig(ConfigFactory.parseString(json));
        List<Row> normalizedData = NormalizeColumnsJob.execute(config, rddData.rowData).getRdd().collect();
        assertEquals("number rows must not change", data.size(), normalizedData.size());
        for (int i = 0; i < data.size(); i++) {
            assertEquals("normalized value[" + i + "]", data.get(i)[0] * 0.1 + 33d, normalizedData.get(i).getDouble(0),
                0.00000001);
        }

    }

    @Test
    public void applyNormalizationToSmallRDD() throws Throwable {

        final Double[] scale = new Double[]{0.1d, 0d, 0d};
        final Double[] translation = new Double[]{0d, 33d, 0d};

        final String json =
            SparkNormalizerPMMLApplyNodeModel.paramsToJson("tab1", new Integer[]{0, 1, 2}, new Double[][]{scale,
                translation}, "out");

        final List<Double[]> data = new ArrayList<Double[]>(4);
        data.add(new Double[]{0d, 4d, 0d});
        data.add(new Double[]{0.0001d, 35d, 1d});
        data.add(new Double[]{0.5d, 0d, 2d});
        data.add(new Double[]{1d, -1d, 3d});

        CreateRDD rddData = new CreateRDD(sparkContextResource.sparkContext, data);
        JobConfig config = new JobConfig(ConfigFactory.parseString(json));
        List<Row> normalizedData = NormalizeColumnsJob.execute(config, rddData.rowData).getRdd().collect();
        assertEquals("number rows must not change", data.size(), normalizedData.size());
        for (int i = 0; i < data.size(); i++) {
            for (int j = 0; j < scale.length; j++) {
                assertEquals("normalized value[" + i + "," + j + "]", data.get(i)[j] * scale[j] + translation[j],
                    normalizedData.get(i).getDouble(j), 0.00000001);
            }
        }

    }
}