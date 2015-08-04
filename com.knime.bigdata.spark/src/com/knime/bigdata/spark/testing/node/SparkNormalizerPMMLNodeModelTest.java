package com.knime.bigdata.spark.testing.node;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.jobs.NormalizeColumnsJob;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NormalizationSettingsFactory;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.pmml.normalize.SparkNormalizerPMMLNodeModel;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SparkNormalizerPMMLNodeModelTest {

    @Test
    public void missingNormalizationModeShouldYieldInvalidConfig() throws Throwable {

        final String json = SparkNormalizerPMMLNodeModel.paramsToJson("tab1", new Integer[]{1, 5, 2, 7}, null, "out");

        final KnimeSparkJob testObj = new NormalizeColumnsJob();
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid("Input parameter '" + ParameterConstants.PARAM_INPUT+"."+ParameterConstants.PARAM_STRING + "' missing."),
            testObj.validate(ConfigFactory.parseString(json)));
    }

    @Test
    public void ensureThatAllRequiredParametersAreSet() throws Throwable {

        final String json = SparkNormalizerPMMLNodeModel.paramsToJson("tab1", new Integer[]{1, 5, 2, 7}, NormalizationSettingsFactory.createNormalizationSettingsForMinMaxScaling(0, 1), "out");

        final KnimeSparkJob testObj = new NormalizeColumnsJob();
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            testObj.validate(ConfigFactory.parseString(json)));
    }
}