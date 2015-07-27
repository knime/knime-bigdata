package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.jobs.LinearRegressionWithSGDJob;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SGDJobParametersTest {

    private static String getParams(final Integer aNumIterations, final Double aRegParam) {
        StringBuilder params = new StringBuilder("{\n");
        params.append("   \"").append(ParameterConstants.PARAM_INPUT).append("\" {\n");

        if (aNumIterations != null) {
            params.append("         \"").append(ParameterConstants.PARAM_NUM_ITERATIONS).append("\": ")
                .append(aNumIterations).append(",\n");
        }
        if (aRegParam != null) {
            params.append("         \"").append(ParameterConstants.PARAM_STRING).append("\": ")
                .append(aRegParam).append(",\n");
        }

        params.append("    }\n");
        params.append("    \n}");
        return params.toString();
    }

    @Test
    public void jobValidationShouldCheckMissingNumIterationsParameter() throws Throwable {
        String params =
            getParams(null, 5d);
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_NUM_ITERATIONS, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingRegularizationParameter() throws Throwable {
        String params = getParams(10, null);
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_STRING, "Input");
    }

    @Test
    public void jobValidationShouldCheckAllValidParams() throws Throwable {
        String params =
            getParams(100, 0.4d);
        KnimeSparkJob testObj = new LinearRegressionWithSGDJob();
        Config config = ConfigFactory.parseString(params);
        Config config2 = ConfigFactory.parseString(SupervisedLearnerJobParametersTest.allValidParams()).withFallback(config);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            testObj.validate(config2));
    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        KnimeSparkJob testObj = new LinearRegressionWithSGDJob();
        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }



}