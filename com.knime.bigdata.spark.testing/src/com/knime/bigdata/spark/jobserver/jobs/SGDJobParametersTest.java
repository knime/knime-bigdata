package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.linear.SGDLearnerTask;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SGDJobParametersTest {

    @Test
    public void jobValidationShouldCheckMissingNumIterationsParameter() throws Throwable {
        final String params =
                SGDLearnerTask.learnerDef("tab1", NominalFeatureInfo.EMPTY, new String[] {"a", "b"}, new Integer[] {0, 1}, 1, null, 0.5);
        myCheck(params, ParameterConstants.PARAM_NUM_ITERATIONS, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingRegularizationParameter() throws Throwable {
        final String params =
                SGDLearnerTask.learnerDef("tab1", NominalFeatureInfo.EMPTY, new String[] {"a", "b"}, new Integer[] {0, 1}, 1, 10, null);
        myCheck(params, LinearRegressionWithSGDJob.PARAM_REGULARIZATION, "Input");
    }

    @Test
    public void jobValidationShouldCheckAllValidParams() throws Throwable {
        final String params = SGDLearnerTask.learnerDef("tab1", NominalFeatureInfo.EMPTY, new String[] {"a", "b"}, new Integer[] {0, 1}, 1, 10, 0.4d);
        final KnimeSparkJob testObj = new LinearRegressionWithSGDJob();
        final Config config = ConfigFactory.parseString(params);
        final JobConfig config2 = new JobConfig(ConfigFactory.parseString(SupervisedLearnerJobParametersTest.allValidParams()).withFallback(config));
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            testObj.validate(config2));
    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        final KnimeSparkJob testObj = new LinearRegressionWithSGDJob();
        final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }



}