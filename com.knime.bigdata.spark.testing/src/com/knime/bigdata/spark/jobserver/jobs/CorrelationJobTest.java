package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.CorrelationMethods;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.statistics.correlation.CorrelationTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class CorrelationJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = CorrelationTaskTest.paramsAsJason(null,
				new Integer[] {0, 1}, CorrelationMethods.spearman, "out");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckWhetherEitherIndicesParameterIsSet()
			throws Throwable {
		CorrelationTaskTest.paramsAsJason("tab1", null, CorrelationMethods.spearman, "out");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckWhetherEitherIndicesOrOutputTableParameterIsSetIndex2Missing()
			throws Throwable {
		CorrelationTaskTest.paramsAsJason("tab1", new Integer[] {0}, CorrelationMethods.spearman, "out");
	}
	
	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckWhetherEitherIndicesOrOutputTableParameterIsSetMoreThan2IndicesButResMissing()
			throws Throwable {
		CorrelationTaskTest.paramsAsJason("tab1", new Integer[] {0, 1, 3}, CorrelationMethods.spearman, null);
	}

	@Test
	public void jobValidationShouldCheckStatMethodParameter()
			throws Throwable {
		String params = CorrelationTaskTest.paramsAsJason("tab1", new Integer[] {0, 1}, null, "out");
		myCheck(params, CorrelationJob.PARAM_STAT_METHOD, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = CorrelationTaskTest.paramsAsJason("tab1",
				new Integer[] {3, 5}, CorrelationMethods.spearman, null);
		KnimeSparkJob testObj = new CorrelationJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new CorrelationJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}