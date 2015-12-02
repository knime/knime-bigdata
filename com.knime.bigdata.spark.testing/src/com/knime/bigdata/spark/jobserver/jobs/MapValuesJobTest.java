package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.ColumnBasedValueMapping;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.convert.number2category.Number2CategoryConverterTaskTest;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 *         (these tests do not need a running job-server)
 *
 */
@SuppressWarnings("javadoc")
public class MapValuesJobTest {

	@Test
	public void jobValidationShouldCheckMissingInputTableParameter()
			throws Throwable {
		final String params = Number2CategoryConverterTaskTest.paramsAsJason(
				null, new ColumnBasedValueMapping(), "output");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckMissingMappingParameter()
			throws Throwable {
		final String params = Number2CategoryConverterTaskTest.paramsAsJason(
				"inTab", null, "output");
		myCheck(params, MapValuesJob.PARAM_MAPPING, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingResultTableParameter()
			throws Throwable {
		final String params = Number2CategoryConverterTaskTest.paramsAsJason(
				"in", new ColumnBasedValueMapping(), null);
		myCheck(params, KnimeSparkJob.PARAM_RESULT_TABLE, "Output");
	}
	
	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		final KnimeSparkJob testObj = new MapValuesJob();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}